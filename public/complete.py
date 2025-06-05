#models.py
from sqlalchemy import Column, Integer, String
from database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)

#schemas.py
from pydantic import BaseModel

class UserCreate(BaseModel):
    email: str
    password: str

class User(BaseModel):
    id: int
    email: str

    class Config:
        orm_mode = True

#crud.py
from sqlalchemy.orm import Session
import models
import schemas
from passlib.hash import bcrypt

def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()

def create_user(db: Session, user: schemas.UserCreate):
    hashed_pw = bcrypt.hash(user.password)
    db_user = models.User(email=user.email, password=hashed_pw)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

#main.py
from fastapi import FastAPI, Depends, HTTPException
import models, database, crud, schemas
from sqlalchemy.orm import Session
import time
from sqlalchemy.exc import OperationalError

app = FastAPI()

MAX_TRIES = 5
for i in range(MAX_TRIES):
    try:
        models.Base.metadata.create_all(bind=database.engine)
        break
    except OperationalError:
        print(f"Database not ready yet. Waiting... {i+1}/{MAX_TRIES}")
        time.sleep(2)
else:
    print("Database connection failed after retries. Exiting.")
    exit(1)

@app.post("/register")
def register_user(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    db_user = crud.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    return crud.create_user(db, user)


'''driver_location service'''

#models.py
from pydantic import BaseModel, Field

class LocationUpdate(BaseModel):
    driver_id: str = Field(..., example="driver_123")
    latitude: float = Field(..., example=19.07)
    longitude: float = Field(..., example=72.87)

#redis_client.py
import redis.asyncio as aioredis
from fastapi import Depends

REDIS_URL = "redis://redis:6379"

async def get_redis():
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield redis
    finally:
        await redis.close()

#main.py
from fastapi import FastAPI, Depends
from models import LocationUpdate
from redis_client import get_redis
import json  

app = FastAPI()

@app.post("/update_location")
async def update_location(location: LocationUpdate, redis = Depends(get_redis)):
    
    value = json.dumps({"lat": location.latitude, "lng": location.longitude})
    await redis.hset("drivers:location", location.driver_id, value)
    return {"message": "Location updated successfully"}


'''ride_match serice'''

#utils.py
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (math.sin(delta_phi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c 

#redis_client.py
import redis.asyncio as aioredis
import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

async def get_redis():
    redis = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
    return redis

async def get_active_drivers():
    redis = await get_redis()
    drivers = await redis.hgetall("drivers:location", encoding="utf-8")
    await redis.close()
    return drivers

#kafka_producer.py
import json
from aiokafka import AIOKafkaProducer
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

producer = None

async def get_producer():
    global producer
    if not producer:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await producer.start()
    return producer

async def publish_match(match_payload):
    producer = await get_producer()
    await producer.send_and_wait("ride_matches", json.dumps(match_payload).encode("utf-8"))

#kafka_consumer.py
import asyncio
import json
from aiokafka import AIOKafkaConsumer
from redis_client import get_active_drivers
from utils import haversine_distance
from kafka_producer import publish_match
import os
from aiokafka.errors import KafkaConnectionError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

async def consume_ride_requests():
    consumer = AIOKafkaConsumer(
        "ride_requests",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="ride-match-group",
        auto_offset_reset="earliest"
    )

    for attempt in range(10): 
        try:
            await consumer.start()
            print("Kafka connected")
            break
        except KafkaConnectionError:
            print(f"Kafka not ready yet... retrying ({attempt + 1}/10)")
            await asyncio.sleep(15)
    else:
        print("Kafka connection failed after retries.")
        return 
    

    try:
        async for msg in consumer:
            request = json.loads(msg.value.decode("utf-8"))
            print(f"Received ride request: {request}")
            await handle_request(request)
    finally:
        await consumer.stop()

async def handle_request(request):
    drivers = await get_active_drivers()
    rider_lat = request["pickup_lat"]
    rider_lng = request["pickup_lng"]

    min_distance = float('inf')
    selected_driver = None

    for driver_id, location_json in drivers.items():
        location = json.loads(location_json)
        distance = haversine_distance(rider_lat, rider_lng, location["lat"], location["lng"])
        if distance < min_distance:
            min_distance = distance
            selected_driver = driver_id

    if selected_driver:
        match_payload = {
            "rider_id": request["rider_id"],
            "driver_id": selected_driver,
            "pickup_lat": rider_lat,
            "pickup_lng": rider_lng,
            "estimated_distance_km": min_distance
        }
        await publish_match(match_payload)
    else:
        print("No drivers available!")

#main.py
from fastapi import FastAPI
import asyncio
from kafka_consumer import consume_ride_requests

app = FastAPI()

@app.get("/")
def home():
    return {"status": "ride-match service running"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_ride_requests())


'''trip service'''

#schemas.py
from pydantic import BaseModel
from enum import Enum

class RideStatus(str, Enum):
    requested = "requested"
    accepted = "accepted"
    ongoing = "ongoing"
    completed = "completed"

class TripCreate(BaseModel):
    rider_id: str
    origin: str
    destination: str

class TripOut(TripCreate):
    id: int
    status: RideStatus
    fare: float

    class Config:
        orm_mode = True

class StatusUpdate(BaseModel):
    status: RideStatus

# models.py
from sqlalchemy import Column, Integer, String, Enum, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
import enum
from datetime import datetime

Base = declarative_base()

class RideStatus(str, enum.Enum):
    requested = "requested"
    accepted = "accepted"
    ongoing = "ongoing"
    completed = "completed"

class Trip(Base):
    __tablename__ = "trips"
    
    id = Column(Integer, primary_key=True, index=True)
    rider_id = Column(String, nullable=False)
    driver_id = Column(String, nullable=True)
    origin = Column(String, nullable=False)
    destination = Column(String, nullable=False)
    status = Column(Enum(RideStatus), default=RideStatus.requested)
    fare = Column(Float, default=0.0)
    timestamp = Column(DateTime, default=datetime.utcnow)

#database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://postgres:postgres@db:5432/postgres"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#crud.py
from sqlalchemy.orm import Session
from models import Trip, RideStatus
from schemas import TripCreate


def create_trip(db: Session, trip: TripCreate) -> Trip:
    db_trip = Trip(
        rider_id=trip.rider_id,
        origin=trip.origin,
        destination=trip.destination,
    )
    db.add(db_trip)
    db.commit()
    db.refresh(db_trip)
    return db_trip


def get_trip(db: Session, trip_id: int) -> Trip:
    return db.query(Trip).filter(Trip.id == trip_id).first()


def update_status(db: Session, trip_id: int, new_status: RideStatus) -> Trip:
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    if trip:
        trip.status = new_status
        db.commit()
        db.refresh(trip)
    return trip

#main.py
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base
import crud, schemas

Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@app.post("/trip/", response_model=schemas.TripOut)
def create_trip(trip: schemas.TripCreate, db: Session = Depends(get_db)):
    return crud.create_trip(db, trip)

@app.get("/trip/{trip_id}", response_model=schemas.TripOut)
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    return crud.get_trip(db, trip_id)

@app.put("/trip/{trip_id}/status", response_model=schemas.TripOut)
def update_trip_status(trip_id: int, status: schemas.StatusUpdate, db: Session = Depends(get_db)):
    return crud.update_status(db, trip_id, status.status)
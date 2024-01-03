from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from kafka import KafkaProducer
from pydantic import BaseModel
from mongodb import MongoDBManager

manager = MongoDBManager(db_name="local_kafka", collection_name="kafkacollection")
# FastAPI app
app = FastAPI()

# OAuth2.0 Authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Fake database for demonstration purposes
fake_users_db = {"testuser": {"username": "testuser", "password": "testpassword"}}

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "json_payloads"
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: str(v).encode("utf-8"),
)


# Models
class Item(BaseModel):
    payload: dict


class UserCreate(BaseModel):
    username: str
    password: str


# Dependency to get the current user
def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    user = fake_users_db.get(token)
    if user is None:
        raise credentials_exception
    return user


# Routes
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = fake_users_db.get(form_data.username)
    if user is None or user["password"] != form_data.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"access_token": form_data.username, "token_type": "bearer"}


@app.post("/register")
async def register_user(user: UserCreate):
    # Check if the username is already taken (you might want to use a real database for this)
    if user.username in fake_users_db:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered",
        )

    # Store the new user in the fake database (replace with actual database operations)
    fake_users_db[user.username] = {
        "username": user.username,
        "password": user.password,
    }

    return {"message": "User registered successfully"}


@app.post("/send_payload")
async def send_payload(item: Item, current_user: dict = Depends(get_current_user)):
    json_payload = item.payload

    # Send payload to Kafka
    producer.send(kafka_topic, value=json_payload)

    # Insert payload into MongoDB
    manager.create_document(json_payload)

    return {"message": "Payload sent and stored successfully"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)

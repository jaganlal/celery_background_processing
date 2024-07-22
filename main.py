from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaError
from celery.result import AsyncResult
from celery import Celery
import json
import requests
from io import BytesIO
from PIL import Image
import numpy as np
from fastapi.middleware.cors import CORSMiddleware

# from imgbeddings import imgbeddings

ibed = None
# ibed = imgbeddings()
app = FastAPI()
app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

KAFKA_TOPIC = "image_ingestion"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
LOG_IMAGES_URL = "http://localhost:8000/log_images"  # Adjust this URL to match your logging endpoint

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

celery_app = Celery(
    'tasks', 
    broker='confluentkafka://localhost:9092',
    backend='redis://localhost:6379/0'
)

class ImageIngestionRequest(BaseModel):
    request_id: str
    images: list[str]
class LogImagesRequest(BaseModel):
    request_id: str
    embeddings: list

@app.post("/ingest_images/")
async def ingest_images(request: ImageIngestionRequest, background_tasks: BackgroundTasks):
    message = {
        "request_id": request.request_id,
        "images": request.images
    }
    producer.produce(KAFKA_TOPIC, json.dumps(message).encode('utf-8'))
    producer.flush()
    background_tasks.add_task(process_image_embeddings, request.request_id)
    return {"message": "Images ingestion started"}

@celery_app.task
def process_image_embeddings(request_id):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'image_processor_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_TOPIC])
    embeddings = [0, 1]

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        message = json.loads(msg.value().decode('utf-8'))
        if message['request_id'] == request_id:
            for image_url in message['images']:
                result = generate_image_embedding.delay(image_url)
                embedding = result.get()  # This blocks until the task is completed, which is not ideal for large batches
                embeddings.append(embedding)
            break

    consumer.close()

    # Send embeddings to the log_images endpoint
    response = requests.post(LOG_IMAGES_URL, json={"request_id": request_id, "embeddings": embeddings})
    return response.status_code

@celery_app.task
def generate_image_embedding(image_url):
    print(f"Image URL: %s" % image_url)
    embedding = []

    # try:
    #   response = requests.get(image_url)

    #   image = Image.open(BytesIO(response.content))
    #   vector = ibed.to_embeddings(image)
    #   embedding = np.array(vector[0])
    # except Exception as e:
    #     print(f"Error is %s" % e)
    

    # image = Image.open(BytesIO(response.content))
    # embedding = np.array([0.5] * 128)  # Placeholder for actual embedding generation
    return embedding

@app.post("/log_images/")
async def log_images(request: LogImagesRequest):
    # Logic to handle logging of image embeddings
    print(f"Received embeddings for request_id: {request.request_id}")
    return {"message": "Embeddings logged successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
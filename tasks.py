import asyncio
import requests
import time
from celery import Celery

celery_app = Celery('tasks', broker='confluentkafka://localhost:9092')

@celery_app.task
def io_bound_task(url):
    response = requests.get(url)
    return response.status_code

@celery_app.task
def collect_results(results, start_time):
    end_time = time.time()
    return {"status": results, "time_taken": end_time - start_time}

@celery_app.task
def process_message(message):
    print(f"Processing message: {message}")
    asyncio.run(long_async_task())
    return {'result': message}

async def long_async_task():
    for i in range(10):
        await asyncio.sleep(1)
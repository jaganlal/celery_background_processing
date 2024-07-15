from contextlib import asynccontextmanager
import time
# from tasks import io_bound_task, collect_results
from producer import produce_message
from celery import chord
# from consumer import init_consumer, BackgroundRunner, init_consumer_v1
import celery.states
from celery.result import AsyncResult
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from threading import Thread

consumer_thread = None

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    print("init lifespan")
    # code to execute when app is loading
    # start_background_consumer()
    # asyncio.create_task(consume_messages())
    yield
    # code to execute when app is shutting down

app = FastAPI(lifespan=app_lifespan)

app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

@app.get("/api/1.0/process_message")
def process_message():
  topic = 'my_test_topic'
  message = 'my new test message'
  produce_message(topic, message)

@app.get("/api/{task_id}/status")
async def status(task_id: str) -> dict:
  res = AsyncResult(task_id)
  if res.state == celery.states.SUCCESS:
    return {'state': celery.states.SUCCESS,
            'result': res.result}
  return {'state': res.state, }


# @app.get("/sequential")
# def sequential_task():
#   start_time = time.time()
#   urls = ["https://httpbin.org/delay/3" for n in range(10)]
#   results = []
#   for url in urls:
#     results.append(io_bound_task(url))
#   end_time = time.time()
#   return {"status": results, "time_taken": end_time - start_time}

# @app.get("/sequential_celery")
# def celery_task():
#   start_time = time.time()
#   urls = ["https://httpbin.org/delay/3" for n in range(10)]
#   tasks = [io_bound_task.s(url) for url in urls]
#   callback = chord(tasks)(collect_results.s(start_time))
#   return {"task_id": callback.id}


# @app.get("/result/{task_id}")
# def get_result(task_id: str):
#   task = collect_results.AsyncResult(task_id)
#   if task.state == 'PENDING':
#     return {"status": "Task is still in progress", "state": task.state}
#   elif task.state != 'FAILURE':
#     return task.result
#   else:
#     return {"status": "Task failed", "state": task.state, "error": str(task.info)}

def start_background_consumer():
  # background_tasks = BackgroundTasks()
  # background_tasks.add_task(init_consumer)
  
  # asyncio.create_task(init_consumer())

  # runner = BackgroundRunner()
  # asyncio.create_task(runner.run_main())

  # background_tasks = BackgroundTasks()
  # runner = BackgroundRunner()
  # background_tasks.add_task(runner.run_main())

  global consumer_thread
  consumer_thread = Thread(target=init_consumer_v1)
  consumer_thread.start()
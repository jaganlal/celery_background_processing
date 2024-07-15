from fastapi import FastAPI
from typing import Any
from confluent_kafka import Consumer, KafkaException
import asyncio
from tasks import process_message

config = {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'my_group',
  'auto.offset.reset': 'earliest'
}

class BackgroundRunner:
  def __init__(self):
      self.value = 0

  async def run_main(self):
    consumer = Consumer(**config)
    consumer.subscribe(['my_test_topic'])

    try:
      while True:
        msg = consumer.poll(1.0)

        if msg is None:
          continue
        if msg.error():
          print(f"Consumer error: {msg.error()}")
          continue

        process_message.delay(msg.value().decode('utf-8'))
    finally:
        consumer.close()

async def init_consumer():
  consumer = Consumer(**config)
  consumer.subscribe(['my_test_topic'])

  for message in consumer:
    print(message.value.decode('utf-8'))
    process_message.delay(message.value.decode('utf-8'))

def init_consumer_v1():
  consumer = Consumer(**config)
  consumer.subscribe(['my_test_topic'])

  try:
    while True:
      msg = consumer.poll(0.2)

      if msg is None:
        continue
      if msg.error():
        if msg.error().code() == KafkaException._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

      print(f"Consumed message: {msg.value().decode('utf-8')}")
      process_message.delay(msg.value().decode('utf-8'))
      # process_message(msg.value().decode('utf-8'))
  except Exception as e:
    print(f"Error while consuming: {e}")
  finally:
    consumer.close()

async def init_consumer_async():
  consumer = Consumer(**config)
  consumer.subscribe(['my_test_topic'])

  try:
    while True:
      msg = consumer.poll(1.0)

      if msg is None:
        continue
      if msg.error():
        if msg.error().code() == KafkaException._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

      print(f"Consumed message: {msg.value().decode('utf-8')}")
      process_message.delay(msg.value().decode('utf-8'))
  except Exception as e:
    print(f"Error while consuming: {e}")
  finally:
    consumer.close()

async def consume_messages() -> None:
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe(['my_test_topic'])
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print(f"Consumed message: {msg.value().decode('utf-8')}")
            process_message.delay(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"Error while consuming: {e}")
        await asyncio.sleep(1)
    
    consumer.close()

async def lifespan(app: FastAPI) -> Any:
    asyncio.create_task(consume_messages())
    yield
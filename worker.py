from confluent_kafka import Consumer, KafkaException
from tasks import process_message

config = {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'my_group',
  'auto.offset.reset': 'earliest'
}
def consume_from_kafka():
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
      # process_message(msg.value().decode('utf-8'))
  except Exception as e:
    print(f"Error while consuming: {e}")
  finally:
    consumer.close()

if __name__ == "__main__":
  consume_from_kafka()
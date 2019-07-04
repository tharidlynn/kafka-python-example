import time
import datetime
import requests
import uuid
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], retries=5)

def get_uiname():
    for _ in range(10):
        try:
            payload = requests.get('http://uinames.com/api/').json()
        except ValueError as e:
            print(e)
            pass
        else:
            payload['id'] = str(uuid.uuid4())
            payload['timestamp'] = datetime.datetime.now().timestamp()
            print(payload)

            # Encode with utf8 to raw bytes
            message_bytes = json.dumps(payload).encode('utf-8')

            producer.send('uiname', message_bytes)
            producer.flush()

if __name__ == '__main__':
    start = time.time()
    get_uiname()
    print(f'Total run time: {time.time() - start}')
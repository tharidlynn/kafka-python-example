import time
import datetime
import requests
import uuid
import json
from kafka import KafkaProducer

from multiprocessing.dummy import Pool as ThreadPool

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], retries=5)


def get_uiname(_):
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
    with ThreadPool() as p:
        p.map(get_uiname, range(1000))

    print(f'Total run time: {time.time() - start}')


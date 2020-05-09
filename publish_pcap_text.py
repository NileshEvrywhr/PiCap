from google.cloud import pubsub_v1
import random
from time import sleep
import os

PROJECT_ID='peppy-freedom-276106'
TOPIC = 'managed_wlp3s0'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/media/knilesh/DATA/docs/Apollo's Landing-0424566883dc.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data = data)

def callback(message_future):
    if message_future.exception(timeout=30):
        print('publishing message on {} threw an exception {}.'.format(topic_name, message_future.exception()))
    else:
        print(message_future.result())
        
if __name__ == '__main__':
    
    f = open('/media/knilesh/DATA/code/PiCap/lorem.txt','r')
    
    while True:
        line = f.readline()
        if not line:
            break
        print(line.strip())
        message_future = publish(publisher, topic_path, line)
        message_future.add_done_callback(callback)
        
        sleep_time = random.choice(range(1,3,1))
        sleep(sleep_time)
    
    f.close() 
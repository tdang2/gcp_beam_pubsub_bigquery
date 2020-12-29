from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials
from random import randrange
import time
import os
import json
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(override=True, dotenv_path=ENV_PATH)

project_id = os.getenv('PROJECT_ID')
topic_id = os.getenv('TOPIC_ID')
credentials = Credentials.from_service_account_file(os.getenv('GCP_DEFAULT_CREDENTIALS'))
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)
users = ['Adam', 'Tri', 'Shrey', 'Ailin', 'Saumya']
actions = ['laughs', 'eats', 'drinks', 'codes', 'sleeps']
timers = [2, 3]


def main():
    while 1:
        sleepTime = timers[randrange(len(timers))]
        data = json.dumps({
            "actor": users[randrange(len(users))],
            "action": actions[randrange(len(actions))],
            "created_at": datetime.utcnow().isoformat()
        }).encode("utf-8")
        future = publisher.publish(topic_path, data)
        time.sleep(sleepTime)


if __name__ == '__main__':
    main()

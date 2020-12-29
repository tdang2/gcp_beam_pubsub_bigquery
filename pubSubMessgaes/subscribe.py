import os
from dotenv import load_dotenv
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(override=True, dotenv_path=ENV_PATH)

project_id = "project-id-here"
subscription_id = "subscription-id-here"
timeout = 10  # Number of seconds the subscriber should listen for messages

credentials = Credentials.from_service_account_file(os.getenv('GCP_DEFAULT_CREDENTIALS'))
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    print(f"Received {message}")
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")


# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()

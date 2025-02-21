from utils import *
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1
import json

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Topic details
PROJECT_ID = os.getenv("PROJECT_ID")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME")
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)


def pull_messages():
    for _ in range(5):
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 1}
        )
        ack_ids = []
        # print(type(response))
        # print(response)
        for received_message in response.received_messages:
            # Extract JSON data
            json_data_str = received_message.message.data.decode("utf-8")
            print(json_data_str)

            # Deserialize the JSON data
            deserialized_data = json.loads(json_data_str)

            # Collect ack ID for acknowledgment
            ack_ids.append(received_message.ack_id)

        # Acknowledge the messages so they won't be sent again
        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )


def main():
    pull_messages()
    pass


if __name__ == "__main__":
    main()

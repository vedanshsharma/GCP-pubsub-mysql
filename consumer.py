from utils import *
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1
import json
import sys

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Topic details
PROJECT_ID = os.getenv("PROJECT_ID")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME")
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)


def transform(data) -> json:
    discounted_price = data["price"]
    if data["category"] == "Electronics":
        if data["name"] in ["Laptop", "Smartphone"]:
            discounted_price = data["price"] * 0.9  # 10% off
        elif data["name"] == "TV":
            discounted_price = data["price"] * 0.8  # 20% off
        elif data["name"] == "Headphones":
            discounted_price = data["price"] * 0.95  # 5% off

    elif data["category"] == "Clothing":
        if data["name"] in ["Shirt", "Dress", "Pants"]:
            discounted_price = data["price"] * 0.85  # 15% off
        elif data["name"] == "Shoes":
            discounted_price = data["price"] * 0.9  # 10% off
        elif data["name"] == "Jacket":
            discounted_price = data["price"] * 0.8  # 20% off

    elif data["category"] == "Home & Garden":
        if "Furniture" in data["name"]:  # Check if "Furniture" is a substring
            discounted_price = data["price"] * 0.9  # 10% off
        elif "Appliance" in data["name"]:
            discounted_price = data["price"] * 0.85  # 15% off

    elif data["category"] == "Beauty":
        if "Makeup" in data["name"]:
            discounted_price = data["price"] * 0.95  # 5% off
        elif "Skincare" in data["name"]:
            discounted_price = data["price"] * 0.9  # 10% off

    elif data["category"] == "Toys":
        if data["name"] in ["Doll", "Action Figure"]:
            discounted_price = data["price"] * 0.8  # 20% off
        elif data["name"] == "Board Game":
            discounted_price = data["price"] * 0.95  # 5% off
    data["price"] = discounted_price
    data["name"] = data["name"].upper()


def pull_messages(json_file_name):
    for i in range(40):
        json_concat_str = ''
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 25}
        )
        ack_ids = []
        # print(type(response))
        # print(response)
        for received_message in response.received_messages:
            # Extract JSON data
            json_data_str = received_message.message.data.decode("utf-8")
            # print(json_data_str)
            # Deserialize the JSON data
            deserialized_data = json.loads(json_data_str)
            transform(deserialized_data)
            deserialized_data_json = json.dumps(json_data_str)
            json_concat_str+= deserialized_data_json + "\n"

            # Collect ack ID for acknowledgment
            ack_ids.append(received_message.ack_id)

        with open(json_file_name , 'a') as f:
            f.write(json_concat_str)
        print("written 25 more records to {}".format(json_file_name))
        # Acknowledge the messages so they won't be sent again
        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )
            
        
    print(json_concat_str)

def main(json_file_name):
    pull_messages(json_file_name)
    pass


if __name__ == "__main__":
    json_file_name = sys.argv[1]
    main(json_file_name)

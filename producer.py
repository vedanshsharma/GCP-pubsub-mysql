import json
import time
import random
from utils import *
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_NAME = os.getenv("TOPIC_NAME")
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


def callback(future):
    try:
        # print(future)
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print("Error publishing message: {}".format(e))


def produce(last_run, current):
    sql = "select * from product "  # where last_updated > '{}'".format(last_run)
    db = dbcon()
    db.connect()
    df = pd.DataFrame(db.execute_query(sql))
    df["last_updated"] = df["last_updated"].astype(
        int
    )  # since time stamp is being accepted as long
    for row_index, row in df.iterrows():
        message = json.dumps(row.to_dict()).encode("utf-8")
        print(message)
        try:
            # print("Here")
            future = publisher.publish(topic_path, data=message)
            future.add_done_callback(callback)
            future.result()
        except Exception as e:
            print(f"Exception encountered: {e}")


def main():
    # get last run
    with open("last_run.json", "r") as json_data:
        d = json.load(json_data)

    current = str(datetime.now()).split(".")[0]

    produce(d["last_run"], current)
    d["last_run"] = current

    with open("last_run.json", "w") as json_data:
        json.dump(d, json_data)


if __name__ == "__main__":
    # print("here")
    main()

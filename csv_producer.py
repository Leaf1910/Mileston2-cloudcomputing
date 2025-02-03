import csv
import os
import json
from google.cloud import pubsub_v1

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sturdy-tine-449202-j2-29cb67b33c8b.json"

# Define project ID and topic ID
project_id = "sturdy-tine-449202-j2"  # Your GCP Project ID
topic_id = "csv-records"  # Your Pub/Sub Topic

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Function to read CSV and publish each row as a message
def publish_csv_data():
    with open('Labels.csv', 'r') as file:
        reader = csv.DictReader(file)  # Read CSV into a dictionary
        for row in reader:
            # Serialize the row dictionary to JSON
            message_json = json.dumps(row)
            message_bytes = message_json.encode("utf-8")

            # Publish the message to the topic
            future = publisher.publish(topic_path, message_bytes)
            print(f"Published: {message_json}")

# Run the producer
publish_csv_data()

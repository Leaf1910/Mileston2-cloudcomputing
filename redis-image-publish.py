import os
import base64
from google.cloud import pubsub_v1

# Set Google Cloud Project ID and Topic Name
PROJECT_ID = "sturdy-tine-449202-j2"
TOPIC_NAME = "image-dataset"  # Just the topic name

# Set authentication (Ensure the JSON file exists)
SERVICE_ACCOUNT_KEY = "sturdy-tine-449202-j2-29cb67b33c8b.json"
if not os.path.exists(SERVICE_ACCOUNT_KEY):
    print(f"Error: Service account key '{SERVICE_ACCOUNT_KEY}' not found.")
    exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Folder containing images
IMAGE_FOLDER = "Dataset_Occluded_Pedestrian"

# Check if folder exists and is not empty
if not os.path.exists(IMAGE_FOLDER) or not os.listdir(IMAGE_FOLDER):
    print(f"Error: Folder '{IMAGE_FOLDER}' does not exist or is empty.")
    exit(1)

# Function to publish images
def publish_images():
    for filename in os.listdir(IMAGE_FOLDER):
        if filename.lower().endswith((".png", ".jpg", ".jpeg")):  # Case-insensitive filter
            image_path = os.path.join(IMAGE_FOLDER, filename)
            
            # Read and encode image
            with open(image_path, "rb") as image_file:
                image_data = base64.b64encode(image_file.read()).decode()  # No utf-8 encoding

            # Publish message
            future = publisher.publish(
                topic_path, 
                data=image_data.encode(),  # Directly encode base64
                image_name=filename  # Pass as an attribute
            )
            print(f"Published {filename}: {future.result()}")  # Confirm publish

# Run the script
if __name__ == "__main__":
    publish_images()

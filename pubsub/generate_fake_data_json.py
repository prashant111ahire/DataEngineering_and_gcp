from google.cloud import pubsub
from faker import Faker
import json
import time

# Replace with your actual Google Cloud project ID and topic name
PROJECT_ID = "PROJECT-NAME"
TOPIC_NAME = "YOUR-TOPIC-NAME"

# Initialize the Pub/Sub client
publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Initialize the Faker generator
fake = Faker()

def generate_fake_data():
    return {
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "phone_number": fake.phone_number(),
        "job_title": fake.job(),
        "birthdate": fake.date_of_birth().strftime("%Y-%m-%d"),
    }

def publish_fake_data():
    while True:
        fake_data = generate_fake_data()
        message_data = json.dumps(fake_data).encode("utf-8")
        publisher.publish(topic_path, data=message_data)
        print("Published message:", fake_data)
        time.sleep(1)  # Simulate streaming delay

if __name__ == "__main__":
    publish_fake_data()
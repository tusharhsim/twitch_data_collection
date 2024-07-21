import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

load_dotenv()
logging.basicConfig(level=logging.INFO)


class NoSQLDatabase:
    """
    A class for interacting with a NoSQL database, specifically for storing Twitch video and chat data.

    Attributes:
        client (MongoClient): The MongoDB client for database operations.
        db (Database): The specific database instance within MongoDB for Twitch data.

    Methods:
        store_video_data(videos): Stores a list of video data in the database.
        store_chat_data(chat_message): Stores a single chat message in the database.
    """

    def __init__(self, uri):
        self.client = MongoClient(uri)
        self.db = self.client.twitch

    def store_video_data(self, videos):
        if videos:
            self.db.videos.insert_many(videos)
            logging.info("Video data stored successfully.")

    def store_chat_data(self, chat_message):
        if chat_message:
            self.db.chat_messages.insert_one(chat_message)
            logging.info("Chat message stored successfully.")


class KafkaMessageHandler:
    """
    A class to handle Kafka interactions, specifically for consuming messages from Kafka topics and processing them.

    Attributes:
        chat_consumer (KafkaConsumer): A Kafka consumer for consuming chat messages from a Kafka topic.
        video_consumer (KafkaConsumer): A Kafka consumer for consuming video data from a Kafka topic.
        db (NoSQLDatabase): The database instance for storing consumed data.
        alert_threshold (int): The threshold of messages that triggers an alert.
        message_count (int): A counter for the number of chat messages received.

    Methods:
        __init__(broker, db, alert_threshold): Initializes the KafkaMessageHandler with specified broker, database, and alert threshold.
        consume_messages(): Consumes chat messages from the Kafka topic and stores them in the database. Triggers an alert if the message count exceeds the threshold.
        consume_video(): Consumes video data from the Kafka topic and stores it in the database.
        send_alert(): Logs an alert message indicating that the chat message volume has exceeded the threshold.
    """

    def __init__(self, broker, db, alert_threshold):
        self.chat_consumer = KafkaConsumer('chat_messages', bootstrap_servers=broker, auto_offset_reset='earliest',
                                           value_deserializer=lambda x: json.loads(x), api_version=(2, 0, 2))
        self.video_consumer = KafkaConsumer('video_data', bootstrap_servers=broker, auto_offset_reset='earliest',
                                            api_version=(2, 0, 2))

        self.db = db
        self.alert_threshold = alert_threshold
        self.message_count = 0

    def consume_messages(self):
        for message in self.chat_consumer:
            self.message_count += 1
            self.db.store_chat_data({'message': message.value})
            if self.message_count > self.alert_threshold:
                self.send_alert()
                self.message_count = 0

    def consume_video(self):
        for video in self.video_consumer:
            self.db.store_video_data(json.loads(video.value.decode('utf-8')).get('data'))

    def send_alert(self):
        logging.info("Alert: Chat message volume exceeded threshold!")


if __name__ == '__main__':
    # load configuration from environment variables
    ALERT_THRESHOLD = int(os.getenv('ALERT_THRESHOLD'))
    MONGO_URI = os.getenv('MONGO_URI')

    db = NoSQLDatabase(MONGO_URI)

    # initialize Twitch API, database connection, and Kafka manager
    kafka_manager = KafkaMessageHandler(['localhost:9092'], db, ALERT_THRESHOLD)

    if not db.client:
        logging.error("Error in initializing mongo DB")
        exit(1)

    # kafka_manager.consume_video()
    kafka_manager.consume_messages()

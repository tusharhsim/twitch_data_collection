import json
import logging
import os
import socket
import sys
import time

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient

load_dotenv()
logging.basicConfig(level=logging.INFO)


class TwitchAPI:
    """
    A class to interact with the Twitch API for fetching user information and videos.

    Attributes:
        client_id (str): The client ID for the Twitch application.
        client_secret (str): The client secret for the Twitch application.
        channel_name (str): The name of the Twitch channel to interact with.
        access_token (str): The OAuth token used for authenticated requests to the Twitch API.
        headers (dict): The headers to include in requests to the Twitch API.
        user_id (str): The Twitch user ID corresponding to the channel_name.

    Methods:
        get_access_token(): Retrieves an OAuth token for authenticated requests.
        get_user_id(): Fetches the user ID for the given channel name.
        rate_limited_request(url): Makes a request to the given URL, handling rate limits.
        get_videos(user_id): Fetches videos for the given user ID.
    """

    def __init__(self, client_id, client_secret, channel_name):
        self.client_id = client_id
        self.client_secret = client_secret
        self.channel_name = channel_name
        self.access_token = self.get_access_token()
        self.headers = {'Client-ID': client_id, 'Authorization': f'Bearer {self.access_token}'}
        self.user_id = self.get_user_id()

    def get_access_token(self):
        try:
            data = f'client_id={self.client_id}&client_secret={self.client_secret}&grant_type=client_credentials'
            resp = requests.post('https://id.twitch.tv/oauth2/token',
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=data)
            return resp.json()['access_token']
        except Exception as e:
            logging.error(f"Error in generating access token: {e}")

    def get_user_id(self):
        try:
            params = {'login': self.channel_name}
            resp = requests.get('https://api.twitch.tv/helix/users', params=params, headers=self.headers)
            return resp.json()['data'][0]['id']
        except Exception as e:
            logging.error(f"Error in getting user/channel id: {e}")

    def rate_limited_request(self, url):
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Ratelimit-Reset', 1))
                logging.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            else:
                response.raise_for_status()
                return response.json()

    def get_videos(self, user_id):
        url = f'https://api.twitch.tv/helix/videos?user_id={user_id}'
        return self.rate_limited_request(url)


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


class KafkaManager:
    """
    A class to manage Kafka interactions, including producing and consuming messages.

    Attributes:
        producer (KafkaProducer): A Kafka producer for sending messages to a Kafka topic.
        consumer (KafkaConsumer): A Kafka consumer for consuming messages from a Kafka topic.

    Methods:
        __init__(broker): Initializes the KafkaManager with a specified broker.
        send_to_kafka(topic, message): Sends a message to a specified Kafka topic.
        consume_messages(callback): Consumes messages from the Kafka topic and processes them using a callback function.
    """

    def __init__(self, broker):
        self.producer = KafkaProducer(bootstrap_servers=broker)
        self.consumer = KafkaConsumer('chat_messages', bootstrap_servers=broker, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='my-group',
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def send_to_kafka(self, topic, message):
        self.producer.send(topic, value=json.dumps(message).encode('utf-8'))
        self.producer.flush()

    def consume_messages(self, callback):
        for message in self.consumer:
            callback(message.value)


class RealTimeAlertSystem:
    """
    A class to monitor chat messages in real-time and trigger alerts when the message volume exceeds a specified threshold.

    Attributes:
        kafka_manager (KafkaManager): An instance of KafkaManager to consume messages from a Kafka topic.
        alert_threshold (int): The threshold of messages that triggers an alert.
        message_count (int): A counter for the number of messages received.

    Methods:
        __init__(self, kafka_manager, alert_threshold): Initializes the RealTimeAlertSystem with a KafkaManager instance and an alert threshold.
        monitor_chat_messages(self): Consumes messages from the Kafka topic and triggers an alert if the message count exceeds the threshold.
        send_alert(self): Logs an alert message indicating that the message volume has exceeded the threshold.
    """

    def __init__(self, kafka_manager, alert_threshold):
        self.kafka_manager = kafka_manager
        self.alert_threshold = alert_threshold
        self.message_count = 0

    def monitor_chat_messages(self):
        def callback(message):
            self.message_count += 1
            if self.message_count > self.alert_threshold:
                self.send_alert()
                self.message_count = 0

        self.kafka_manager.consume_messages(callback)

    def send_alert(self):
        logging.info("Alert: Chat message volume exceeded threshold!")


class TwitchDataCollector:
    """
    A class responsible for collecting and storing Twitch-related data, including chat messages and video information.

    Attributes:
        twitch_api (TwitchAPI): An instance of the TwitchAPI class for making API calls.
        db (NoSQLDatabase): An instance of the NoSQLDatabase class for storing data in a MongoDB database.
        kafka_manager (KafkaManager): An instance of the KafkaManager class for producing and consuming Kafka messages.
        buffer_size (int): The maximum number of chat messages to collect before stopping.

    Methods:
        __init__(self, twitch_api, db, kafka_manager, buffer_size=10000): Initializes the TwitchDataCollector with the necessary components and buffer size.
        store_message_data(self, channel_name): Connects to the Twitch IRC server to collect chat messages for a specific channel.
        collect_and_store_data(self, user_id, channel_name): Collects and stores video data and chat messages for a specific Twitch channel.
    """

    def __init__(self, twitch_api, db, kafka_manager, buffer_size=10000):
        self.twitch_api = twitch_api
        self.db = db
        self.kafka_manager = kafka_manager
        self.buffer_size = buffer_size

    def store_message_data(self, channel_name):
        try:
            channel_name = '#' + channel_name
            msg_length = 256
            with socket.socket() as sock:
                sock.connect((IRC_SERVER, IRC_PORT))
                sock.send(f"PASS {IRC_TOKEN}\n".encode('utf-8'))
                sock.send(f"NICK {IRC_NICKNAME}\n".encode('utf-8'))
                sock.send(f"JOIN {channel_name}\n".encode('utf-8'))

                while True:
                    resp = sock.recv(msg_length).decode('utf-8')
                    if resp.startswith('PING'):
                        sock.send("PONG\n".encode('utf-8'))
                    elif len(resp) > 0:
                        self.db.store_chat_data({'message': resp})
                        self.kafka_manager.send_to_kafka('chat_messages', {'message': resp})
        except Exception as e:
            logging.error(f"Error in retrieving IRC messages: {e}")

    def collect_and_store_data(self, user_id, channel_name):
        try:
            videos = self.twitch_api.get_videos(user_id)
            self.db.store_video_data(videos['data'])
            self.store_message_data(channel_name)
        except Exception as e:
            logging.error(f"Error in storing data: {e}")


if __name__ == '__main__':
    CHANNEL_NAME = sys.argv[1]

    # load configuration from environment variables
    ALERT_THRESHOLD = int(os.getenv('ALERT_THRESHOLD'))
    TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
    TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
    IRC_SERVER = os.getenv('IRC_SERVER')
    IRC_PORT = int(os.getenv('IRC_PORT'))
    IRC_TOKEN = os.getenv('IRC_TOKEN')
    IRC_NICKNAME = os.getenv('IRC_NICKNAME')
    MONGO_URI = os.getenv('MONGO_URI')
    KAFKA_BROKER = os.getenv('KAFKA_BROKER')

    # initialize Twitch API, database connection, and Kafka manager
    twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, CHANNEL_NAME)
    db = NoSQLDatabase(MONGO_URI)
    kafka_manager = KafkaManager(KAFKA_BROKER)

    if not twitch_api.user_id or not db.client:
        logging.error("Error in initializing Twitch API or database.")
        exit(1)

    data_collector = TwitchDataCollector(twitch_api, db, kafka_manager, ALERT_THRESHOLD)
    alert_system = RealTimeAlertSystem(kafka_manager, ALERT_THRESHOLD)

    data_collector.collect_and_store_data(twitch_api.user_id, twitch_api.channel_name)
    alert_system.monitor_chat_messages()

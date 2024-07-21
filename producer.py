import json
import logging
import os
import socket
import sys
import time

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

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


class KafkaMessageProducer:
    """
        A class to manage Kafka interactions, specifically for producing and sending messages to Kafka topics.

        Attributes:
            producer (KafkaProducer): A Kafka producer for sending messages to a Kafka topic.

        Methods:
            __init__(broker): Initializes the KafkaMessageProducer with a specified broker.
            send_to_kafka(topic, message): Sends a message to a specified Kafka topic.
    """

    def __init__(self, broker):
        self.producer = KafkaProducer(bootstrap_servers=broker, api_version=(2, 0, 2))

    def send_to_kafka(self, topic, message):
        self.producer.send(topic, value=json.dumps(message).encode('utf-8'))
        self.producer.flush()


class TwitchDataCollector:
    """
    A class responsible for collecting and storing Twitch-related data, including chat messages and video information.

    Attributes:
        twitch_api (TwitchAPI): An instance of the TwitchAPI class for making API calls.
        kafka_manager (KafkaManager): An instance of the KafkaManager class for producing and consuming Kafka messages.

    Methods:
        __init__(self, twitch_api, db, kafka_manager, buffer_size=10000): Initializes the TwitchDataCollector with the necessary components and buffer size.
        store_message_data(self, channel_name): Connects to the Twitch IRC server to collect chat messages for a specific channel.
        collect_and_store_data(self, user_id, channel_name): Collects and stores video data and chat messages for a specific Twitch channel.
    """

    def __init__(self, twitch_api, kafka_sender):
        self.twitch_api = twitch_api
        self.kafka_manager = kafka_sender

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
                        self.kafka_manager.send_to_kafka('chat_messages', {'message': resp})
        except Exception as e:
            logging.error(f"Error in retrieving IRC messages: {e}")

    def store_video_data(self, user_id):
        try:
            videos = self.twitch_api.get_videos(user_id)
            self.kafka_manager.send_to_kafka('video_data', {'video': videos['data']})
        except Exception as e:
            logging.error(f"Error in retrieving video data: {e}")

    def collect_and_store_data(self, user_id, channel_name):
        self.store_video_data(user_id)
        self.store_message_data(channel_name)


if __name__ == '__main__':
    CHANNEL_NAME = sys.argv[1]

    # load configuration from environment variables
    TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
    TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
    IRC_SERVER = os.getenv('IRC_SERVER')
    IRC_PORT = int(os.getenv('IRC_PORT'))
    IRC_TOKEN = os.getenv('IRC_TOKEN')
    IRC_NICKNAME = os.getenv('IRC_NICKNAME')

    # initialize Twitch API, database connection, and Kafka manager
    twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, CHANNEL_NAME)
    kafka_manager = KafkaMessageProducer(['localhost:9092'])

    if not twitch_api.user_id:
        logging.error("Error in initializing Twitch API")
        exit(1)

    data_collector = TwitchDataCollector(twitch_api, kafka_manager)
    data_collector.collect_and_store_data(twitch_api.user_id, twitch_api.channel_name)

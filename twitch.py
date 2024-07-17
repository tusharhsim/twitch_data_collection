import logging
import os
import socket
import time

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
logging.basicConfig(level=logging.INFO)


class TwitchAPI:
    """
            A class to interact with the Twitch API for accessing channel and video information.

            Attributes:
                client_id (str): The client ID for Twitch API authentication.
                client_secret (str): The client secret for Twitch API authentication.
                channel_name (str): The name of the Twitch channel to interact with.
                access_token (str): The OAuth token obtained for API requests.
                headers (dict): The headers to include in API requests, including authorization.
                user_id (str): The Twitch user ID corresponding to the channel name.

            Methods:
                get_access_token(): Retrieves an access token using the client credentials.
                get_user_id(): Retrieves the user ID for the given channel name.
                rate_limited_request(url): Makes a request to the given URL, handling rate limits.
                get_videos(user_id): Retrieves videos for the given user ID, handling pagination.
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
        client (MongoClient): A MongoClient object for database operations.
        db (Database): The specific database used for storing Twitch data.

    Methods:
        store_video_data(videos): Stores video data in the database.
        store_chat_data(chat_message): Stores chat message data in the database.
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


class TwitchDataCollector:
    """
    A class responsible for collecting and storing Twitch data, including chat messages and video information.

    Attributes:
        twitch_api (TwitchAPI): An instance of the TwitchAPI class for making API requests.
        db (NoSQLDatabase): An instance of the NoSQLDatabase class for database operations.
        buffer_size (int): The maximum number of chat messages to collect before storing.

    Methods:
        store_message_data(channel_name): Collects and stores chat messages for a given channel.
        collect_and_store_data(user_id, channel_name): Collects and stores both video and chat data.
    """

    def __init__(self, twitch_api, db, buffer_size=10000):
        self.twitch_api = twitch_api
        self.db = db
        self.buffer_size = buffer_size

    def store_message_data(self, channel_name):
        try:
            channel_name = '#' + channel_name
            message_count, msg_length = 0, 256
            with socket.socket() as sock:
                sock.connect((IRC_SERVER, IRC_PORT))
                sock.send(f"PASS {IRC_TOKEN}\n".encode('utf-8'))
                sock.send(f"NICK {IRC_NICKNAME}\n".encode('utf-8'))
                sock.send(f"JOIN {channel_name}\n".encode('utf-8'))

                while message_count < self.buffer_size:
                    resp = sock.recv(msg_length).decode('utf-8')
                    if resp.startswith('PING'):
                        sock.send("PONG\n".encode('utf-8'))
                    elif len(resp) > 0:
                        self.db.store_chat_data({'message': resp})
                    message_count += msg_length

                logging.info('Alert: Chat message volume exceeded threshold!')
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
    # an active account for testing
    CHANNEL_NAME = 'jinnytty'

    # load configuration from environment variables
    ALERT_THRESHOLD = int(os.getenv('ALERT_THRESHOLD'))
    TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
    TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
    IRC_SERVER = os.getenv('IRC_SERVER')
    IRC_PORT = int(os.getenv('IRC_PORT'))
    IRC_TOKEN = os.getenv('IRC_TOKEN')
    IRC_NICKNAME = os.getenv('IRC_NICKNAME')
    MONGO_URI = os.getenv('MONGO_URI')

    # initialize Twitch API and database connection
    twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, CHANNEL_NAME)
    db = NoSQLDatabase(MONGO_URI)

    if not twitch_api.user_id or not db.client:
        logging.error("Error in initializing Twitch API or database.")
        exit(1)

    data_collector = TwitchDataCollector(twitch_api, db, ALERT_THRESHOLD)
    data_collector.collect_and_store_data(twitch_api.user_id, twitch_api.channel_name)

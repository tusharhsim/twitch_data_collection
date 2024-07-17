# Twitch Data Collector

This project integrates with the Twitch API and IRC chat to collect and store Twitch chat messages and video data into a MongoDB database. It's designed for developers and researchers interested in analyzing Twitch streaming data.

## Features

- **Twitch API Integration**: Collects video data from specified Twitch channels.
- **IRC Chat Integration**: Collects chat messages in real-time from Twitch IRC servers.
- **MongoDB Storage**: Stores collected video and chat data in a MongoDB database for further analysis.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.6 or higher installed on your system.
- A Twitch account and a registered application on the Twitch Developer Console to obtain `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET`.
- A MongoDB database setup for storing the collected data.

## Configuration

Create a `.env` file in the root directory of the project with the following variables:

```dotenv
IRC_URI='https://twitchapps.com/tmi/'
ALERT_THRESHOLD=3000
TWITCH_CLIENT_ID='your_twitch_client_id'
TWITCH_CLIENT_SECRET='your_twitch_client_secret'
IRC_SERVER='irc.chat.twitch.tv'
IRC_PORT=6667
IRC_TOKEN='your_irc_token'
IRC_NICKNAME='your_irc_nickname'
MONGO_URI='your_mongodb_uri'

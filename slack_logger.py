import os
import time
from datetime import datetime
import re
from slackclient import SlackClient

class ChannelClient:
    def __init__(self, token, channel):
        try:
            self.channel = channel
            self.client = SlackClient(token)
        except:
            pass

    def send(self, message):
        try:
            self.client.api_call(
                "chat.postMessage",
                channel=self.channel,
                text=message
            )
        except:
            pass

class Logger:
    def __init__(self):
        try:
            TOKEN = 'xoxp-258202746933-258063506178-297983833041-2cfa0ee64f660ce7c419fdda83bbbd9e'
            CHANNEL = 'onliner-log'
            self.client = ChannelClient(TOKEN, CHANNEL)
        except:
            pass

    def info(self, message):
        self.client.send('[{0}] {1}'.format(str(datetime.now()), message))
    
    def infoNoError(self, message):
        try:
            self.info(message)
        except:
            pass

if __name__ == '__main__':
    logger = Logger()
    logger.infoNoError('started')
    logger.infoNoError('finished')
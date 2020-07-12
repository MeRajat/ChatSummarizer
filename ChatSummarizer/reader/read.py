import os
from slack import WebClient
from slack.errors import SlackApiError
from datetime import datetime, timedelta
from typing import Dict


class SlackReader:
    def __init__(self, key: str = None):
        assert key is not None
        self.client = WebClient(key)

    def join_channel(self, channel: str = None):
        raise NotImplementedError

    @staticmethod
    def get_time_to_start_from(days: int):
        delta = datetime.today() - timedelta(days)
        return delta.timestamp()

    def parse_thread(self, channel: str, ts: str):
        replies = self.client.conversations_replies(channel=channel, ts=ts)
        replies_text = []
        for reply in replies.data["messages"]:
            text = reply.get("text", "")
            replies_text.append(text)
        return replies_text

    def parse_messages(self, channel: str, data: Dict):
        messages_text = []
        if data["ok"]:
            ## Iter through messages and create a list of list with thred
            for message in data["messages"]:
                message_thread = []
                parent_message = message["text"]
                message_thread.append(parent_message)
                reply_count = message.get("reply_count", 0)
                if reply_count > 0:
                    ts = message.get("ts")
                    assert ts is not None
                    replies_text = self.parse_thread(channel, ts)
                    message_thread.append(replies_text)
                messages_text.append(message_thread)
        return messages_text

    def read_history(self, channel: str, days: int = 1):
        assert channel is not None
        ts = self.get_time_to_start_from(days=days)
        res = self.client.conversations_history(channel=channel, oldest=ts)
        ## Parse messages
        messages_text = self.parse_messages(channel=channel, data=res.data)
        return messages_text


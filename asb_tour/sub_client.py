import asyncio
import json
from types import SimpleNamespace
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient

class SubscriptionClient(object):
    def __init__(self, conn_str, topic_name, sub_name):
        self._conn_str = conn_str
        self.topic_name = topic_name
        self.sub_name = sub_name
        self._sequence_number = 0
        self._loop = asyncio.get_event_loop()
        self._messages = []

    def _getbus(self):
        return ServiceBusClient.from_connection_string(conn_str=self._conn_str)

    async def _peek_loop(self, count):
        async with self._getbus() as bus:
            receiver = bus.get_subscription_receiver(
                topic_name=self.topic_name,
                subscription_name=self.sub_name,
                prefetch=count
            )
            async with receiver:
                received_msgs = await receiver.peek(message_count=count, sequence_number=self._sequence_number)
                for msg in received_msgs:
                    self._messages.append(self._tomsg(msg))
                    self._sequence_number = msg.sequence_number + 1

    def clear(self):
        self._messages = []
        self._sequence_number = 0

    @property
    def message_count(self):
        return len(self._messages)

    def find_message(self, msgid):
        return next(
            (x for x in self._messages if x.message_id == msgid),
            None)

    def peek(self, count=50):
        self._loop.run_until_complete(self._peek_loop(count))
        return self._messages

    def _fmt_msg_body(self, msg):
        try:
            return json.loads(str(msg))
        except Exception as e:
            return str(msg)

    def _tomsg(self, msg):
        sp = msg.properties
        m = dict(
            message_id = sp.message_id.decode('utf-8'),
            sequence_number = msg.sequence_number,
            enqueued_time_utc = msg.enqueued_time_utc,
            label = sp.subject.decode('utf-8') if sp.subject is not None else '',
            size = msg.message.get_message_encoded_size(),
            body = self._fmt_msg_body(msg)
        )
        ap = self._get_user_props(msg)
        ap.update(self._get_system_props(msg))
        m['all_properties'] = ap
        return SimpleNamespace(**m)

    def _get_system_props(self, msg):
        sp = msg.properties
        bp = dict(
            message_id = sp.message_id.decode('utf-8'),
            label = sp.subject.decode('utf-8') if sp.subject is not None else '',
            content_type = sp.content_type.decode('utf-8') if sp.content_type else '',
            creation_time = str(sp.creation_time) if sp.creation_time else '',
            content_encoding = sp.content_encoding.decode('utf-8') if sp.content_encoding else '',
            correlation_id = sp.correlation_id.decode() if sp.correlation_id else '',
            to = sp.to.decode() if sp.to else '',
            reply_to = sp.reply_to.decode() if sp.reply_to else '',
            user_id = sp.user_id.decode()  if sp.user_id else '',
            size = msg.message.get_message_encoded_size()
        )
        for key,value in msg.annotations.items():
            val = value
            if isinstance(value, str):
                val = value
            elif isinstance(value, bytes):
                val = value.decode('utf-8')
            bp[key.decode()] = val
        return bp

    def _get_user_props(self, msg):
        up = dict()
        if msg.user_properties is None:
            return up
        for key, value in msg.user_properties.items():
            val = value
            if isinstance(value, str):
                val = value
            elif isinstance(value, bytes):
                val = value.decode('utf-8')
            up[key.decode("utf-8")] = val
        return up


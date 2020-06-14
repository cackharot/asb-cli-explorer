import asyncio
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient

class SubscriptionClient(object):
    def __init__(self, conn_str):
        self._conn_str = conn_str
        self._sequence_number = 0
        self._loop = asyncio.get_event_loop()
        self._messages = []

    def _getbus(self):
        return ServiceBusClient.from_connection_string(conn_str=self._conn_str)

    async def _peek_loop(self, tp_name, sub_name):
        async with self._getbus() as bus:
            receiver = bus.get_subscription_receiver(
                topic_name=tp_name,
                subscription_name=sub_name,
                prefetch=50
            )
            async with receiver:
                received_msgs = await receiver.peek(message_count=50, sequence_number=self._sequence_number)
                for msg in received_msgs:
                    self._messages.append(msg)
                    self._sequence_number = msg.sequence_number + 1

    def clear(self):
        self._messages = []
        self._sequence_number = 0

    @property
    def message_count(self):
        return len(self._messages)

    def find_message(self, seqno):
        l = [x for x in self._messages if x.sequence_number == seqno]
        if len(x) == 1:
            return l[0]
        return None

    def messages(self, tp_name, sub_name):
        self._loop.run_until_complete(self._peek_loop(tp_name, sub_name))
        return self._messages

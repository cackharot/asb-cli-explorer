import asyncio
import json
from types import SimpleNamespace
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient
from asb_tour.sub_client import SubscriptionClient

class DlqClient(SubscriptionClient):
    async def _peek_loop(self, count):
        async with self._getbus() as bus:
            receiver = bus.get_subscription_deadletter_receiver(
                topic_name=self.topic_name,
                subscription_name=self.sub_name,
                prefetch=count
            )
            async with receiver:
                received_msgs = await receiver.peek(message_count=count, sequence_number=self._sequence_number)
                for msg in received_msgs:
                    self._messages.append(self._tomsg(msg))
                    self._sequence_number = msg.sequence_number + 1

    def peek(self, count):
        self._loop.run_until_complete(self._peek_loop(count))
        return self._messages

    def messages(self):
        return []

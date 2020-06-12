import os
import asyncio
import sys
import json
from azure.servicebus.aio import ServiceBusClient

CONNECTION_STR = os.environ['SB_CONN_STR']
TOPIC_NAME = os.environ.get("SB_TOPIC_NAME", '')
SUBSCRIPTION_NAME = os.environ.get("SB_SUBSCRIPTION_NAME", '')
SHOW_BROKER_PROPS = bool(os.environ.get('SB_SHOW_BROKER_PROPS', None))
SHOW_USER_PROPS = bool(os.environ.get('SB_SHOW_USER_PROPS', None))

def getmsgprops(msg):
  bp = dict()
  for key, value in msg.properties.__dict__.items():
    bp[key] = str(value)
  return bp

def getusrprops(msg):
  up = dict()
  for key, value in msg.user_properties.items():
    up[key.decode("utf-8")] = value.decode("utf-8")
  return up

async def main():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

    async with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            prefetch=10
        )
        async with receiver:
            received_msgs = await receiver.receive(max_batch_size=10, max_wait_time=5)
            for msg in received_msgs:
                pl = json.loads(str(msg))
                if SHOW_USER_PROPS:
                  pl['user_props'] = getusrprops(msg)
                if SHOW_BROKER_PROPS:
                  pl['broker_props'] = getmsgprops(msg)
                print(json.dumps(pl), flush=True)
                await msg.abandon()

if len(TOPIC_NAME) <= 0 or len(SUBSCRIPTION_NAME) <= 0:
  print('Provide SB_TOPIC_NAME, SB_SUBSCRIPTION_NAME')
  sys.exit(-1)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())


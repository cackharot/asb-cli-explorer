import os
import json
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient

def getmsgprops(msg):
  bp = dict()
  for key, value in msg.properties.__dict__.items():
    bp[key] = str(value)
  return bp

def getusrprops(msg):
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

def display_msg(msg, settings):
  pl = dict()
  try:
    pl = json.loads(str(msg))
  except Exception as e:
    pl['raw_body'] = str(msg)

  if settings.show_user_props:
    pl['user_props'] = getusrprops(msg)
  if settings.show_broker_props:
    pl['broker_props'] = getmsgprops(msg)
  print(json.dumps(pl), flush=True)

async def main_loop(settings):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=settings.conn_str)

    async with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(
            topic_name=settings.topic,
            subscription_name=settings.subscription,
            prefetch=10
        )
        async with receiver:
            received_msgs = await receiver.receive(max_batch_size=10, max_wait_time=5)
            for msg in received_msgs:
              display_msg(msg, settings)
              await msg.abandon()


async def send_msg(settings, body, user_props):
  client = ServiceBusClient.from_connection_string(conn_str=settings.conn_str)
  async with client:
    sender = client.get_topic_sender(topic_name=settings.topic)
    async with sender:
      msg = Message(body, subject=user_props.get('label', None))
      if 'label' in user_props:
        user_props.pop('label')
      msg.user_properties = user_props
      await sender.send(msg)
      print(f"Successfully sent message to topic '{settings.topic}'")
  pass

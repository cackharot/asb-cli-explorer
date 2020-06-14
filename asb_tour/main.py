import os
import functools
import signal
import asyncio
import json
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient

def getmsgprops(msg):
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
    print(e)
    pl['raw_body'] = str(msg)

  if settings.show_user_props:
    pl['user_props'] = getusrprops(msg)
  if settings.show_system_props:
    pl['system_props'] = getmsgprops(msg)
  try:
    print(json.dumps(pl), flush=True)
  except Exception as e:
    print(e)
    print(str(msg))

def ask_exit(signame, loop, cancel_request):
    cancel_request.set()

def add_signals(loop, cancel_request):
  for signame in {'SIGINT', 'SIGTERM'}:
    loop.add_signal_handler(
        getattr(signal, signame),
        functools.partial(ask_exit, signame, loop , cancel_request))

async def peek_loop(settings):
    loop = asyncio.get_running_loop()
    cancel_request_event = asyncio.Event()
    add_signals(loop, cancel_request_event)
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=settings.conn_str)

    async with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(
            topic_name=settings.topic,
            subscription_name=settings.subscription,
            prefetch=10
        )
        async with receiver:
            sno = 0
            while not cancel_request_event.is_set():
              received_msgs = await receiver.peek(message_count=10, sequence_number=sno)
              for msg in received_msgs:
                display_msg(msg, settings)
                sno = msg.sequence_number + 1
              await asyncio.sleep(0.25)


async def send_msg(settings, body, user_props, system_props):
  client = ServiceBusClient.from_connection_string(conn_str=settings.conn_str)
  async with client:
    sender = client.get_topic_sender(topic_name=settings.topic)
    async with sender:
      msg = Message(body, subject=system_props.get('Label', None))
      msg.annotations = system_props
      msg.user_properties = user_props
      await sender.send(msg)
      print(f"Successfully sent message to topic '{settings.topic}'")
  pass

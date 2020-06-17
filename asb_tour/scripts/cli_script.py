import click
import asyncio
import json
from types import SimpleNamespace
from asb_tour.main import peek_loop, send_msg
from asb_tour.explorer import tui_app
from asb_tour.topic_client import TopicClient
from asb_tour.sub_client import SubscriptionClient
from asb_tour.dlq_client import DlqClient

@click.group()
def cli():
    pass

@cli.command('peek', short_help='Receive messages from a subscription')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.option('--subscription', required=True, help='Topic name')
@click.option('--show-user-props', is_flag=True, default=False, help='Show user properties on message?')
@click.option('--show-system-props', is_flag=True, default=False, help='Show system properties on message?')
def peek(conn_str, topic, subscription, show_user_props, show_system_props):
    opt = dict(
        conn_str=conn_str,
        topic=topic,
        subscription=subscription,
        show_user_props=show_user_props,
        show_system_props=show_system_props
    )
    settings = SimpleNamespace(**opt)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(peek_loop(settings))
    pass

@cli.command('send', short_help='Send messages to a topic')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.option('--props', default=None, help='User properties as keyvalue pairs')
@click.option('--sys-props', default=None, help='System properties as keyvalue pairs')
@click.option('--data-file', default=None, type=click.File('r'), help='File path , message payload')
@click.argument('msg', required=False, metavar='<msg>')
def send(conn_str, topic, props, sys_props, data_file, msg):
    """
    Send the given message with user properties to the {topic}
    <props> Message user properties e.g, key1=val1,key2=val2'
    """
    if data_file:
        msg = data_file.read()
    user_props = dict()
    system_props = dict()
    if props is not None:
        user_props = dict([kv.split('=') for kv in props.split(',') if '=' in kv])
    if sys_props is not None:
        system_props = dict([kv.split('=') for kv in sys_props.split(',') if '=' in kv])
    settings = SimpleNamespace(**dict(conn_str=conn_str,topic=topic))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_msg(settings, msg, user_props, system_props))
    pass

@cli.command('explore')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker. Must be a management key!')
def explore(conn_str):
    """
    Opens a TUI to explore the topics and subscription details,
    also peek subscriptions messages and it's DLQ
    """
    tui_app(conn_str)
    pass

@cli.command('list')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker. Must be a management key!')
def list(conn_str):
    """
    List available topics and its subscriptions
    """
    tc = TopicClient(conn_str)
    data = dict()
    for t,sub in tc.topics():
        data[t] = [s.name for s in sub]
    click.echo(json.dumps(data))

@cli.group()
def dlq():
    """
    Commands to process Dead Letter Queues. Has subcommands, peek and move, purge
    """
    pass

@dlq.command('peek')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker. Must be a management key!')
@click.option('--topic', required=True, help='Topic name')
@click.option('--sub', required=True, help='Subscripton name')
@click.option('--count', required=True, help='Number of messages to peek', type=int)
def dlq_peek(conn_str, topic, sub, count):
    """
    Peek given number of messages from the DLQ
    """
    dc = DlqClient(conn_str, topic, sub)
    msgs = dc.peek(count)
    for msg in msgs:
        d = msg.__dict__
        print(json.dumps(d, sort_keys=True, default=str), flush=True)
    pass

@dlq.command('move')
def dlq_move():
    """
    Move the list of message_id's from DLQ to the given topic
    """
    pass

@dlq.command('purge')
def dlq_purge():
    """
    Deletes all messages from DLQ
    """
    pass

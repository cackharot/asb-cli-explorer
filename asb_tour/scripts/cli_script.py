import click
import asyncio
import json
from types import SimpleNamespace
from asb_tour.main import peek_loop, send_msg
from asb_tour.explorer import tui_app
from asb_tour.topic_client import TopicClient

@click.group()
def cli():
    pass

@cli.command('peek', short_help='Receive messages from a subscription')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.option('--subscription', required=True, help='Topic name')
@click.option('--show-user-props', is_flag=True, default=False, help='Show user properties on message?')
@click.option('--show-broker-props', is_flag=True, default=False, help='Show broker properties on message?')
def peek(conn_str, topic, subscription, show_user_props, show_broker_props):
    opt = dict(
        conn_str=conn_str,
        topic=topic,
        subscription=subscription,
        show_user_props=show_user_props,
        show_broker_props=show_broker_props
    )
    settings = SimpleNamespace(**opt)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(peek_loop(settings))
    pass

@cli.command('send', short_help='Send messages to a topic')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.option('--props', default=None, help='User properties as keyvalue pairs')
@click.option('--data-file', default=None, type=click.File('r'), help='File path , message payload')
@click.argument('msg', required=False, metavar='<msg>')
def send(conn_str, topic, props, data_file, msg):
    """
    Send the given message with user properties to the {topic}
    <props> Message user properties e.g, key1=val1,key2=val2'
    """
    if data_file:
        msg = data_file.read()
    user_props = dict()
    if props is not None:
        user_props = dict([kv.split('=') for kv in props.split(',') if '=' in kv])
    settings = SimpleNamespace(**dict(conn_str=conn_str,topic=topic))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_msg(settings, msg, user_props))
    pass

@cli.command('explore')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker. Must be a management key!')
def explore(conn_str):
    tui_app(conn_str)
    pass

@cli.command('list')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker. Must be a management key!')
def list(conn_str):
    tc = TopicClient(conn_str)
    click.echo(json.dumps(tc.topics()))

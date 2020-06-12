import click
import asyncio
from types import SimpleNamespace
from asb_tour.main import main_loop, send_msg

@click.group()
def cli():
    pass

@cli.command('receive', short_help='Receive messages from a subscription')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.option('--subscription', required=True, help='Topic name')
@click.option('--show-user-props', is_flag=True, default=False, help='Show user properties on message?')
@click.option('--show-broker-props', is_flag=True, default=False, help='Show broker properties on message?')
def receive(conn_str, topic, subscription, show_user_props, show_broker_props):
    loop = asyncio.get_event_loop()
    opt = dict(
        conn_str=conn_str,
        topic=topic,
        subscription=subscription,
        show_user_props=show_user_props,
        show_broker_props=show_broker_props
    )
    settings = SimpleNamespace(**opt)
    loop.run_until_complete(main_loop(settings))
    pass

@cli.command('send', short_help='Send messages to a topic')
@click.option('--conn-str', required=True, envvar='SB_CONN_STR', help='Connection string to the Azure Service bus broker')
@click.option('--topic', required=True, help='Topic name')
@click.argument('props', default=None, metavar='<props>')
@click.argument('msg', required=True, metavar='<msg>')
def send(conn_str, topic, props, msg):
    """
    Send the given message with user properties to the {topic}
    <props> Message user properties e.g, key1=val1,key2=val2'
    """
    user_props = dict()
    if props is not None:
        user_props = dict([kv.split('=') for kv in props.split(',') if '=' in kv])
    settings = SimpleNamespace(**dict(conn_str=conn_str,topic=topic))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_msg(settings, msg, user_props))
    pass

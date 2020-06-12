import click
import asyncio
from types import SimpleNamespace
from asb_tour.main import main_loop

@click.command()
@click.option('--topic', required=True, help='Topic name')
@click.option('--subscription', required=True, help='Topic name')
@click.option('--show-user-props', is_flag=True, default=False, help='Show user properties on message?')
@click.option('--show-broker-props', is_flag=True, default=False, help='Show broker properties on message?')
def cli(topic, subscription, show_user_props, show_broker_props):
    #  validate(topic, subscription)
    loop = asyncio.get_event_loop()
    opt = dict(
        topic=topic,
        subscription=subscription,
        show_user_props=show_user_props,
        show_broker_props=show_broker_props
    )
    settings = SimpleNamespace(**opt)
    loop.run_until_complete(main_loop(settings))

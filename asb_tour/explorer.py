import npyscreen
import functools
import asyncio
import curses
from azure.servicebus import Message
from azure.servicebus.aio import ServiceBusClient

MSG_PAYLOAD_VIEW_FRM = 'MSG_PAYLOAD_VIEW_FRM'

class MessageRepo(object):
    def __init__(self, conn_str, topic, subscription):
        self.conn_str = conn_str
        self.topic = topic
        self.subscription = subscription
        self.sequence_number = 0
        self.messages = []
        self.loop = asyncio.get_event_loop()

    async def peek_loop(self):
        servicebus_client = ServiceBusClient.from_connection_string(conn_str=self.conn_str)

        async with servicebus_client:
            receiver = servicebus_client.get_subscription_receiver(
                topic_name=self.topic,
                subscription_name=self.subscription,
                prefetch=50
            )
            async with receiver:
                received_msgs = await receiver.peek(message_count=50, sequence_number=self.sequence_number)
                for msg in received_msgs:
                    self.messages.append(msg)
                    self.sequence_number = msg.sequence_number + 1

    def clear(self):
        self.messages = []
        self.sequence_number = 0

    def get_messages(self):
        self.loop.run_until_complete(self.peek_loop())
        return self.messages

class MessageList(npyscreen.MultiLineAction):
    def __init__(self, *args, **keywords):
        super(MessageList, self).__init__(*args, **keywords)
        self.add_handlers({
            curses.ascii.ESC: self.when_exit,
            'q': self.when_exit,
            "^R": self.when_refresh,
            "^K": self.when_clear,
            "^D": self.when_view
        })

    def display_value(self, msg):
        return "%s\t%s\t\t%s" % (msg.sequence_number, str(msg)[:50], msg.enqueued_time_utc)

    def actionHighlighted(self, highlighted_msg, keypress):
        self.parent.parentApp.getForm(MSG_PAYLOAD_VIEW_FRM).value = highlighted_msg
        self.parent.parentApp.switchForm(MSG_PAYLOAD_VIEW_FRM)

    def when_refresh(self, *args, **keywords):
        self.parent.update_list()

    def when_clear(self, *args, **keywords):
        self.parent.parentApp.subscription.clear()
        self.parent.update_list()

    def when_view(self, *args, **keywords):
        self.parent.update_list()

    def when_exit(self, *args, **keywords):
        curses.beep()
        self.parent.parentApp.setNextForm(None)
        self.parent.parentApp.switchForm(None)
        self.editing = False

class TopicsColumn(npyscreen.BoxTitle):
    pass

class MessagesColumn(npyscreen.BoxTitle):
    _contained_widget = MessageList
    pass

class MainLayout(npyscreen.FormBaseNew):
    def create(self):
        self.how_exited_handers[npyscreen.wgwidget.EXITED_ESCAPE] = self.exit_application
        h, w = terminal_dimensions()
        self.add(TopicsColumn,
                 name='Topics & Subscriptions',
                 relx = 2,
                 rely = 2,
                 max_width = 30,
                 max_height = h - 5)
        self.wMain = self.add(MessagesColumn,
                 name='MESSAGES',
                 relx = 32,
                 rely = 2,
                 scroll_exit = True,
                 max_height = h - 5)

    def beforeEditing(self):
        self.update_list()

    def update_list(self):
#       npyscreen.notify('Peeking messages in subscription', title='Please wait!')
        self.wMain.footer = 'Peeking messages...'
        self.wMain.values = self.parentApp.subscription.get_messages()
        self.wMain.footer = "Messages Count: %d" % len(self.parentApp.subscription.messages)
        self.wMain.display()

    def exit_application(self):
        curses.beep()
        self.parentApp.setNextForm(None)
        self.editing = False

def terminal_dimensions():
    return curses.initscr().getmaxyx()

class MsgExplorerApp(npyscreen.NPSAppManaged):
    def __init__(self, conn_str, *args, **kwargs):
        super(MsgExplorerApp, self).__init__(*args, **kwargs)
        self.conn_str = conn_str
        self.subscription = MessageRepo(self.conn_str, 'test-tp', 'log')

    def onStart(self):
        self.addForm("MAIN", MainLayout, name = "Azure Service Bus Explorer")
        self.addForm(MSG_PAYLOAD_VIEW_FRM, MessageViewRecord)

class MessageViewRecord(npyscreen.ActionForm):
    def create(self):
        self.value = None
        self.wgBody = self.add(npyscreen.TitleText, name = "Body:",)

    def beforeEditing(self):
        if self.value:
            msg = self.value
            self.name = "Message id : %s" % (msg.sequence_number)
#            self.message_id         = msg.message_id
            self.seq_no             = msg.sequence_number
            self.wgBody.value       = str(msg)
        else:
            self.name = "New Message"
#            self.message_id     = ''
            self.seq_no         = ''
            self.wgBody.value   = ''

    def on_ok(self):
        self.parentApp.switchFormPrevious()

    def on_cancel(self):
        self.parentApp.switchFormPrevious()

def run_tui(conn_str, *args):
    #npyscreen.setTheme(npyscreen.Themes.ColorfulTheme)
    npyscreen.setTheme(npyscreen.Themes.TransparentThemeDarkText)
    app = MsgExplorerApp(conn_str)
    app.run()

def tui_app(conn_str):
    npyscreen.wrapper_basic(functools.partial(run_tui, conn_str))
    print('Done')

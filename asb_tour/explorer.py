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

class MessageList(npyscreen.GridColTitles):
    default_column_number = 3
    def __init__(self, *args, **keywords):
        super(MessageList, self).__init__(*args, **keywords)
        self.columns = 3
        self.col_margin = 0
        self.select_whole_line = True
        self.on_select_callback = self.selected
        self.col_titles = ['SeqNo', "Body", "Enqueued Time"]

    def set_up_handlers(self):
        super(MessageList, self).set_up_handlers()
        self.handlers.update({
            ord('q'): self.when_exit,
            curses.ascii.NL: self.when_view,
            curses.ascii.CR: self.when_view
        })

    def selected(self):
        row = self.selected_row()
        msg = self.parent.get_message(row[0])
        if msg is None:
            return
        # TODO: Show message details in bottom pane

    def when_view(self, *args, **keywords):
        row = self.selected_row()
        msg = self.parent.get_message(row[0])
        if msg is None:
            return
        self.parent.parentApp.getForm(MSG_PAYLOAD_VIEW_FRM).value = msg
        self.parent.parentApp.switchForm(MSG_PAYLOAD_VIEW_FRM)

    def when_exit(self, *args, **keywords):
        curses.beep()
        self.parent.parentApp.setNextForm(None)
        self.editing = False
        self.parent.parentApp.switchFormNow()

class TopicsColumn(npyscreen.BoxTitle):
    pass

class MessagesColumn(npyscreen.BoxTitle):
    _contained_widget = MessageList
    pass

class MainLayout(npyscreen.FormBaseNew):
    def create(self):
        h, w = terminal_dimensions()
        self.add(TopicsColumn,
                 name='Topics & Subscriptions',
                 relx = 2,
                 rely = 2,
                 max_width = 30,
                 max_height = h - 4)
        self.wMain = self.add(MessagesColumn,
                 name='MESSAGES',
                 relx = 32,
                 rely = 2,
                 scroll_exit = False,
                 max_height = h - 4)
        self.update_list()

    def set_up_handlers(self):
        super(MainLayout, self).set_up_handlers()
        self.handlers.update({
            ord("q"): self.h_exit,
            curses.ascii.ESC: self.h_exit,
            "^R": self.h_refresh,
            "^K": self.h_clear,
        })

    def h_clear(self, *args, **keywords):
        self.parentApp.subscription.clear()
        self.wMain.values = []
        self.wMain.footer = 'Messges Count: %d'
        self.wMain.display()
        #self.update_list()

    def h_refresh(self, *args, **keywords):
        self.update_list()

    def h_exit(self, *args, **keywords):
        curses.beep()
        self.parentApp.setNextForm(None)
        self.editing = False
        self.parentApp.switchFormNow()

    def beforeEditing(self):
        #self.update_list()
        pass

    def update_list(self):
#        npyscreen.notify('Peeking messages in subscription', title='Please wait!')
        self.wMain.footer = 'Peeking messages...'
        lst = self.parentApp.subscription.get_messages()
        self.wMain.values = [[x.sequence_number, str(x)[:50], x.enqueued_time_utc] for x in lst]
        self.wMain.footer = "Messages Count: %d" % len(self.parentApp.subscription.messages)
        self.wMain.display()

    def get_message(self, seq_no):
        l = [x for x in self.parentApp.subscription.messages if x.sequence_number == seq_no]
        if len(l) == 1:
            return l[0]
        else:
            return None

def terminal_dimensions():
    return curses.initscr().getmaxyx()

class MsgExplorerApp(npyscreen.NPSAppManaged):
    def __init__(self, conn_str, *args, **kwargs):
        super(MsgExplorerApp, self).__init__(*args, **kwargs)
        self.conn_str = conn_str
        self.subscription = MessageRepo(self.conn_str, 'test-tp', 'log')
        self.keypress_timeout_default = 3

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
    npyscreen.setTheme(npyscreen.Themes.TransparentThemeLightText)
    app = MsgExplorerApp(conn_str)
    app.run()

def tui_app(conn_str):
    npyscreen.wrapper_basic(functools.partial(run_tui, conn_str))
    print('Done')

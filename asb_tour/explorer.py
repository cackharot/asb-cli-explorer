import npyscreen
import functools
import asyncio
import curses
import json

from asb_tour.topic_client import TopicClient
from asb_tour.sub_client import SubscriptionClient

MSG_PAYLOAD_VIEW_FRM = 'MSG_PAYLOAD_VIEW_FRM'

class MessageList(npyscreen.GridColTitles):
    default_column_number = 5
    def __init__(self, *args, **keywords):
        super(MessageList, self).__init__(*args, **keywords)
        self.col_margin = 0
        self.row_height = 1
        #self.column_width_requested = 25
        self.select_whole_line = True
        self.on_select_callback = self.selected
        self.col_titles = ['MessageId', 'SeqNo', 'Label', 'Size', 'Enqueued Time']

    def set_up_handlers(self):
        super(MessageList, self).set_up_handlers()
        self.handlers.update({
            ord('q'): self.when_exit,
            curses.ascii.NL: self.when_view,
            curses.ascii.CR: self.when_view
        })

    def selected(self):
        row = self.selected_row()
        msg = self.parent.selected_message(row[0])

    def when_view(self, *args, **keywords):
        row = self.selected_row()
        msg = self.parent.selected_message(row[0])

    def when_exit(self, *args, **keywords):
        curses.beep()
        self.parent.parentApp.setNextForm(None)
        self.editing = False
        self.parent.parentApp.switchFormNow()

class TopicsTreeWidget(npyscreen.MLTreeAction):
    def actionHighlighted(self, treenode, key_press):
        if key_press != curses.ascii.NL:
            return
        if treenode.hasChildren():
            # topic or ns selected
            return
        sub_name = treenode.content.split(' ')[0]
        topic_name = treenode.getParent().content
        self.parent.fetch_messages_request(topic_name, sub_name)

class TopicsColumn(npyscreen.BoxTitle):
    _contained_widget = TopicsTreeWidget
    pass

class MessagesColumn(npyscreen.BoxTitle):
    _contained_widget = MessageList
    pass

class MessageDetailPane(npyscreen.BoxTitle):
    _contained_widget = npyscreen.Pager

    def set_up_handlers(self):
        super(MessageDetailPane, self).set_up_handlers()
        self.handlers.update({
            ord("q"): self.h_exit,
            curses.ascii.ESC: self.h_exit,
            "^Q": self.h_exit,
        })

    def h_exit(self, *args, **keywords):
        curses.beep()
        self.editing = False
        self.parent.parentApp.switchFormNow()

class MainLayout(npyscreen.FormBaseNew):
    def create(self):
        h, w = terminal_dimensions()
        mh = int(h*0.45)
        self.wTopics = self.add(TopicsColumn,
                 name='Topics & Subscriptions',
                 relx = 2,
                 rely = 2,
                 max_width = 30,
                max_height = h - 4,
                scroll_exit=False,
                exit_right=True)
        self.wMain = self.add(MessagesColumn,
                    name='MESSAGES',
                    relx = 32,
                    rely = 2,
                    editable=True,
                    scroll_exit = False,
                    column_width = 20,
                    max_height = mh)
        self.wMsgDetail = self.add(MessageDetailPane,
                              name="Message Details",
                              relx=32,
                              rely = mh+2,
                              scroll_exit=True,
                              max_height = h-mh-4,
                              editable=True,
                              center=False,
                              autowrap=False)
        self.subclient = SubscriptionClient(self.parentApp.conn_str)
        self.update_request = False
        self.update_messages_request = False
        self.h_clear()
        self.update_list()

    def set_up_handlers(self):
        super(MainLayout, self).set_up_handlers()
        self.handlers.update({
            ord("q"): self.h_exit,
            curses.ascii.ESC: self.h_exit,
            "^Q": self.h_exit,
            "^R": self.h_refresh,
            "^K": self.h_clear,
        })

    def h_clear(self, *args, **keywords):
        self.subclient.clear()
        self.wTopics.footer = ""
        self.wMain.values = []
        self.wMain.footer = 'Messges Count: 0'
        self.wMsgDetail.values = """
Select a subscription, messages will be displayed above and
you can scroll through the messages to see the full payload
and user/system properties here.
* Press 'Ctrl+R' to refresh topics and messages.
* Press 'Ctrl+K' to clear topics and messages.
* Press ESC or 'q' or 'Ctrl+Q' to quit the application.
        """.split("\n")
        self.display()

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

    def while_waiting(self):
        if self.update_request:
            self.fetch_topics()
            self.update_request = False

        if self.update_messages_request:
            self.fetch_messages(self.topic_name, self.sub_name)
            self.update_messages_request = False

    def fetch_topics(self):
        tp = self.parentApp.tp_client
        lst = tp.topics()
        treedata = npyscreen.NPSTreeData(content=tp.namespace, selectable=True,ignoreRoot=False)
        tpc = subc = 0
        for topic_name,sb_lst in lst:
            t = treedata.newChild(content=topic_name, selectable=False, selected=False)
            tpc = tpc + 1
            for sb in sb_lst:
                title = "%s (%d)" % (sb.name, sb.message_count)
                t.newChild(content=title, selectable=True)
                subc = subc + 1
        self.wTopics.values = treedata
        self.wTopics.footer = "Topics (%s), Subs (%d)" % (tpc, subc)
        self.wTopics.display()

    def fetch_messages_request(self, topic_name, sub_name):
        self.topic_name = topic_name
        self.sub_name = sub_name
        self.update_messages_request = True
        self.wMain.footer = 'Peeking messages...'
        self.wMain.display()

    def fetch_messages(self, topic_name='test-tp', sub_name='log'):
        lst = self.subclient.messages(topic_name, sub_name)
        self.wMain.values = [
            [
                x.message_id,
                x.sequence_number,
                x.label,
                x.size,
                x.enqueued_time_utc
            ] for x in lst]
        self.wMain.footer = "Messages Count: %d" % self.subclient.message_count
        self.wMain.editable = self.subclient.message_count > 0
        self.wMain.display()

    def update_list(self):
        self.wTopics.footer = 'Loading...'
        self.wMain.editable = False
        self.wMsgDetail.editable = False
        self.update_request = True
        self.display()

    def selected_message(self, msgid):
        msg = self.subclient.find_message(msgid)
        self.wMsgDetail.values = msg.body.split('\n') if msg.body else ''
        if not self.wMsgDetail.editable:
            self.wMsgDetail.editable = True
        self.wMsgDetail.display()
        return msg

def terminal_dimensions():
    return curses.initscr().getmaxyx()

class MsgExplorerApp(npyscreen.NPSAppManaged):
    def __init__(self, conn_str, *args, **kwargs):
        super(MsgExplorerApp, self).__init__(*args, **kwargs)
        self.conn_str = conn_str
        self.tp_client = TopicClient(self.conn_str)
        self.keypress_timeout_default = 3

    def onStart(self):
        self.addForm("MAIN", MainLayout, name = "Azure Service Bus Explorer")
        #self.addForm(MSG_PAYLOAD_VIEW_FRM, MessageViewRecord)

def run_tui(conn_str, *args):
    #npyscreen.setTheme(npyscreen.Themes.ColorfulTheme)
    npyscreen.setTheme(npyscreen.Themes.TransparentThemeLightText)
    app = MsgExplorerApp(conn_str)
    app.run()

def tui_app(conn_str):
    npyscreen.wrapper_basic(functools.partial(run_tui, conn_str))

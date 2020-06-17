import npyscreen
import functools
import asyncio
import curses
import json

from asb_tour.topic_client import TopicClient
from asb_tour.sub_client import SubscriptionClient
from asb_tour.dlq_client import DlqClient

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
        if row is not None:
            self.parent.selected_message(row[0])

    def when_view(self, *args, **keywords):
        self.selected()

    def when_exit(self, *args, **keywords):
        curses.beep()
        self.parent.parentApp.setNextForm(None)
        self.editing = False
        self.parent.parentApp.switchFormNow()

class TopicsTreeWidget(npyscreen.MLTreeAction):
    def actionHighlighted(self, treenode, key_press):
        if key_press != curses.ascii.NL:
            return
        c = treenode.content
        if treenode.hasChildren() and (c != 'queue' or c != 'dlq'):
            # topic or ns selected
            return
        sub_name = treenode.getParent().content.split(' ')[0]
        topic_name = treenode.getParent().getParent().content
        self.parent.fetch_messages_request(topic_name, sub_name, is_dlq=c=='dlq')

class TopicsColumn(npyscreen.BoxTitle):
    _contained_widget = TopicsTreeWidget
    pass

class MessagesColumn(npyscreen.BoxTitle):
    _contained_widget = MessageList
    pass

class MessagePropsPane(npyscreen.BoxTitle):
    _contained_widget = npyscreen.MultiLine
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
        dw = int((w-32)*.6)
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
                                   name="Message Payload",
                                   relx=32,
                                   rely = mh+2,
                                   scroll_exit=True,
                                   exit_right=True,
                                   max_height = h-mh-4,
                                   width = dw,
                                   editable=True,
                                   center=False,
                                   autowrap=True)
        self.wMsgProps = self.add(MessagePropsPane,
                                  name="Message Properties",
                                  relx = 32 + dw,
                                  rely = mh+2,
                                  max_height= h - mh - 4,
                                  scroll_exit=True,
                                  exit_left=True)
        self._subclients = dict()
        self._dlqclients = dict()
        self.topic_name = None
        self.sub_name = None
        self.is_dlq = False
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
        for _,client in self._subclients.items():
            client.clear()
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
        if self.topic_name and self.sub_name:
            self.fetch_messages_request(self.topic_name, self.sub_name, self.is_dlq)

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
            self.fetch_messages(self.topic_name, self.sub_name, self.is_dlq)
            self.update_messages_request = False

    def fetch_topics(self):
        tp = self.parentApp.tp_client
        lst = tp.topics()
        treedata = npyscreen.NPSTreeData(content=tp.namespace, selectable=True,ignoreRoot=False)
        tpc = subc = 0
        for topic_name,sb_lst in lst:
            tp_selected = self.topic_name == topic_name
            t = treedata.newChild(content=topic_name, selectable=False, selected=tp_selected)
            tpc = tpc + 1
            for sb in sb_lst:
                sb_selected = self.sub_name == sb.name
                title = "%s (%d)" % (sb.name, sb.message_count)
                sub_node = t.newChild(content=title, selectable=True, selected=sb_selected)
                sub_node.newChild(content='queue', selectable=True)
                sub_node.newChild(content='dlq', selectable=True)
                subc = subc + 1
        self.wTopics.values = treedata
        self.wTopics.footer = "Topics (%s), Subs (%d)" % (tpc, subc)
        self.wTopics.display()

    def fetch_messages_request(self, topic_name, sub_name, is_dlq=False):
        if topic_name != self.topic_name or sub_name != self.sub_name:
            self.wMain.values = []
        self.topic_name = topic_name
        self.sub_name = sub_name
        self.is_dlq = is_dlq
        self.update_messages_request = True
        self.wMain.footer = "Peeking {} messages...".format('dlq' if is_dlq else '')
        self.wMain.edit()
        self.wMain.display()

    def get_subclient(self, topic_name, sub_name, is_dlq):
        key = "{0}-{1}".format(topic_name, sub_name)
        if is_dlq and key in self._dlqclients:
            return self._dlqclients[key]
        if not is_dlq and key in self._subclients:
            return self._subclients[key]

        if is_dlq:
            client = DlqClient(self.parentApp.conn_str, topic_name, sub_name)
            self._dlqclients[key] = client
        else:
            client = SubscriptionClient(self.parentApp.conn_str, topic_name, sub_name)
            self._subclients[key] = client
        return client

    def fetch_messages(self, topic_name, sub_name, is_dlq):
        client = self.get_subclient(topic_name, sub_name, is_dlq)
        lst = client.peek(50)
        self.wMain.values = [
            [
                x.message_id,
                x.sequence_number,
                x.label,
                x.size,
                x.enqueued_time_utc
            ] for x in lst]
        self.wMain.footer = "Messages Count: {}".format(client.message_count)
        self.wMain.editable = client.message_count > 0
        self.wMain.display()

    def update_list(self):
        self.wTopics.footer = 'Loading...'
        self.wMain.editable = False
        self.wMsgDetail.editable = False
        self.wMsgProps.editable = False
        self.update_request = True
        self.display()

    def selected_message(self, msgid):
        client = self.get_subclient(self.topic_name, self.sub_name, self.is_dlq)
        msg = client.find_message(msgid)
        payload = ''
        if isinstance(msg.body, str):
            payload = msg.body
        elif isinstance(msg.body, dict):
            payload = json.dumps(msg.body, indent=2, sort_keys=True, default=str)
        self.wMsgDetail.values = payload.split('\n')
        if not self.wMsgDetail.editable:
            self.wMsgDetail.editable = True
            self.wMsgProps.editable = True
        self.wMsgProps.values = [
            "{0:15.15}\t{1}".format(k, v)
            for k,v in msg.all_properties.items()
        ]
        self.wMsgDetail.display()
        self.wMsgProps.display()
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

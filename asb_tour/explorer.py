import npyscreen
import functools
import asyncio
import curses

from asb_tour.topic_client import TopicClient
from asb_tour.sub_client import SubscriptionClient

MSG_PAYLOAD_VIEW_FRM = 'MSG_PAYLOAD_VIEW_FRM'

class MessageList(npyscreen.GridColTitles):
    default_column_number = 3
    def __init__(self, *args, **keywords):
        super(MessageList, self).__init__(*args, **keywords)
        self.col_margin = 0
        self.row_height = 1
        #self.column_width_requested = 25
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
        msg = self.parent.selected_message(row[0])

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

class MainLayout(npyscreen.FormBaseNew):
    def create(self):
        h, w = terminal_dimensions()
        self.wTopics = self.add(TopicsColumn,
                 name='Topics & Subscriptions',
                 relx = 2,
                 rely = 2,
                 max_width = 30,
                 max_height = h - 4)
        self.wMain = self.add(MessagesColumn,
                 name='MESSAGES',
                 relx = 32,
                 rely = 2,
                 scroll_exit = True,
                 column_width = 20,
                 max_height = h - 4)
        self.subclient = SubscriptionClient(self.parentApp.conn_str)
        self.update_request = False
        self.update_messages_request = False
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
        self.subclient.clear()
        self.wMain.values = []
        self.wMain.footer = 'Messges Count: 0'
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
        self.wMain.values = [[x.sequence_number, str(x)[:50], x.enqueued_time_utc] for x in lst]
        self.wMain.footer = "Messages Count: %d" % self.subclient.message_count
        self.wMain.display()

    def update_list(self):
        self.wTopics.footer = 'Loading...'
        self.wTopics.display()
        self.update_request = True

    def selected_message(self, seqno):
        msg = self.subclient.find_message(seqno)
        # TODO: Show message details in bottom pane
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

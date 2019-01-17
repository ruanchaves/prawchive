import psycopg2
import praw
import select
import json
import re

class SmartStream(object):

    def __init__(self):
        self.channel = 'events'

    def fetch(self):
        conn = psycopg2.connect("dbname=reddit user=postgres")
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("LISTEN {0};".format(self.channel))
        while 1:
            if select.select([conn], [], [], 5) == ([], [], []):
                yield None #timeout
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    yield { 'payload' : notify.payload,
                            'pid' : notify.pid,
                            'channel' : notify.channel }

    def __iter__(self):
        for item in self.fetch():
            if item == None:
                continue
            payload = json.loads(item['payload'])
            if payload['action'] == 'INSERT':
                yield { 'id' : payload['data']['reddit_id'],
                       'class' : payload['data']['class'] }

class Bot(object):

    def __init__(self,username=None):
        self.username = username
        self.driver = Driver()
        self.reddit = ''
        self.blacklist = 'blacklist'
        self.driver.limit = 7000
        self.block_command = 'block'

    def auth(self):
        query = 'SELECT * FROM accounts WHERE username = \'{0}\';'
        query = query.format(self.username)
        account = self.driver.pull(query)[0]
        self.reddit = praw.Reddit(client_id=account[1],
                                  client_secret=account[2],
                                  password=account[3],
                                  user_agent=account[4],
                                  username=account[5])

    def block(self,idx,class_=None):
        query = 'INSERT INTO {0} (reddit_id, class) VALUES (%s, %s)'.format(self.blacklist)
        self.driver.push_var(query, (idx,class_) )

    def unblock(self,idx):
        query = "DELETE FROM {0} WHERE reddit_id = '{1}';".format(self.blacklist,idx)
        print(query)
        self.driver.push(query)

    def check(self,idx,class_=None):
        self.driver.check(self.blacklist)
        if class_ != None:
            query = 'SELECT * FROM {0} WHERE reddit_id = %s AND class = %s'.format(self.blacklist)
            result = self.driver.pull_var(query, (idx,class_) )
        else:
            query = 'SELECT * FROM {0} WHERE reddit_id = %s'.format(self.blacklist)
            result = self.driver.pull_var(query, (idx,) )
        if any(result):
            return True
        else:
            return False

    def is_class(self,obj,class_='None'):
        if class_ in str(type(obj)).lower():
            return True
        else:
            return False

    def get_type(self,obj):
        classes = ['comment', 'submission', 'redditor']
        for c in classes:
            if self.is_class(obj, c):
                return c
            else:
                return None

    def read(self,item):
        if item['class'] == 'submission':
            return self.reddit.submission(id=item['id'])
        elif item['class'] == 'comment':
            return self.reddit.comment(id=item['id'])

class Driver(object):
    def __init__(self):
        self.DATABASE_URL = ''
        self.conn = ''

        #self.local_connect()
        self.heroku_connect()

        self.cur = self.conn.cursor()
        self.limit = 1000
        self.trim = self.limit / 2
        self.init_file = 'initialize.sql'

    def initialize(self):
        self.conn.set_session(autocommit=True)
        self.cur.execute(open(self.init_file,'r').read())

    def heroku_connect(self):
        self.DATABASE_URL = os.environ['DATABASE_URL']
        self.conn = psycopg2.connect(DATABASE_URL, sslmode='require')

    def local_connect(self):
        self.conn = psycopg2.connect("dbname=reddit user=postgres")

    def pull(self,query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def pull_var(self,query,var):
        self.cur = self.conn.cursor()
        self.cur.execute(query,var)
        rows = self.cur.fetchall()
        return rows

    def push(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def push_var(self, query, var):
        self.cur.execute(query, var)
        self.conn.commit()

    def serialize(self,table=None):
        if not table:
            self.cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename NOT LIKE '%\_%';")
            result = [ y for x in self.cur.fetchall() for y in x ]
        else:
            result = [ table ]
        for t in result:
            query = "ALTER SEQUENCE {0}_id_seq RESTART WITH 1;".format(t)
            self.cur.execute(query)
            self.conn.commit()

    def check(self,table):
        query = 'select count(*) from {0}'.format(table)
        count = self.pull(query)
        count = count[0][0]
        if count >= self.limit:
            query = "DELETE FROM {0};"
            query = query.format(table,self.limit)
            self.push(query)
            try:
                self.serialize(table)
            except:
                pass

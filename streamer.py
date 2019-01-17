import praw
import os
import psycopg2

from utils import Driver

class Streamer(object):
    def __init__(self):
        self.reddit = ''
        self.subreddits_names = []
        self.subreddits_table = 'subreddits'
        self.subreddits = []
        self.results = []
        self.account = {}

    def auth(self):
        return praw.Reddit(client_id=self.account['CLIENT_ID'],
                           client_secret=self.account['CLIENT_SECRET'],
                           password=self.account['PASSWORD'],
                           user_agent=self.account['USER_AGENT'],
                           username=self.account['USERNAME'])

    def update(self):
        self.reddit = self.auth()
        driver = Driver()
        subs = driver.pull('select * from {0}'.format(self.subreddits_table))
        self.subreddits_names = [ x for t in subs for x in t ]
        self.subreddits_names = [ x for x in self.subreddits_names if isinstance(x,str) ]

    def translate(self):
        self.subreddits = [ self.reddit.subreddit(x) for x in self.subreddits_names ]

    def compile(self,**kwargs):
        self.results = []
        for subreddit in self.subreddits:
            self.results.extend(subreddit.new(**kwargs))
            self.results.extend(subreddit.comments(**kwargs))
        self.results.sort(key=lambda post: post.created_utc, reverse=True)
        return self.results

    def __iter__(self):
        stream = praw.models.util.stream_generator(lambda **kwargs: self.compile(**kwargs))
        for idx,post in enumerate(stream):
            yield post

    def __call__(self):
        self.update()
        self.translate()

class Manager(object):

    def __init__(self):
        self.account = ''
        self.driver = Driver()
        self.streamer = Streamer()
        self.stream_table = 'stream'
        self.accounts_table = 'accounts'
        self.limit = self.driver.limit

    def auth(self,idx=1):
        query = 'SELECT * FROM accounts WHERE id = {0};'
        count_query = 'SELECT count(*) FROM {0};'
        query = query.format(idx)
        count_query = count_query.format(self.accounts_table)
        count = self.driver.pull(count_query)[0][0]
        if(idx > count ):
            self.auth(1)
            return
        try:
            account = self.driver.pull(query)[0]
            self.account = {
                'id' : account[0],
                'CLIENT_ID' : account[1],
                'CLIENT_SECRET' : account[2],
                'PASSWORD' : account[3],
                'USER_AGENT' : account[4],
                'USERNAME' : account[5]
            }
        except:
            self.auth(idx+1)
            return

    def build(self):
        self.streamer.account = self.account
        self.streamer()

    def get_type(self,post):
        types = ['comment', 'submission']
        post_type = str(type(post)).lower()
        for t in types:
            if t in post_type:
                return t

    def run(self):
        self.driver.check(self.stream_table)
        select_query = 'select * from {0} where reddit_id = %s'
        insert_query = 'insert into {0} (reddit_id,class) values (%s,%s)'
        idx = 0
        try:
            for post in self.streamer:
                idx += 1
                post_type = self.get_type(post)
                copies = self.driver.pull_var(select_query.format(self.stream_table), (post.id,))
                copies = [ x for t in copies for x in t ]
                if not any(copies):
                    self.driver.push_var(insert_query.format(self.stream_table), (post.id,post_type) )
                if idx >= self.limit:
                    self.driver.check(self.stream_table)
                    idx = 1
        except:
            self.auth()
            self.build()
            self.run()


    def __call__(self):
        self.auth()
        self.build()
        self.run()

if __name__ == '__main__':
    driver = Driver()
    driver.initialize()
    man = Manager()
    man()

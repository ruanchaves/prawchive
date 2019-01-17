from utils import SmartStream, Bot
import select
import psycopg2
import json
import praw
import re
import savepagenow
import os

class ArchiveBot(Bot):

    def __init__(self,username):
        super().__init__(username)
        self.template = ""
        self.block_command = ''
        self.unblock_command = ''

    def archive(self,url):
        page, is_not_cache = savepagenow.capture_or_cache(url)
        return page

    def compile(self,md_urls):

        if not md_urls:
            return None
        if any([ self.regexp(v) for v in md_urls.values() ]):
            return None

        template = self.template
        body = ''

        for key in md_urls.keys():
            body += '* [{0}]({1})    \n'.format(key,self.archive(md_urls[key]) )
        return body

    def regexp(self,text):
        query = "SELECT expression FROM regexp;"
        result = [ y for x in self.driver.pull(query) for y in x ]
        for pattern in result:
            if pattern in text:
                return True
        else:
            return False

    def process(self,body):
        INLINE_LINK_RE = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
        md_urls = dict(INLINE_LINK_RE.findall(body))
        return md_urls

    def get_message(self,obj):
        body = self.get_body(obj)
        message = self.compile(self.process(body))
        if not message:
            return None
        message = self.template.format(message)
        return message

    def get_body(self,obj):
        if self.is_class(obj, 'comment'):
            body = obj.body
        elif self.is_class(obj,'submission'):
            body = "[{0}]({1}) {2}".format(obj.title,obj.url,obj.selftext)
        return body

    def __call__(self,item):

        myself = self.reddit.redditor(self.username)
        if not self.check(myself.id):
            self.block(myself.id,'redditor')

        obj = self.read(item)
        obj_id = obj.id
        body = self.get_body(obj)
        redditor = obj.author
        redditor_id = redditor.id


        if self.unblock_command in body:
            if self.check(redditor_id):
                self.unblock(redditor_id)
                return None
        elif self.block_command in body:
            if not self.check(redditor_id):
                self.block(redditor_id,'redditor')
                return None

        if self.check(obj_id):
            return None
        if self.check(redditor_id):
            return None

        try:
            obj.reply( self.get_message(obj) )
        except Exception as e:
            pass
        self.block(obj.id, self.get_type(obj))

if __name__ == '__main__':
    bot = ArchiveBot('')
    bot.auth()
    stream = SmartStream()
    for item in stream:
        bot(item)

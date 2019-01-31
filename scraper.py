#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Dependencies
import numpy as np
import pandas as pd
import threading
import _thread
from splinter import Browser
import requests
import lxml
from datetime import datetime
import pymongo
import json
from bs4 import BeautifulSoup
from tqdm import tqdm_notebook as tqdm
from tqdm import trange


# Import and Initialize Sentiment Analyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()


# In[3]:


get_ipython().system('which chromedriver')


# In[4]:


client = pymongo.MongoClient("mongodb://traffickAdmin:y2U3gQBgiV8Kq2u@traffickcluster-shard-00-00-xeuqd.mongodb.net:27017,traffickcluster-shard-00-01-xeuqd.mongodb.net:27017,traffickcluster-shard-00-02-xeuqd.mongodb.net:27017/test?ssl=true&replicaSet=TraffickCluster-shard-0&authSource=admin&retryWrites=true", connect=False)
db = client.Forums
diablo_collection = db.Diablo


# In[5]:


def scrape(start, stop):
    errors = []
    executable_path = {'executable_path': '/home/xanderroy/bin/chromedriver'}
    browser = Browser('chrome', **executable_path, headless=False)
    for n in tqdm(range(start, stop), desc='Pages'):
        browser.visit(f'https://us.battle.net/forums/en/d3/3354739/?page={n}')
        browser.reload()
        html = browser.html
        soup = BeautifulSoup(html, 'lxml-xml')
        try: 
            soup.find_all('div','main-frame-error')
        except TimeoutException as te:
            browser.reload()

        titles = soup.find_all('span', class_="ForumTopic-title")
        links = soup.find_all('span', class_="ForumTopic-timestamp")

        for link in tqdm(links, desc='Topics', leave=False):
            
            link = 'https://us.battle.net' + link.find('a')['href']
            browser.visit(link)
            html = browser.html

            soup = BeautifulSoup(html, 'lxml-xml')
            pages = soup.find_all('a','Pagination-button Pagination-button--ordinal')
            try:
                last_page = int(pages[-1].text)
            except IndexError:
                last_page = 1
            topic_collected = False
            for i in tqdm(range(1, last_page), leave=False, desc='Pages'):
                if topic_collected == False:
                    browser.visit('https://us.battle.net/forums/en/d3/topic/20769689106?page=' +
                                  str(i))
                    html = browser.html
                    soup = BeautifulSoup(html, 'lxml-xml')


                    topic = soup.find("section", "Topic")

                    topic_info = json.loads(topic['data-topic'])
                    topic_id = topic_info['id']
                    content = topic.find("div", "Topic-content")
                    posts = content.find_all('div', 'TopicPost ')


                    for post in posts:
                        qoutes = []
                        post_data = json.loads(post['data-topic-post'])
                        try: 
                            if diablo_collection.count_documents({'topicid': topic_id}) >= post_data['lastPosition']:
                                topic_collected = True
                        except KeyError:
                            topic_collected = True
                        post_author = post_data['author']['name']
                        post_id = post_data['id']
                        post_html = post.find('div', 'TopicPost-bodyContent')
                        post_text = post.find('div', 'TopicPost-bodyContent').get_text()
                        post_time = post.find('a', 'TopicPost-timestamp')['data-tooltip-content']
                        post_time = datetime.strptime(post_time, '%m/%d/%Y %I:%M %p')
                        post_upvotes = post_data['rank']['voteUp']
                        post_downvotes = post_data['rank']['voteDown']

                        try:
                            blue = post.find('div', 'TopicPost TopicPost--blizzard ').text
                            blue_post = 1

                        except AttributeError:
                                blue_post = 0





                        qoutes_html = post_html.find_all('blockquote')

                        for qoute in qoutes_html:
                            try:

                                qoute_author = qoute.find('a').text
                                qoute_date = qoute.find('span', 'bml-quote-date').text
                                qoute_text = qoute.text.replace(
                                    qoute_date,"").replace("Posted by", "").replace(qoute_author, "")

                                post_text = post_text.replace(
                                    qoute_date,"").replace("Posted by", "").replace(
                                    qoute_author, "").replace(qoute_text, "")

                                qoute_scores = analyzer.polarity_scores(qoute_text)
                                qoute_date =  datetime.strptime(qoute.find('span', 'bml-quote-date').text,
                                                                 '%m/%d/%Y %I:%M %p')






                                qoutes.append({'Date' : qoute_date,
                                              'Autor':  qoute_author,
                                              'Text': qoute_text,
                                              'neg': qoute_scores['neg'],
                                               'neu': qoute_scores['neu'],
                                               'pos': qoute_scores['pos'],
                                               'compound': qoute_scores['compound']})
                            except AttributeError as A:
                                qoutes.append("Error")
                        post_scores = analyzer.polarity_scores(post_text)




                        post_doct = {"topicid": topic_id,
                                    "postid" : post_id,
                                     'author' : post_author,
                                     'isblue' : blue_post,
                                     'time' : post_time,
                                     'text' : post_text,
                                     'upvotes' : post_upvotes,
                                     "Downvotes" : post_downvotes,
                                     'neg': post_scores['neg'],
                                     'neu': post_scores['neu'],
                                     'pos': post_scores['pos'],
                                     'compound': post_scores['compound'],
                                     'qoutes' : qoutes,

                                    }
                        try:
                            diablo_collection.update_one({'postid': post_id}, {"$set": post_doct}, upsert=True)
                        except AttributeError as E:
                            errors.append((E, topicid))

                                


# In[7]:


_thread.start_new_thread ( scrape, (1, 10))


# In[8]:


c


# In[9]:


_thread.start_new_thread ( scrape, (20, 100))


# In[ ]:


# _thread.start_new_thread ( scraper, (1, 1000))
#  _thread.start_new_thread ( scraper, (1001, 2000))
#  _thread.start_new_thread ( scraper, (2001, 3000))
# _thread.start_new_thread ( scraper, (3021, 4000))
# _thread.start_new_thread ( scraper, (4001, 5000))
# # _thread.start_new_thread ( scraper, (5001, 6000))
# _thread.start_new_thread ( scraper, (6001, 7000))
# _thread.start_new_thread ( scraper, (7001, 8000))
# # _thread.start_new_thread ( scraper, (8001, 9000))
# # _thread.start_new_thread ( scraper, (9001, 9900))

# last_page = 10049


# In[425]:


def scraper(start_number, end_number):
    executable_path = {'executable_path': '/home/xanderroy/bin/chromedriver'}
    browser = Browser('chrome', **executable_path, headless=False)
    identifiers = []
    post_dict = {}

    for x in range(start_number, end_number + 1 ):
        try:
            url = f'https://us.battle.net/forums/en/d3/3354739/?page={x}'
            browser.visit(url)

            html = browser.html
            soup = BeautifulSoup(html, 'lxml-xml')


            titles = soup.find_all('span', class_="ForumTopic-title")
            links = soup.find_all('span', class_="ForumTopic-timestamp")


            for title, link in zip(titles, links):
                link = 'https://us.battle.net' + link.find('a')['href']


                reply_text = []
                reply_sentiment = []
                browser.visit(link)
                html = browser.html
                soup = BeautifulSoup(html, 'lxml-xml')
                try:
                    soup.find('div', class_="error-issue").text
                    print("404 error")
                except AttributeError:
                    replies = soup.find_all('div', class_="TopicPost-bodyContent")
                    dtimes = soup.find_all('a', class_="TopicPost-timestamp")
                    for reply, dtime in zip(replies, dtimes):
                        author = re
                        posted_at = dtime['data-tooltip-content']

                        reply_text.append(reply.text)
                        reply_sentiment.append(analyzer.polarity_scores(reply.text))


                    db.d3forum2.update_one({'title':title.text.strip()}, {"$set":
                                                                          {'title':  title.text.strip(),
                                                                           'posted_at' : datetime.strptime(posted_at[-19:], '%m/%d/%Y %I:%M %p'),
                                                                            'text' : reply_text,
                                                                           'reply_sentiment': reply_sentiment}}, upsert=True)

        except TimeoutException as te:
            print(te)
            continue


# In[426]:


threading.enumerate() 


# In[ ]:


get_ipython().run_line_magic('pinfo', 'threading')


# In[ ]:





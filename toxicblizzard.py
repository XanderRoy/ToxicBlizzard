#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Dependencies
import numpy as np
import pandas as pd
from splinter import Browser
import requests
import pymongo
from bs4 import BeautifulSoup

# Import and Initialize Sentiment Analyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()


# In[2]:


get_ipython().system('which chromedriver')


# In[3]:


# Initialize PyMongo to work with MongoDBs
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)


# In[38]:


# Define database and collection
db = client.diablo_db
collection = db.d3forum


# In[67]:


executable_path = {'executable_path': '/home/xanderroy/bin/chromedriver'}
browser = Browser('chrome', **executable_path, headless=False)


# In[ ]:


identifiers = []
post_dict = {}

for x in range(18, 9900):
    print(f"page {x} of 9900")
    url = f'https://us.battle.net/forums/en/d3/3354739/?page={x}'
    browser.visit(url)

    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    
    titles = soup.find_all('span', class_="ForumTopic-title")
    links = soup.find_all('span', class_="ForumTopic-timestamp")


    for title, link in zip(titles, links):
        link = 'https://us.battle.net' + link.find('a')['href']

        if link[-18:-7] not in identifiers:
            reply_text = []
            browser.visit(link)
            html = browser.html
            soup = BeautifulSoup(html, 'html.parser')
            try:
                soup.find('div', class_="error-issue").text
                print("404 error")
            except AttributeError:
                    replies = soup.find_all('div', class_="TopicPost-bodyContent")
                    dtimes = soup.find_all('a', class_="TopicPost-timestamp")
                    for reply, dtime in zip(replies, dtimes):
                      
                        posted_at = dtime['data-tooltip-content']

                        reply_text.append(reply.text)

                        post_dict[str(link[-18:-7])] = {'title': title.text.strip(), 
                            'posted_at' : posted_at[-19:],
                            'text' : reply_text}
        
                        post = {
                        'title': title.text.strip(), 
                        'posted_at' : posted_at[-19:],
                        'text' : reply_text}
                        collection.insert_one(post)

                
                    print(link[-18:-7])
                    identifiers.append(link[-18:-7])
                    print('--' *  8)


# In[12]:


sentiments = {}

for  post in post_dict:
    print(post)
    reply_count=0
    for reply in post_dict[post]['text']:
        reply_count += 1
        results = analyzer.polarity_scores(reply)
        compound = results["compound"]
        pos = results["pos"]
        neu = results["neu"]
        neg = results["neg"] 


        # Add each value to the appropriate list
        sentiments[str(post_dict[post]['title'])+" reply "+ str(reply_count)] = {
                                 "Date":   post_dict[post]['posted_at'],
                               "Compound": compound,
                               "Positive": pos,
                               "Negative": neu,
                               "Neutral": neg
                                }
     


# In[ ]:


sentiments_pd = pd.DataFrame.from_dict(sentiments).T
sentiments_pd


# In[43]:





# In[ ]:





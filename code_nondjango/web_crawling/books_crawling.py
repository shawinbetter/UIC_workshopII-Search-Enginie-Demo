# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 14:23:51 2019

@author: QIU Yoawen
"""

import urllib
import urllib.request
import requests
from bs4 import BeautifulSoup
import re
import traceback

def read_pageHtml(url):#a function to read the html code
    file = urllib.request.urlopen(url)
    data = file.read()
    return data 

def read_title(url):#since the title is not avaliable on the html page, we need to load another html to get
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    title = soup.find('h1',itemprop='name').text
    return title

def storageToLocalFiles(storagePath, data):#store in the path 
    fhandle = open(storagePath,"wb")
    fhandle.write(data)
    fhandle.close()

    
def download(i):
    url_book = 'http://www.gutenberg.org/cache/epub/'+str(i)+'/pg'+str(i)+'.txt'
    url_title = 'http://www.gutenberg.org/ebooks/'+str(i)
    try:#if failed, tell me the infomation and jump
        data = read_pageHtml(url_book)
        title = read_title(url_title)
    except:
        traceback.print_exc()
        return 
    comp = re.compile('[^A-Z^a-z^0-9^]')#It will be a big mistake if we contain some space in our tile
    title = comp.sub('_', title)#replace it!
    storagePath = "C:/Users/94883/Desktop/Workshop/text/"+str(title)+".txt"
    try:
        storageToLocalFiles(storagePath, data)
    except:
        traceback.print_exc()

for i in range(5000,7000):#It should larger than 200MB
    download(i)
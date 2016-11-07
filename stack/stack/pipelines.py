# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
import logging
from stack.items   import *
from scrapy.conf import settings
from scrapy.exceptions import DropItem




class psqlDBPipeline(object):
  def __init__(self):
    self.connection = psycopg2.connect(host='localhost', database='scrape_ex', user='Gannon')
    self.cursor = self.connection.cursor()

  def process_item(self, item, spider):
    try:
      if type(item) is StackItem:
        # logging.warning(item.get('title'))
        self.cursor.execute("""INSERT INTO test (title, url) VALUES(%s, %s)""", (item.get('title'), item.get('url'), ))
      self.connection.commit()
      self.cursor.fetchall()

    except psycopg2.DatabaseError, e:
      logging.warning("this is going inside the except")
      print "Error: %s" % e
    return item

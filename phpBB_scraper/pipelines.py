# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
from datetime import datetime
from tqdm import tqdm


class PhpbbScraperPipeline(object):
    def process_item(self, item, spider):
        return item


class SQLitePipeline(object):
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.pbar = None
        self.item_count = 0
        self.spider = None

    def open_spider(self, spider):
        """Initialize database connection when spider opens"""
        self.spider = spider
        # Create database file in the same directory as posts.csv was
        self.connection = sqlite3.connect("posts.db")
        self.cursor = self.connection.cursor()

        # Create table if it doesn't exist
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id TEXT,
                post_id TEXT,
                poster_id TEXT,
                username TEXT,
                post_count TEXT,
                post_time TEXT,
                post_text TEXT,
                quote_text TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create indexes for better query performance
        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_topic_id ON posts(topic_id)
        """
        )
        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_post_id ON posts(post_id)
        """
        )
        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_poster_id ON posts(poster_id)
        """
        )

        self.connection.commit()

        # Initialize progress bar
        self.pbar = tqdm(desc="Scraping posts", unit=" posts", dynamic_ncols=True)

    def close_spider(self, spider):
        """Close database connection when spider closes"""
        if self.pbar is not None:
            self.pbar.close()
        if self.connection:
            self.connection.close()

    def process_item(self, item, spider):
        """Insert scraped item into database"""
        self.cursor.execute(
            """
            INSERT INTO posts (topic_id, post_id, poster_id, username, post_count, 
                             post_time, post_text, quote_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                item.get("TopicID"),
                item.get("PostID"),
                item.get("PosterID"),
                item.get("Username"),
                item.get("PostCount"),
                item.get("PostTime"),
                item.get("PostText"),
                item.get("QuoteText"),
            ),
        )

        self.connection.commit()

        # Update progress bar
        self.item_count += 1
        self.pbar.update(1)
        
        # Get queue stats from spider's crawler
        stats = self.spider.crawler.stats.get_stats()
        pending = stats.get('scheduler/enqueued', 0) - stats.get('scheduler/dequeued', 0)
        
        self.pbar.set_postfix({
            'total': self.item_count,
            'queue': pending
        })

        return item

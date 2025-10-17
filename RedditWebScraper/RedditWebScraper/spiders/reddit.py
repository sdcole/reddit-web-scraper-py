import random
import scrapy
import html
import os
import time
from datetime import datetime

# List of realistic User-Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 Chrome/141.0.0.0 Mobile Safari/537.36",
]


def clean_text(text):
    """Escape problematic characters for JSON or DB output"""
    if text:
        return html.unescape(text).strip()
    return ""


class RedditSpider(scrapy.Spider):
    name = "reddit"

    custom_settings = {
        # Crawl tuning
        "DOWNLOAD_DELAY": 5 + random.uniform(0, 15),
        "CONCURRENT_REQUESTS": 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3,
        "AUTOTHROTTLE_MAX_DELAY": 20,
        "COOKIES_ENABLED": False,

        # Pipeline
        "ITEM_PIPELINES": {
            "RedditWebScraper.pipelines.PostgresSQLPipeline": 300,
        },

        # PostgreSQL connection (loaded from environment)
        "POSTGRES_URI": os.getenv("POSTGRES_URI"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }

    def __init__(self, endpoints=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Accept comma-separated subreddit URLs via cmd arg
        if endpoints:
            self.start_urls = endpoints.split(",")
        else:
            self.start_urls = ["https://www.reddit.com/r/WallStreetBets.json"]

    def start_requests(self):
        """Start requests with randomized User-Agent"""
        for url in self.start_urls:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        """Parse subreddit JSON listing and enqueue post detail requests"""
        data = response.json()

        for post in data["data"]["children"]:
            post_data = post["data"]
            post_url = f"https://www.reddit.com{post_data['permalink']}.json"

            yield scrapy.Request(
                url=post_url,
                callback=self.parse_thread,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                meta={
                    "reddit_id": post_data.get("id"),
                    "thread_title": clean_text(post_data.get("title")),
                    "thread_url": f"https://www.reddit.com{post_data.get('permalink')}",
                    "author": post_data.get("author"),
                    "subreddit": post_data.get("subreddit"),
                    "posted_datetime": datetime.fromtimestamp(post_data["created_utc"]),
                    "post_body": clean_text(post_data.get("selftext")),
                    "score": post_data.get("score"),            # existing
                    "upvote_ratio": post_data.get("upvote_ratio")  # ✅ new
                },
            )

        # Handle pagination
        after = data["data"].get("after")
        if after:
            base_url = response.url.split("?")[0]
            next_page = f"{base_url}?after={after}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            self.logger.info(f"Following pagination to next page: {next_page}")
            yield scrapy.Request(next_page, headers=headers, callback=self.parse)

    def parse_thread(self, response):
        """Parse a single Reddit post thread and all comments"""
        if random.randint(1, 15) == 1:
            pause_time = random.randint(30, 45)
            self.logger.info(f"Pausing for {pause_time} seconds to simulate human behavior...")
            time.sleep(pause_time)

        meta = response.meta
        data = response.json()

        comments = []
        if len(data) > 1:
            for c in data[1]["data"]["children"]:
                if c["kind"] == "t1":
                    comments.append(self.parse_comment(c["data"], depth=0))

        yield {
            "reddit_id": meta.get("reddit_id"),
            "title": meta.get("thread_title"),
            "body": meta.get("post_body"),
            "url": meta.get("thread_url"),
            "author": meta.get("author"),
            "subreddit": meta.get("subreddit"),
            "posted_datetime": meta.get("posted_datetime"),
            "score": meta.get("score"),
            "upvote_ratio": meta.get("upvote_ratio"),  # ✅ new
            "comments": comments,
        }


    def parse_comment(self, comment_data, depth=0):
        """Recursively parse nested comment structure with accurate depth"""
        replies = []

        replies_data = comment_data.get("replies")
        if isinstance(replies_data, dict):  # Reddit sets replies="" when no replies exist
            children = replies_data.get("data", {}).get("children", [])
            for child in children:
                if child.get("kind") == "t1":  # comment type
                    replies.append(self.parse_comment(child["data"], depth=depth + 1))

        return {
            "reddit_id": comment_data.get("id"),
            "author": comment_data.get("author"),
            "score": comment_data.get("score"),
            "body": clean_text(comment_data.get("body")),
            "posted_datetime": datetime.fromtimestamp(comment_data["created_utc"])
            if comment_data.get("created_utc")
            else None,
            "depth": depth,
            "replies": replies,
        }

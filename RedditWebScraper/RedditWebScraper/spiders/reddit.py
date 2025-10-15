import random
import scrapy
import html
import os
from datetime import datetime
import time

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
        "DOWNLOAD_DELAY": 5 + random.uniform(0, 15),
        "CONCURRENT_REQUESTS": 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3,
        "AUTOTHROTTLE_MAX_DELAY": 20,
        "COOKIES_ENABLED": False,
        "ITEM_PIPELINES": {
            "RedditWebScraper.pipelines.PostgresSQLPipeline": 300,
        },
        "POSTGRES_URI": os.getenv("POSTGRES_URI"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }

    def __init__(self, endpoints=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Accept comma-separated URLs via cmd
        if endpoints:
            self.start_urls = endpoints.split(",")
        else:
            self.start_urls = ["https://www.reddit.com/r/WallStreetBets.json"]

    def start_requests(self):
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
                    "thread_title": clean_text(post_data["title"]),
                    "thread_url": f"https://www.reddit.com{post_data['permalink']}",
                    "reddit_id": post_data["id"],
                    "author": post_data.get("author"),
                    "subreddit": post_data.get("subreddit"),
                    "posted_datetime": datetime.fromtimestamp(post_data["created_utc"]),
                    "post_body": clean_text(post_data.get("selftext")),
                },
            )

        # Handle pagination dynamically
        after = data["data"].get("after")
        if after:
            base_url = response.url.split('?')[0]
            next_page = f"{base_url}?after={after}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            yield scrapy.Request(next_page, headers=headers, callback=self.parse)

    def parse_thread(self, response):
        if random.randint(1, 15) == 1:
            self.logger.info("Pausing for 45-60 seconds to simulate human traffic...")
            time.sleep(random.randint(30, 45))

        """Parse a single Reddit post thread and all comments"""
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
            "comments": comments,
        }

    def parse_comment(self, comment_data, depth=0):
        """Recursively parse nested comment structure with accurate depth"""
        replies = []

        replies_data = comment_data.get("replies")
        if isinstance(replies_data, dict):  # Reddit sets replies="" when no replies exist
            children = replies_data.get("data", {}).get("children", [])
            for child in children:
                if child.get("kind") == "t1":  # comment
                    replies.append(self.parse_comment(child["data"], depth=depth + 1))

        return {
            "reddit_id": comment_data.get("id"),
            "author": comment_data.get("author"),
            "score": comment_data.get("score"),
            "body": clean_text(comment_data.get("body")),
            "posted_datetime": datetime.fromtimestamp(comment_data.get("created_utc"))
            if comment_data.get("created_utc")
            else None,
            "depth": depth,
            "replies": replies,
        }

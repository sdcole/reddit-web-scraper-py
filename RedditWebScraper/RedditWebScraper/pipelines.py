import psycopg2
from datetime import datetime

    
class PostgresSQLPipeline:
    def __init__(self, postgres_uri, postgres_db, postgres_user, postgres_password):
        self.postgres_uri = postgres_uri
        self.postgres_db = postgres_db
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            postgres_uri=crawler.settings.get("POSTGRES_URI"),
            postgres_db=crawler.settings.get("POSTGRES_DB"),
            postgres_user=crawler.settings.get("POSTGRES_USER"),
            postgres_password=crawler.settings.get("POSTGRES_PASSWORD"),
        )

    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            host=self.postgres_uri,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def get_or_create_user(self, username):
        if not username:
            return None
        try:
            self.cur.execute(
                """
                INSERT INTO users (username)
                VALUES (%s)
                ON CONFLICT (username) DO NOTHING
                RETURNING id
                """,
                (username,),
            )
            res = self.cur.fetchone()
            if res:
                return res[0]
            # Already exists
            self.cur.execute("SELECT id FROM users WHERE username=%s", (username,))
            return self.cur.fetchone()[0]
        except Exception:
            self.conn.rollback()
            return None

    def get_or_create_subreddit(self, subreddit_name):
        if not subreddit_name:
            return None
        try:
            self.cur.execute(
                """
                INSERT INTO subreddits (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (subreddit_name,),
            )
            res = self.cur.fetchone()
            if res:
                return res[0]
            # Already exists
            self.cur.execute("SELECT id FROM subreddits WHERE name=%s", (subreddit_name,))
            return self.cur.fetchone()[0]
        except Exception:
            self.conn.rollback()
            return None

    def process_item(self, item, spider):
        try:
            subreddit_id = self.get_or_create_subreddit(item.get("subreddit"))
            author_id = self.get_or_create_user(item.get("author"))

            # Insert post
            self.cur.execute(
                """
                INSERT INTO posts (reddit_id, subreddit_id, author_id, title, selftext, url, created_utc, num_comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (reddit_id) DO UPDATE
                SET title = EXCLUDED.title,
                    selftext = EXCLUDED.selftext,
                    url = EXCLUDED.url
                RETURNING id
                """,
                (
                    item.get("reddit_id"),
                    subreddit_id,
                    author_id,
                    item.get("title"),
                    item.get("body"),
                    item.get("url"),
                    item.get("posted_datetime"),
                    len(item.get("comments", [])),
                ),
            )
            post_id = self.cur.fetchone()[0]

            # Recursive insert for comments
            def insert_comment(comment, parent_db_id=None, depth=0):
                comment_author_id = self.get_or_create_user(comment.get("author"))
                self.cur.execute(
                    """
                    INSERT INTO comments (reddit_id, post_id, parent_id, author_id, body, score, created_utc, depth)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (reddit_id) DO UPDATE
                    SET body = EXCLUDED.body,
                        score = EXCLUDED.score,
                        depth = EXCLUDED.depth
                    RETURNING id
                    """,
                    (
                        comment.get("reddit_id"),
                        post_id,
                        parent_db_id,
                        comment_author_id,
                        comment.get("body"),
                        comment.get("score"),
                        comment.get("posted_datetime"),
                        depth,
                    ),
                )
                comment_db_id = self.cur.fetchone()[0]

                # Recurse into replies
                for reply in comment.get("replies", []):
                    insert_comment(reply, parent_db_id=comment_db_id, depth=depth + 1)

            for c in item.get("comments", []):
                insert_comment(c, parent_db_id=None, depth=0)

            self.conn.commit()
        except Exception as e:
            print(f"Error processing item: {e}")
            self.conn.rollback()
        return item

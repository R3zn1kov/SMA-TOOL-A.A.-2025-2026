import requests
import pandas as pd
from bs4 import BeautifulSoup
import nltk
from typing import List, Dict, Union
from requests import Response
from parsel import Selector
import logging
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse, urlencode
import unicodedata
import json
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Download necessary NLTK data (run once)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


class RedditExtractor:
    """Reddit Textual Content Extractor"""

    def __init__(self, request_timeout: int = 30):
        """Initialize Reddit extractor with session and configuration"""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        })
        self.request_timeout = request_timeout

        # Enhanced rate limiting settings to prevent IP/bot blocking
        self.base_delay = 3  # Increased base delay between requests
        self.max_delay = 30  # Increased maximum delay for exponential backoff
        self.backoff_factor = 2.0  # Increased multiplier for exponential backoff
        self.retry_attempts = 5  # Increased number of retry attempts on failure
        self.requests_count = 0  # Track number of requests made
        self.session_start_time = time.time()  # Track session duration

    def safe_request(self, url: str, delay_multiplier: float = 1.0) -> requests.Response:
        """Make a safe request with rate limiting and retry logic"""
        current_delay = self.base_delay * delay_multiplier

        # Adaptive delay based on request count and session duration
        self.requests_count += 1
        session_duration = time.time() - self.session_start_time

        # Increase delay for sustained scraping sessions
        if self.requests_count > 50:
            adaptive_delay = min(2.0, self.requests_count / 100)
            current_delay += adaptive_delay

        # Take longer breaks for extended sessions
        if session_duration > 300:  # 5 minutes
            current_delay *= 1.5

        for attempt in range(self.retry_attempts):
            try:
                # Apply progressive delay between requests
                if attempt > 0:
                    current_delay = min(current_delay * self.backoff_factor, self.max_delay)
                    log.info(f"Retry attempt {attempt + 1}, waiting {current_delay:.1f} seconds...")

                time.sleep(current_delay)

                # Log every 25th request to monitor scraping rate
                if self.requests_count % 25 == 0:
                    log.info(f"Made {self.requests_count} requests in {session_duration:.1f}s, avg rate: {self.requests_count/session_duration:.2f} req/s")

                response = self.session.get(url, timeout=self.request_timeout)

                # Check for rate limiting response codes
                if response.status_code == 429:  # Too Many Requests
                    log.warning(f"Rate limited on attempt {attempt + 1}, increasing delay...")
                    current_delay *= 3  # More aggressive backoff
                    if attempt < self.retry_attempts - 1:
                        continue

                # Check for other blocking indicators
                if response.status_code in [403, 502, 503]:
                    log.warning(f"Potential blocking detected (status {response.status_code}), backing off...")
                    current_delay *= 2
                    if attempt < self.retry_attempts - 1:
                        continue

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                log.warning(f"Request timeout on attempt {attempt + 1}/{self.retry_attempts}")
                if attempt == self.retry_attempts - 1:
                    raise

            except requests.exceptions.RequestException as e:
                log.warning(f"Request failed on attempt {attempt + 1}/{self.retry_attempts}: {e}")
                if attempt == self.retry_attempts - 1:
                    raise

        # This should not be reached due to the raise statements above
        raise requests.exceptions.RequestException("All retry attempts failed")

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text by removing accents and handling special characters"""
        if not text:
            return ""

        normalized = unicodedata.normalize('NFKD', text)
        normalized = ''.join([c for c in normalized if not unicodedata.combining(c)])
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    @staticmethod
    def process_text_with_nltk(text: str) -> str:
        """Process text using NLTK for tokenization and lemmatization"""
        if not text:
            return ""

        try:
            tokens = word_tokenize(text.lower())
            lemmatizer = WordNetLemmatizer()
            lemmatized = [lemmatizer.lemmatize(word) for word in tokens]
            return ' '.join(lemmatized)
        except Exception as e:
            log.error(f"Error in NLTK processing: {e}")
            return text

    def parse_post_info(self, response: Response) -> Dict:
        """Parse post data from a subreddit post"""
        selector = Selector(response.text)
        info = {}
        label = selector.xpath("//faceplate-tracker[@source='post']/a/span/div/text()").get()
        comments = selector.xpath("//shreddit-post/@comment-count").get()
        upvotes = selector.xpath("//shreddit-post/@score").get()
        info["authorId"] = selector.xpath("//shreddit-post/@author-id").get()
        info["author"] = selector.xpath("//shreddit-post/@author").get()
        info["authorProfile"] = "https://www.reddit.com/user/" + info["author"] if info["author"] else None
        info["subreddit"] = selector.xpath("//shreddit-post/@subreddit-prefixed-name").get()
        info["postId"] = selector.xpath("//shreddit-post/@id").get()
        info["postLabel"] = self.normalize_text(label.strip() if label else None)
        info["publishingDate"] = selector.xpath("//shreddit-post/@created-timestamp").get()
        info["postTitle"] = self.normalize_text(selector.xpath("//shreddit-post/@post-title").get())

        info["postLink"] = selector.xpath("//shreddit-canonical-url-updater/@value").get()
        if not info["postLink"]:
            info["postLink"] = selector.xpath("//link[@rel='canonical']/@href").get()
        if not info["postLink"]:
            info["postLink"] = response.url

        info["commentCount"] = int(comments) if comments else None
        info["upvoteCount"] = int(upvotes) if upvotes else None
        info["attachmentType"] = selector.xpath("//shreddit-post/@post-type").get()
        info["attachmentLink"] = selector.xpath("//shreddit-post/@content-href").get()

        return info

    def parse_post_comments(self, response: Response) -> List[Dict]:
        """Parse post comments and flatten them for CSV storage"""
        comments_list = []

        def parse_comment(parent_selector, parent_id=None, depth=0) -> str:
            """Parse a comment object"""
            # Try multiple selectors for different Reddit layouts
            author = (parent_selector.xpath("./@data-author").get() or
                     parent_selector.xpath(".//a[contains(@class, 'author')]/@href").get())
            if author and author.startswith('/user/'):
                author = author.replace('/user/', '')

            link = parent_selector.xpath("./@data-permalink").get()
            comment_id = parent_selector.xpath("./@data-fullname").get()

            # Try multiple selectors for comment body
            comment_body = None
            body_selectors = [
                ".//div[@class='md']/p/text()",
                ".//div[contains(@class, 'usertext-body')]/div/p/text()",
                ".//div[contains(@class, 'usertext-body')]//text()",
                ".//div[@class='md']//text()",
                ".//p//text()",
                ".//div[contains(@class, 'Comment')]//text()",
                ".//div[contains(@class, 'RichTextJSON-root')]//text()"
            ]

            for selector in body_selectors:
                body_texts = parent_selector.xpath(selector).getall()
                if body_texts:
                    comment_body = ' '.join([text.strip() for text in body_texts if text.strip()])
                    if comment_body:
                        break

            # If no body found, try getting all text content from the comment
            if not comment_body:
                all_text = parent_selector.xpath(".//text()").getall()
                # Filter out common UI elements
                filtered_text = []
                skip_terms = ['reply', 'permalink', 'save', 'report', 'give award', 'share', 'level 1', 'level 2', 'level 3', 'points', 'point', 'hour ago', 'hours ago', 'day ago', 'days ago', 'minute ago', 'minutes ago']
                for text in all_text:
                    text = text.strip()
                    if len(text) > 10 and not any(skip in text.lower() for skip in skip_terms):
                        filtered_text.append(text)
                if filtered_text:
                    comment_body = ' '.join(filtered_text[:3])  # Take first few meaningful texts

            normalized_body = self.normalize_text(comment_body) if comment_body else None

            # Try multiple selectors for score/votes
            score = None
            score_selectors = [
                ".//span[contains(@class, 'likes')]/@title",
                ".//span[contains(@class, 'score')]/@title",
                ".//span[contains(@class, 'score')]/text()",
                "./@data-score",
                ".//div[contains(@class, 'score')]//text()"
            ]

            for selector in score_selectors:
                score_val = parent_selector.xpath(selector).get()
                if score_val:
                    try:
                        score = int(score_val)
                        break
                    except:
                        continue

            try:
                subreddit = response.url.split("/r/")[1].split("/")[0] if "/r/" in response.url else None
            except:
                subreddit = None

            # Try multiple selectors for timestamp
            created_time = None
            time_selectors = [
                ".//time/@datetime",
                ".//time/@title",
                "./@data-timestamp"
            ]

            for selector in time_selectors:
                time_val = parent_selector.xpath(selector).get()
                if time_val:
                    created_time = time_val
                    break

            # Only add comment if it has meaningful content
            if normalized_body and len(normalized_body.strip()) > 0:
                comment_data = {
                    "comment_id": comment_id or f"comment_{len(comments_list)}",
                    "parent_id": parent_id,
                    "parent_chain": parent_id if parent_id else None,
                    "author": author or "[unknown]",
                    "author_id": parent_selector.xpath("./@data-author-fullname").get(),
                    "subreddit": subreddit,
                    "link": "https://www.reddit.com" + link if link else None,
                    "created_time": created_time,
                    "body": normalized_body,
                    "body_processed": self.process_text_with_nltk(normalized_body) if normalized_body else None,
                    "score": score or 0,
                    "depth": depth,
                    "reply_count": 0  # Will be updated during processing
                }

                comments_list.append(comment_data)
                log.info(f"HTML: Found comment {len(comments_list)} at depth {depth}: {normalized_body[:50]}...")
                return comment_id or f"comment_{len(comments_list)}"

            return None

        def process_replies(what, parent_comment_id, depth=0):
            """Recursively process replies with improved nesting detection"""
            if depth > 15:  # Increased depth limit for deeper threads
                return

            # More comprehensive selectors for nested comments
            reply_selectors = [
                ".//div[@data-type='comment'][not(ancestor::div[@data-type='comment'])]",  # Direct children only
                ".//div[contains(@class, 'child')]/div[@data-type='comment']",  # Child containers
                ".//div[contains(@class, 'child')]//div[@data-type='comment']",  # All descendants in child containers
                ".//div[contains(@class, 'thing') and @data-type='comment']",
                ".//div[contains(@class, 'comment') and not(contains(@class, 'parent'))]",
                ".//div[contains(@class, 'Comment') and contains(@class, 'nested')]"
            ]

            replies_found = 0
            processed_ids = set()

            for selector in reply_selectors:
                reply_boxes = what.xpath(selector)

                for reply_box in reply_boxes:
                    # Get unique identifier to avoid processing same comment multiple times
                    reply_id = reply_box.xpath("./@data-fullname").get() or reply_box.xpath("./@id").get()
                    if reply_id and reply_id in processed_ids:
                        continue

                    if reply_id:
                        processed_ids.add(reply_id)

                    reply_comment_id = parse_comment(reply_box, parent_comment_id, depth + 1)
                    if reply_comment_id:
                        replies_found += 1
                        # Look for nested replies in this reply
                        nested_replies = process_replies(reply_box, reply_comment_id, depth + 1)
                        replies_found += nested_replies

            # Alternative approach: look for comment chains by following sibling elements
            if replies_found == 0 and depth < 5:
                # Try to find reply chains using different patterns
                chain_selectors = [
                    ".//following-sibling::div[@data-type='comment']",
                    ".//div[contains(@class, 'morechildren')]/following-sibling::div[@data-type='comment']",
                    ".//div[contains(@class, 'child')]//div"
                ]

                for chain_selector in chain_selectors:
                    chain_items = what.xpath(chain_selector)
                    for chain_item in chain_items[:10]:  # Limit to prevent excessive processing
                        chain_id = chain_item.xpath("./@data-fullname").get()
                        if chain_id and chain_id not in processed_ids:
                            processed_ids.add(chain_id)
                            chain_comment_id = parse_comment(chain_item, parent_comment_id, depth + 1)
                            if chain_comment_id:
                                replies_found += 1

            return replies_found

        selector = Selector(response.text)
        log.info("Starting comment extraction...")

        # Try multiple selectors for the main comment container
        main_selectors = [
            "//div[@class='sitetable nestedlisting']/div[@data-type='comment']",
            "//div[contains(@class, 'sitetable')]//div[@data-type='comment']",
            "//div[@data-type='comment']",
            "//div[contains(@class, 'comment')]",
            "//div[contains(@class, 'Comment')]",
            "//div[contains(@class, 'thing')][@data-type='comment']"
        ]

        found_comments = False
        total_replies = 0

        for main_selector in main_selectors:
            items = selector.xpath(main_selector)
            if items:
                log.info(f"Found {len(items)} top-level comments using selector: {main_selector}")
                found_comments = True

                for item in items:
                    comment_id = parse_comment(item, depth=0)
                    if comment_id:
                        # Process replies and count them
                        reply_count = process_replies(item, comment_id, 0)
                        total_replies += reply_count

                log.info(f"Processed {total_replies} nested replies for top-level comments")
                break

        if not found_comments:
            log.warning("No comments found with any selector, trying fallback approach...")
            # Fallback: try to find any element with comment-like content
            all_divs = selector.xpath("//div")
            for div in all_divs:
                text_content = div.xpath(".//text()").getall()
                if text_content:
                    combined_text = ' '.join([t.strip() for t in text_content if t.strip()])
                    if len(combined_text) > 50 and 'reply' in combined_text.lower():
                        parse_comment(div, depth=0)

        log.info(f"Total comments extracted: {len(comments_list)}")
        return comments_list

    def parse_reddit_json_comments(self, json_data) -> List[Dict]:
        """Parse comments from Reddit JSON API response"""
        comments_list = []

        def extract_comment_from_json(comment_data, parent_id=None, depth=0):
            """Extract comment data from JSON structure with improved nesting"""
            try:
                # Handle 'more' comments object
                if comment_data.get('kind') == 'more':
                    log.info(f"Found 'more comments' object at depth {depth}")
                    return 0

                if not comment_data or comment_data.get('kind') != 't1':
                    return 0

                data = comment_data.get('data', {})
                if not data:
                    return 0

                author = data.get('author', '[unknown]')
                if author in ['[deleted]', '[removed]']:
                    return 0  # Skip deleted comments

                body = data.get('body', '')
                if not body or body in ['[deleted]', '[removed]']:
                    return 0

                normalized_body = self.normalize_text(body)
                if not normalized_body or len(normalized_body.strip()) < 3:
                    return 0

                # Create full parent chain for better tracking
                parent_chain = []
                if parent_id:
                    parent_chain.append(parent_id)

                comment_info = {
                    "comment_id": data.get('id'),
                    "parent_id": parent_id,
                    "parent_chain": " > ".join(parent_chain) if parent_chain else None,
                    "author": author,
                    "author_id": data.get('author_fullname'),
                    "subreddit": data.get('subreddit'),
                    "link": f"https://www.reddit.com{data.get('permalink', '')}" if data.get('permalink') else None,
                    "created_time": data.get('created_utc'),
                    "body": normalized_body,
                    "body_processed": self.process_text_with_nltk(normalized_body) if normalized_body else None,
                    "score": data.get('score', 0),
                    "depth": depth,
                    "reply_count": 0  # Will be updated
                }

                comments_list.append(comment_info)
                current_comment_id = comment_info["comment_id"]
                replies_processed = 0

                log.info(f"JSON: Found comment {len(comments_list)} at depth {depth}: {normalized_body[:50]}...")

                # Process replies recursively
                replies = data.get('replies')
                if replies:
                    if isinstance(replies, dict):
                        reply_data = replies.get('data', {})
                        children = reply_data.get('children', [])
                    elif isinstance(replies, str) and replies == "":
                        # Empty replies string means no replies
                        children = []
                    else:
                        children = []

                    for child in children:
                        child_replies = extract_comment_from_json(child, current_comment_id, depth + 1)
                        replies_processed += child_replies + 1 if child_replies >= 0 else 0

                    # Update the reply count for the current comment
                    if replies_processed > 0:
                        comment_info["reply_count"] = replies_processed
                        log.info(f"Comment {current_comment_id} has {replies_processed} replies")

                return replies_processed

            except Exception as e:
                log.error(f"Error parsing JSON comment at depth {depth}: {e}")
                return 0

        try:
            # Reddit JSON structure: [post_data, comments_data]
            if isinstance(json_data, list) and len(json_data) > 1:
                comments_section = json_data[1]
                if isinstance(comments_section, dict):
                    comment_data = comments_section.get('data', {})
                    children = comment_data.get('children', [])

                    for child in children:
                        extract_comment_from_json(child, depth=0)

        except Exception as e:
            log.error(f"Error parsing JSON data: {e}")

        return comments_list

    def get_old_reddit_url(self, url: str) -> str:
        """Convert any Reddit URL to old.reddit.com format safely"""
        try:
            parsed = urlparse(url)
            if not parsed.netloc.endswith('reddit.com'):
                return url
            path = parsed.path
            old_reddit_url = f"https://old.reddit.com{path}"
            if parsed.query:
                old_reddit_url += f"?{parsed.query}"
            return old_reddit_url
        except Exception as e:
            log.error(f"Error converting to old.reddit URL: {e}")
            return url

    def get_subreddit_posts(self, subreddit: str, time_range: str = "week", sort: str = "hot", limit: int = 500) -> List[Dict]:
        """Get list of posts from a subreddit"""
        try:
            # Clean subreddit name
            subreddit = subreddit.replace('r/', '').replace('/r/', '').strip('/')

            # Build URL for subreddit
            base_url = f"https://old.reddit.com/r/{subreddit}"

            # Add sorting and time range
            if sort in ["top", "controversial"]:
                params = {"sort": sort, "t": time_range}
            else:
                params = {"sort": sort}

            if limit > 25:
                params["limit"] = min(limit, 500)  # Increased limit

            url = f"{base_url}/.json?" + urlencode(params)

            log.info(f"Fetching subreddit posts from: {url}")
            response = self.safe_request(url)

            data = response.json()
            posts = []

            if 'data' in data and 'children' in data['data']:
                for post_data in data['data']['children']:
                    if post_data.get('kind') == 't3':  # Post type
                        post_info = post_data.get('data', {})

                        # Filter by time range if needed
                        post_time = datetime.fromtimestamp(post_info.get('created_utc', 0))

                        post_url = f"https://www.reddit.com{post_info.get('permalink', '')}"

                        posts.append({
                            'id': post_info.get('id'),
                            'title': post_info.get('title', ''),
                            'author': post_info.get('author', ''),
                            'score': post_info.get('score', 0),
                            'num_comments': post_info.get('num_comments', 0),
                            'created_utc': post_info.get('created_utc'),
                            'created_time': post_time,
                            'url': post_url,
                            'subreddit': post_info.get('subreddit', subreddit),
                            'selftext': post_info.get('selftext', ''),
                            'domain': post_info.get('domain', ''),
                            'upvote_ratio': post_info.get('upvote_ratio', 0)
                        })

            log.info(f"Found {len(posts)} posts in r/{subreddit}")
            return posts

        except Exception as e:
            log.error(f"Error fetching subreddit posts: {e}")
            return []

    def filter_posts_by_time_range(self, posts: List[Dict], time_range_days: int) -> List[Dict]:
        """Filter posts by time range in days"""
        if not time_range_days:
            return posts

        cutoff_time = datetime.now() - timedelta(days=time_range_days)
        filtered_posts = []

        for post in posts:
            if 'created_time' in post:
                if post['created_time'] >= cutoff_time:
                    filtered_posts.append(post)

        return filtered_posts

    def extract_subreddit_comments(self, subreddit: str, time_range_days: int = 7, sort: str = "hot",
                                max_posts: int = 500, max_comments_per_post: int = 5000,
                                progress_callback=None) -> Dict:
        """Extract comments from multiple posts in a subreddit"""
        try:
            log.info(f"Starting subreddit textual content extraction for r/{subreddit}")

            # Get posts from subreddit
            reddit_time_range = "week" if time_range_days <= 7 else ("month" if time_range_days <= 30 else "year")
            posts = self.get_subreddit_posts(subreddit, reddit_time_range, sort, max_posts)

            if not posts:
                return {"posts": [], "comments": [], "summary": {}}

            # Filter by exact time range if needed
            if time_range_days:
                posts = self.filter_posts_by_time_range(posts, time_range_days)

            # Limit number of posts
            posts = posts[:max_posts]

            log.info(f"Processing {len(posts)} posts from r/{subreddit}")

            all_comments = []
            processed_posts = []
            total_comments = 0

            for i, post in enumerate(posts):
                try:
                    # Update progress
                    if progress_callback:
                        progress = 0.2 + (0.7 * (i + 1) / len(posts))
                        progress_callback(progress, f"Processing post {i+1}/{len(posts)}: {post['title'][:40]}...")

                    log.info(f"Processing post {i+1}/{len(posts)}: {post['title'][:50]}...")

                    # Extract comments from this post
                    post_data = self.extract_reddit_post(post['url'], "top")

                    if post_data and post_data.get("comments"):
                        # Limit comments per post
                        post_comments = post_data["comments"][:max_comments_per_post]

                        # Add post metadata to each comment
                        for comment in post_comments:
                            comment['post_id'] = post['id']
                            comment['post_title'] = post['title']
                            comment['post_score'] = post['score']
                            comment['post_author'] = post['author']
                            comment['post_created_time'] = post['created_time']

                        all_comments.extend(post_comments)
                        total_comments += len(post_comments)

                        processed_posts.append({
                            **post,
                            'comments_extracted': len(post_comments),
                            'extraction_success': True
                        })

                        log.info(f"Extracted {len(post_comments)} comments from post {i+1}")
                    else:
                        processed_posts.append({
                            **post,
                            'comments_extracted': 0,
                            'extraction_success': False
                        })
                        log.warning(f"No comments found for post {i+1}")

                    # Enhanced rate limiting between posts to prevent IP/bot blocking
                    if i < len(posts) - 1:
                        # Progressive delay based on number of posts processed
                        base_delay = 4
                        progressive_delay = min(base_delay + (i * 0.5), 15)  # Cap at 15 seconds
                        time.sleep(progressive_delay)

                        # Additional delay for every 10 posts to be extra safe
                        if (i + 1) % 10 == 0:
                            log.info(f"Processed {i + 1} posts, taking extended break...")
                            time.sleep(20)

                except Exception as e:
                    log.error(f"Error processing post {i+1}: {e}")
                    processed_posts.append({
                        **post,
                        'comments_extracted': 0,
                        'extraction_success': False,
                        'error': str(e)
                    })
                    continue

            summary = {
                'subreddit': subreddit,
                'total_posts_found': len(posts),
                'total_posts_processed': len(processed_posts),
                'total_comments': total_comments,
                'time_range_days': time_range_days,
                'sort_method': sort,
                'processing_time': datetime.now().isoformat()
            }

            log.info(f"Subreddit textual content extraction complete. Total comments: {total_comments}")

            return {
                "posts": processed_posts,
                "comments": all_comments,
                "summary": summary
            }

        except Exception as e:
            log.error(f"Error in subreddit textual content extraction: {e}")
            return {"posts": [], "comments": [], "summary": {}, "error": str(e)}

    def extract_reddit_post(self, url: str, sort: Union[str] = "new") -> Dict:
        """Extract subreddit post and comment data"""
        try:
            response = self.safe_request(url)

            post_data = {}
            post_data["info"] = self.parse_post_info(response)

            post_id_match = re.search(r'/comments/([a-z0-9]+)/', url)
            if post_id_match:
                post_id = post_id_match.group(1)

            if post_data["info"]["postLink"]:
                old_reddit_url = self.get_old_reddit_url(post_data["info"]["postLink"])
            else:
                old_reddit_url = self.get_old_reddit_url(url)

            # Remove limit parameter to get all comments
            if '?' in old_reddit_url:
                bulk_comments_page_url = f"{old_reddit_url}&sort={sort}&limit=5000"
            else:
                bulk_comments_page_url = f"{old_reddit_url}?sort={sort}&limit=5000"

            response = self.safe_request(bulk_comments_page_url, delay_multiplier=1.5)

            post_data["comments"] = self.parse_post_comments(response)

            # If we didn't get many comments, try the JSON API as backup
            if len(post_data["comments"]) < 10:
                log.info("Few comments found with HTML extraction, trying JSON API...")
                try:
                    json_url = old_reddit_url.replace('old.reddit.com', 'www.reddit.com') + '.json'
                    if '?' in json_url:
                        json_url += "&limit=5000"
                    else:
                        json_url += "?limit=5000"

                    json_response = self.safe_request(json_url, delay_multiplier=2.0)
                    json_data = json_response.json()

                    json_comments = self.parse_reddit_json_comments(json_data)
                    if len(json_comments) > len(post_data["comments"]):
                        log.info(f"JSON API found more comments: {len(json_comments)} vs {len(post_data['comments'])}")
                        post_data["comments"] = json_comments
                except Exception as e:
                    log.warning(f"JSON API fallback failed: {e}")

            # Add debug info
            comment_count = len(post_data["comments"])
            log.info(f"Extracted {comment_count} comments from Reddit post")

            return post_data

        except requests.exceptions.Timeout:
            log.error("Request timed out. The server took too long to respond.")
            return {"info": {}, "comments": []}
        except requests.exceptions.RequestException as e:
            log.error(f"Request error: {e}")
            return {"info": {}, "comments": []}
        except Exception as e:
            log.error(f"Error during extraction: {e}")
            return {"info": {}, "comments": []}

    @staticmethod
    def process_comments_with_pandas(comments: List[Dict]) -> pd.DataFrame:
        """Process comments using pandas to handle duplicates and clean data"""
        if not comments:
            return pd.DataFrame()

        try:
            df = pd.DataFrame(comments)
            initial_count = len(df)
            df.drop_duplicates(inplace=True)
            duplicate_count = initial_count - len(df)

            content_dup_count = df.duplicated(subset=['body'], keep='first').sum()
            df.drop_duplicates(subset=['body'], keep='first', inplace=True)

            df.fillna({
                'body': '',
                'body_processed': '',
                'score': 0,
                'author': '[deleted]'
            }, inplace=True)

            if 'created_time' in df.columns:
                df['created_time'] = pd.to_datetime(df['created_time'], errors='coerce')

            return df

        except Exception as e:
            log.error(f"Error processing comments: {e}")
            return pd.DataFrame(comments)
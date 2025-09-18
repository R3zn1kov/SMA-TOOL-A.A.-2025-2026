import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
import nltk
from typing import List, Dict, Union
from requests import Response
from parsel import Selector
import logging
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse, urlencode
import unicodedata
import io
import base64
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

# Create a session object for persistent connections
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
})

# Set timeout for all requests
REQUEST_TIMEOUT = 30  # 30 seconds timeout


def normalize_text(text: str) -> str:
    """Normalize text by removing accents and handling special characters"""
    if not text:
        return ""

    normalized = unicodedata.normalize('NFKD', text)
    normalized = ''.join([c for c in normalized if not unicodedata.combining(c)])
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized


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


def parse_post_info(response: Response) -> Dict:
    """parse post data from a subreddit post"""
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
    info["postLabel"] = normalize_text(label.strip() if label else None)
    info["publishingDate"] = selector.xpath("//shreddit-post/@created-timestamp").get()
    info["postTitle"] = normalize_text(selector.xpath("//shreddit-post/@post-title").get())

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


def parse_post_comments(response: Response) -> List[Dict]:
    """parse post comments and flatten them for CSV storage"""
    comments_list = []

    def parse_comment(parent_selector, parent_id=None, depth=0) -> str:
        """parse a comment object"""
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

        normalized_body = normalize_text(comment_body) if comment_body else None

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
                "body_processed": process_text_with_nltk(normalized_body) if normalized_body else None,
                "score": score or 0,
                "depth": depth,
                "reply_count": 0  # Will be updated during processing
            }

            comments_list.append(comment_data)
            log.info(f"HTML: Found comment {len(comments_list)} at depth {depth}: {normalized_body[:50]}...")
            return comment_id or f"comment_{len(comments_list)}"

        return None

    def process_replies(what, parent_comment_id, depth=0):
        """recursively process replies with improved nesting detection"""
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


def parse_reddit_json_comments(json_data) -> List[Dict]:
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

            normalized_body = normalize_text(body)
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
                "body_processed": process_text_with_nltk(normalized_body) if normalized_body else None,
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


def get_old_reddit_url(url: str) -> str:
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


def get_subreddit_posts(subreddit: str, time_range: str = "week", sort: str = "hot", limit: int = 25) -> List[Dict]:
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
            params["limit"] = min(limit, 100)  # Reddit's limit

        url = f"{base_url}/.json?" + urlencode(params)

        log.info(f"Fetching subreddit posts from: {url}")
        time.sleep(2)  # Rate limiting

        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

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


def filter_posts_by_time_range(posts: List[Dict], time_range_days: int) -> List[Dict]:
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


def scrape_subreddit_comments(subreddit: str, time_range_days: int = 7, sort: str = "hot",
                            max_posts: int = 10, max_comments_per_post: int = 50,
                            progress_callback=None) -> Dict:
    """Extract comments from multiple posts in a subreddit"""
    try:
        log.info(f"Starting subreddit textual content extraction for r/{subreddit}")

        # Get posts from subreddit
        reddit_time_range = "week" if time_range_days <= 7 else ("month" if time_range_days <= 30 else "year")
        posts = get_subreddit_posts(subreddit, reddit_time_range, sort, max_posts * 2)

        if not posts:
            return {"posts": [], "comments": [], "summary": {}}

        # Filter by exact time range if needed
        if time_range_days:
            posts = filter_posts_by_time_range(posts, time_range_days)

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
                post_data = scrape_reddit_post(post['url'], "top")

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

                # Rate limiting between posts (increased for better reliability)
                if i < len(posts) - 1:
                    time.sleep(4)

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


def scrape_reddit_post(url: str, sort: Union["old", "new", "top"] = "new") -> Dict:
    """extract subreddit post and comment data"""
    try:
        # Add delay to avoid being blocked
        import time
        time.sleep(2)

        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        post_data = {}
        post_data["info"] = parse_post_info(response)

        post_id_match = re.search(r'/comments/([a-z0-9]+)/', url)
        if post_id_match:
            post_id = post_id_match.group(1)

        if post_data["info"]["postLink"]:
            old_reddit_url = get_old_reddit_url(post_data["info"]["postLink"])
        else:
            old_reddit_url = get_old_reddit_url(url)

        # Remove limit parameter to get all comments
        if '?' in old_reddit_url:
            bulk_comments_page_url = f"{old_reddit_url}&sort={sort}&limit=500"
        else:
            bulk_comments_page_url = f"{old_reddit_url}?sort={sort}&limit=500"

        # Add another delay before fetching comments
        time.sleep(2)
        response = session.get(bulk_comments_page_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        post_data["comments"] = parse_post_comments(response)

        # If we didn't get many comments, try the JSON API as backup
        if len(post_data["comments"]) < 10:
            log.info("Few comments found with HTML extraction, trying JSON API...")
            try:
                json_url = old_reddit_url.replace('old.reddit.com', 'www.reddit.com') + '.json'
                if '?' in json_url:
                    json_url += "&limit=500"
                else:
                    json_url += "?limit=500"

                time.sleep(2)
                json_response = session.get(json_url, timeout=REQUEST_TIMEOUT)
                json_response.raise_for_status()
                json_data = json_response.json()

                json_comments = parse_reddit_json_comments(json_data)
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
        st.error("Request timed out. The server took too long to respond.")
        return {"info": {}, "comments": []}
    except requests.exceptions.RequestException as e:
        st.error(f"Request error: {e}")
        return {"info": {}, "comments": []}
    except Exception as e:
        st.error(f"Error during extraction: {e}")
        return {"info": {}, "comments": []}


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
        st.error(f"Error processing comments: {e}")
        return pd.DataFrame(comments)


def encode_special_characters(text):
    """Encode special characters in a text string"""
    encoded_text = ''
    special_characters = {'&': '%26', '=': '%3D', '+': '%2B', ' ': '%20'}
    for char in text.lower():
        encoded_text += special_characters.get(char, char)
    return encoded_text


def scrape_google_news_with_time_ranges(query: str, country: str = "US", max_articles: int = 100) -> pd.DataFrame:
    """Extract Google News with multiple time ranges to get more articles"""
    all_articles = []

    # Time range parameters to get more articles
    time_ranges = [
        "",  # All time
        "&when:1d",  # Past day
        "&when:7d",  # Past week
        "&when:1m",  # Past month
        "&when:1y"  # Past year
    ]

    query_encoded = encode_special_characters(query)

    # Extended country and language parameters
    country_params = {
        "US": {"hl": "en-US", "gl": "US", "ceid": "US%3Aen"},
        "IT": {"hl": "it-IT", "gl": "IT", "ceid": "IT%3Ait"},
        "UK": {"hl": "en-GB", "gl": "GB", "ceid": "GB%3Aen"},
        "DE": {"hl": "de-DE", "gl": "DE", "ceid": "DE%3Ade"},
        "FR": {"hl": "fr-FR", "gl": "FR", "ceid": "FR%3Afr"},
        "ES": {"hl": "es-ES", "gl": "ES", "ceid": "ES%3Aes"},
        "CA": {"hl": "en-CA", "gl": "CA", "ceid": "CA%3Aen"},
        "AU": {"hl": "en-AU", "gl": "AU", "ceid": "AU%3Aen"},
        "JP": {"hl": "ja-JP", "gl": "JP", "ceid": "JP%3Aja"},
        "BR": {"hl": "pt-BR", "gl": "BR", "ceid": "BR%3Apt"},
        "IN": {"hl": "en-IN", "gl": "IN", "ceid": "IN%3Aen"},
        "RU": {"hl": "ru-RU", "gl": "RU", "ceid": "RU%3Aru"},
        "CN": {"hl": "zh-CN", "gl": "CN", "ceid": "CN%3Azh"}
    }

    params = country_params.get(country, country_params["US"])
    articles_collected = 0
    target_articles = min(max_articles, 500)  # Cap at 500

    for time_range in time_ranges:
        if articles_collected >= target_articles:
            break

        try:
            # Multiple pagination attempts for each time range
            for start_param in range(0, 100, 10):  # Try pagination
                if articles_collected >= target_articles:
                    break

                # Add delay to avoid being blocked
                time.sleep(3)  # Increased delay between requests

                # Build URL with pagination
                base_url = f"https://news.google.com/search?q={query_encoded}{time_range}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"
                if start_param > 0:
                    url = f"{base_url}&start={start_param}"
                else:
                    url = base_url

                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                articles = soup.find_all('article')
                if not articles:  # No more articles for this time range
                    break

                links = [article.find('a')['href'] for article in articles if article.find('a')]
                links = [link.replace("./articles/", "https://news.google.com/articles/") for link in links]

                news_text = [article.get_text(separator='\n') for article in articles]
                news_text_split = [text.split('\n') for text in news_text]

                # Ensure we have enough links for all articles
                while len(links) < len(news_text_split):
                    links.append('Missing')

                batch_articles = []
                for i, text in enumerate(news_text_split):
                    if articles_collected >= target_articles:
                        break

                    article_data = {
                        'Title': text[2] if len(text) > 2 else 'Missing',
                        'Source': text[0] if len(text) > 0 else 'Missing',
                        'Time': text[3] if len(text) > 3 else 'Missing',
                        'Author': text[4].split('By ')[-1] if len(text) > 4 else 'Missing',
                        'Link': links[i] if i < len(links) else 'Missing',
                        'Time_Range': time_range.replace('&when:', '') if time_range else 'all_time',
                        'Page': start_param // 10 + 1
                    }
                    batch_articles.append(article_data)
                    articles_collected += 1

                all_articles.extend(batch_articles)

                # If we got fewer than 10 articles, this time range is exhausted
                if len(batch_articles) < 10:
                    break

        except requests.exceptions.Timeout:
            st.warning(f"Request timed out for time range: {time_range}")
            continue
        except Exception as e:
            st.warning(f"Error extracting time range {time_range}: {e}")
            continue

    if not all_articles:
        return pd.DataFrame()

    # Create DataFrame and remove duplicates based on title and link
    df = pd.DataFrame(all_articles)
    df = df.drop_duplicates(subset=['Title', 'Link'], keep='first')

    return df


def scrape_google_news_with_pagination(query: str, country: str = "US", max_articles: int = 100) -> pd.DataFrame:
    """Attempt to extract multiple pages of Google News results"""
    all_articles = []

    query_encoded = encode_special_characters(query)

    # Extended country and language parameters (same as above)
    country_params = {
        "US": {"hl": "en-US", "gl": "US", "ceid": "US%3Aen"},
        "IT": {"hl": "it-IT", "gl": "IT", "ceid": "IT%3Ait"},
        "UK": {"hl": "en-GB", "gl": "GB", "ceid": "GB%3Aen"},
        "DE": {"hl": "de-DE", "gl": "DE", "ceid": "DE%3Ade"},
        "FR": {"hl": "fr-FR", "gl": "FR", "ceid": "FR%3Afr"},
        "ES": {"hl": "es-ES", "gl": "ES", "ceid": "ES%3Aes"},
        "CA": {"hl": "en-CA", "gl": "CA", "ceid": "CA%3Aen"},
        "AU": {"hl": "en-AU", "gl": "AU", "ceid": "AU%3Aen"},
        "JP": {"hl": "ja-JP", "gl": "JP", "ceid": "JP%3Aja"},
        "BR": {"hl": "pt-BR", "gl": "BR", "ceid": "BR%3Apt"},
        "IN": {"hl": "en-IN", "gl": "IN", "ceid": "IN%3Aen"},
        "RU": {"hl": "ru-RU", "gl": "RU", "ceid": "RU%3Aru"},
        "CN": {"hl": "zh-CN", "gl": "CN", "ceid": "CN%3Azh"}
    }

    params = country_params.get(country, country_params["US"])
    articles_collected = 0
    target_articles = min(max_articles, 500)  # Cap at 500

    # Try different approaches to get more results
    approaches = [
        "",  # Standard search
        "&tbm=nws",  # News tab
        "&tbs=sbd:1",  # Sort by date
        "&tbs=qdr:d",  # Past day
        "&tbs=qdr:w",  # Past week
        "&tbs=qdr:m",  # Past month
    ]

    for approach in approaches:
        if articles_collected >= target_articles:
            break

        try:
            time.sleep(3)

            # Try with different start parameters - increased range for more articles
            for start in range(0, 200, 10):  # Increased range
                if articles_collected >= target_articles:
                    break

                url = f"https://news.google.com/search?q={query_encoded}{approach}&start={start}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"

                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                articles = soup.find_all('article')
                if not articles:  # No more articles found
                    break

                links = [article.find('a')['href'] for article in articles if article.find('a')]
                links = [link.replace("./articles/", "https://news.google.com/articles/") for link in links]

                news_text = [article.get_text(separator='\n') for article in articles]
                news_text_split = [text.split('\n') for text in news_text]

                while len(links) < len(news_text_split):
                    links.append('Missing')

                batch_articles = []
                for i, text in enumerate(news_text_split):
                    if articles_collected >= target_articles:
                        break

                    article_data = {
                        'Title': text[2] if len(text) > 2 else 'Missing',
                        'Source': text[0] if len(text) > 0 else 'Missing',
                        'Time': text[3] if len(text) > 3 else 'Missing',
                        'Author': text[4].split('By ')[-1] if len(text) > 4 else 'Missing',
                        'Link': links[i] if i < len(links) else 'Missing',
                        'Approach': approach if approach else 'standard',
                        'Page': start // 10 + 1
                    }
                    batch_articles.append(article_data)
                    articles_collected += 1

                all_articles.extend(batch_articles)

                # If we got fewer than 10 articles, this approach is exhausted
                if len(batch_articles) < 10:
                    break

                time.sleep(2)  # Delay between pages

        except Exception as e:
            st.warning(f"Error with approach {approach}: {e}")
            continue

    if not all_articles:
        return pd.DataFrame()

    # Create DataFrame and remove duplicates
    df = pd.DataFrame(all_articles)
    df = df.drop_duplicates(subset=['Title', 'Link'], keep='first')

    return df


def scrape_google_news(query: str, country: str = "US", method: str = "time_ranges", max_articles: int = 100) -> pd.DataFrame:
    """Main Google News extraction function with multiple methods"""
    if method == "time_ranges":
        return scrape_google_news_with_time_ranges(query, country, max_articles)
    elif method == "pagination":
        return scrape_google_news_with_pagination(query, country, max_articles)
    else:
        # Original method as fallback
        return scrape_google_news_with_time_ranges(query, country, max_articles)


def create_download_link(df, filename):
    """Create a download link for DataFrame as CSV"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download {filename}</a>'
    return href


# Streamlit App
st.title("Reddit & Google News Textual Content Extractor")
st.markdown("---")

# Reddit Extractor Section
st.header("🔴 Reddit Textual Content Extractor")

# Mode selection
extraction_mode = st.selectbox(
    "Modalità di estrazione:",
    ["Single Post", "Subreddit"],
    help="Single Post: Estrae da un singolo post | Subreddit: Estrae da multipli post di un subreddit",
    key="extraction_mode"
)

if extraction_mode == "Single Post":
    st.subheader("Estrazione Post Singolo")

    reddit_url = st.text_input(
        "URL Reddit:",
        placeholder="https://www.reddit.com/r/subreddit/comments/...",
        key="reddit_url"
    )

    reddit_sort = st.selectbox(
        "Ordina commenti per:",
        ["new", "old", "top"],
        key="reddit_sort"
    )

else:  # Subreddit mode
    st.subheader("Estrazione Subreddit")

    col1, col2 = st.columns([2, 1])

    with col1:
        subreddit_name = st.text_input(
            "Nome Subreddit:",
            placeholder="italy, programming, news (senza r/)",
            key="subreddit_name"
        )

    with col2:
        time_range_days = st.selectbox(
            "Intervallo temporale:",
            [1, 3, 7, 14, 30],
            index=2,
            format_func=lambda x: f"Ultimi {x} giorni",
            key="time_range_days"
        )

    col3, col4, col5 = st.columns(3)

    with col3:
        subreddit_sort = st.selectbox(
            "Ordina post per:",
            ["hot", "new", "top", "rising"],
            key="subreddit_sort"
        )

    with col4:
        max_posts = st.number_input(
            "Numero massimo post:",
            min_value=1,
            max_value=50,
            value=10,
            key="max_posts"
        )

    with col5:
        max_comments_per_post = st.number_input(
            "Commenti per post:",
            min_value=5,
            max_value=200,
            value=50,
            key="max_comments_per_post"
        )

# Dynamic button text based on mode
button_text = "Extract Reddit Post" if extraction_mode == "Single Post" else "Extract Subreddit"

if st.button(button_text, key="reddit_button"):
    if extraction_mode == "Single Post":
        if reddit_url:
            with st.spinner("Extracting Reddit post..."):
                post_data = scrape_reddit_post(reddit_url, reddit_sort)

                if post_data and post_data.get("comments"):
                    df_comments = process_comments_with_pandas(post_data["comments"])

                    # Show results

                    # Show debug information
                    if len(df_comments) > 0:
                        st.info(f"Raw comments extracted: {len(post_data['comments'])}")
                        st.info(f"Comments after deduplication: {len(df_comments)}")

                        # Show nesting statistics
                        if 'depth' in df_comments.columns:
                            depth_stats = df_comments['depth'].value_counts().sort_index()
                            st.info("**Comment depth distribution:**")
                            for depth, count in depth_stats.items():
                                indent = "  " * int(depth)
                                st.write(f"{indent}Depth {depth}: {count} comments")

                        # Show parent-child relationships
                        top_level_comments = len(df_comments[df_comments['depth'] == 0]) if 'depth' in df_comments.columns else len(df_comments)
                        nested_comments = len(df_comments) - top_level_comments
                        st.info(f"Top-level comments: {top_level_comments}")
                        st.info(f"Nested replies: {nested_comments}")

                        # Show full dataset
                        st.dataframe(df_comments)
                    else:
                        st.warning("No valid comments found after processing.")

                    # Download button
                    csv = df_comments.to_csv(index=False)
                    st.download_button(
                        label="Download Reddit Comments CSV",
                        data=csv,
                        file_name="reddit_comments.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No comments found or error occurred during extraction.")
                    # Show debug info even when no comments found
                    if post_data:
                        st.info(f"Post data available: {bool(post_data.get('info'))}")
                        st.info(f"Raw comments list length: {len(post_data.get('comments', []))}")
        else:
            st.error("Please enter a Reddit URL")

    else:  # Subreddit mode
        if subreddit_name:
            with st.spinner(f"Extracting from r/{subreddit_name}... This may take several minutes..."):
                # Create progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_progress(progress, status):
                    progress_bar.progress(progress)
                    status_text.text(status)

                update_progress(0.1, "Fetching posts from subreddit...")

                subreddit_data = scrape_subreddit_comments(
                    subreddit_name,
                    time_range_days,
                    subreddit_sort,
                    max_posts,
                    max_comments_per_post,
                    progress_callback=update_progress
                )

                progress_bar.progress(1.0)
                status_text.text("Content extraction complete!")

                if subreddit_data and subreddit_data.get("comments"):
                    df_comments = process_comments_with_pandas(subreddit_data["comments"])
                    summary = subreddit_data.get("summary", {})

                    # Show results

                    # Show summary statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Posts Found", summary.get('total_posts_found', 0))
                    with col2:
                        st.metric("Posts Processed", summary.get('total_posts_processed', 0))
                    with col3:
                        st.metric("Total Comments", summary.get('total_comments', 0))

                    # Show detailed statistics
                    if len(df_comments) > 0:
                        st.subheader("📊 Content Extraction Statistics")

                        # Show nesting statistics
                        if 'depth' in df_comments.columns:
                            depth_stats = df_comments['depth'].value_counts().sort_index()
                            st.write("**Comment depth distribution:**")
                            for depth, count in depth_stats.items():
                                indent = "  " * int(depth)
                                st.write(f"{indent}Depth {depth}: {count} comments")

                        # Show posts breakdown
                        if 'post_title' in df_comments.columns:
                            st.subheader("📝 Posts with Comments")
                            post_stats = df_comments.groupby('post_title').size().sort_values(ascending=False)
                            for post_title, comment_count in post_stats.head(10).items():
                                st.write(f"• {post_title[:60]}... ({comment_count} comments)")

                        # Show full dataset
                        st.subheader("💬 All Comments")
                        st.dataframe(df_comments)

                    # Download buttons
                    if len(df_comments) > 0:
                        col1, col2 = st.columns(2)

                        with col1:
                            csv_comments = df_comments.to_csv(index=False)
                            st.download_button(
                                label="Download Comments CSV",
                                data=csv_comments,
                                file_name=f"reddit_{subreddit_name}_comments.csv",
                                mime="text/csv"
                            )

                        with col2:
                            if subreddit_data.get("posts"):
                                df_posts = pd.DataFrame(subreddit_data["posts"])
                                csv_posts = df_posts.to_csv(index=False)
                                st.download_button(
                                    label="Download Posts CSV",
                                    data=csv_posts,
                                    file_name=f"reddit_{subreddit_name}_posts.csv",
                                    mime="text/csv"
                                )

                else:
                    st.warning("No comments found or error occurred during extraction.")
                    if subreddit_data.get("error"):
                        st.error(f"Error: {subreddit_data['error']}")
        else:
            st.error("Please enter a subreddit name")

st.markdown("---")

# Google News Extractor Section
st.header("📰 Google News Textual Content Extractor")
st.subheader("Inserisci query di ricerca Google News")

col1, col2 = st.columns([3, 1])

with col1:
    news_query = st.text_input(
        "Query di ricerca:",
        placeholder="US Economy, Italian Politics, etc.",
        key="news_query"
    )

with col2:
    country = st.selectbox(
        "Paese/Lingua:",
        ["US", "IT", "UK", "DE", "FR", "ES", "CA", "AU", "JP", "BR", "IN", "RU", "CN"],
        format_func=lambda x: {
            "US": "🇺🇸 United States (English)",
            "IT": "🇮🇹 Italy (Italiano)",
            "UK": "🇬🇧 United Kingdom (English)",
            "DE": "🇩🇪 Germany (Deutsch)",
            "FR": "🇫🇷 France (Français)",
            "ES": "🇪🇸 Spain (Español)",
            "CA": "🇨🇦 Canada (English)",
            "AU": "🇦🇺 Australia (English)",
            "JP": "🇯🇵 Japan (日本語)",
            "BR": "🇧🇷 Brazil (Português)",
            "IN": "🇮🇳 India (English)",
            "RU": "🇷🇺 Russia (Русский)",
            "CN": "🇨🇳 China (中文)"
        }[x],
        key="country_select"
    )

# Add advanced configuration options
col3, col4 = st.columns([1, 1])

with col3:
    extraction_method = st.selectbox(
        "Metodo di estrazione:",
        ["time_ranges", "pagination"],
        help="time_ranges: cerca in diversi periodi temporali | pagination: prova paginazione avanzata",
        key="extraction_method"
    )

with col4:
    max_articles = st.number_input(
        "Numero massimo articoli:",
        min_value=1,
        max_value=500,
        value=100,
        step=10,
        help="Numero massimo di articoli da raccogliere (1-500)",
        key="max_articles"
    )

if st.button("Extract Google News", key="news_button"):
    if news_query:
        with st.spinner(f"Extracting Google News... Targeting {max_articles} articles..."):
            # Add progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()

            status_text.text("Starting Google News content extraction...")
            progress_bar.progress(0.1)

            df_news = scrape_google_news(news_query, country, extraction_method, max_articles)

            progress_bar.progress(1.0)
            status_text.text("Content extraction complete!")

            if not df_news.empty:
                # Show results

                # Show enhanced statistics
                col1, col2 = st.columns(2)

                with col1:
                    st.metric("Articles Collected", len(df_news))
                    st.metric("Target Articles", max_articles)

                with col2:
                    # Show coverage percentage
                    coverage = min(100, (len(df_news) / max_articles) * 100)
                    st.metric("Collection Rate", f"{coverage:.1f}%")

                    # Show unique sources
                    unique_sources = df_news['Source'].nunique() if 'Source' in df_news.columns else 0
                    st.metric("Unique Sources", unique_sources)

                # Show detailed breakdown
                st.subheader("📊 Content Extraction Statistics")

                if 'Time_Range' in df_news.columns:
                    st.write("**Articles by time range:**")
                    time_range_stats = df_news['Time_Range'].value_counts()
                    for time_range, count in time_range_stats.items():
                        st.write(f"• {time_range}: {count} articles")

                if 'Approach' in df_news.columns:
                    st.write("**Articles by search approach:**")
                    approach_stats = df_news['Approach'].value_counts()
                    for approach, count in approach_stats.items():
                        st.write(f"• {approach}: {count} articles")

                if 'Page' in df_news.columns:
                    st.write("**Articles by page:**")
                    page_stats = df_news['Page'].value_counts().sort_index()
                    for page, count in page_stats.items():
                        st.write(f"• Page {page}: {count} articles")

                # Show source distribution
                if 'Source' in df_news.columns:
                    st.write("**Top 10 News Sources:**")
                    source_counts = df_news['Source'].value_counts().head(10)
                    for source, count in source_counts.items():
                        st.write(f"• {source}: {count} articles")

                # Show full dataset
                st.subheader("📰 All Articles")
                st.dataframe(df_news)

                # Download button
                csv = df_news.to_csv(index=False)
                st.download_button(
                    label="Download Google News CSV",
                    data=csv,
                    file_name=f"google_news_{country.lower()}_{max_articles}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No news articles found or error occurred during extraction.")
                st.info("Try adjusting your search query or increasing the article limit.")
    else:
        st.error("Please enter a search query")

st.markdown("---")
st.markdown("*Made with Streamlit*")
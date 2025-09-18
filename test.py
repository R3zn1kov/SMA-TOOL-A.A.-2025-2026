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
from datetime import datetime
import re
from urllib.parse import urlparse
import unicodedata
import io
import base64

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

    def parse_comment(parent_selector, parent_id=None) -> Dict:
        """parse a comment object"""
        author = parent_selector.xpath("./@data-author").get()
        link = parent_selector.xpath("./@data-permalink").get()
        dislikes = parent_selector.xpath(".//span[contains(@class, 'dislikes')]/@title").get()
        upvotes = parent_selector.xpath(".//span[contains(@class, 'likes')]/@title").get()
        downvotes = parent_selector.xpath(".//span[contains(@class, 'unvoted')]/@title").get()
        comment_id = parent_selector.xpath("./@data-fullname").get()

        comment_body = parent_selector.xpath(".//div[@class='md']/p/text()").get()
        if not comment_body:
            comment_body = parent_selector.xpath(".//div[contains(@class, 'usertext-body')]/div/p/text()").get()

        normalized_body = normalize_text(comment_body) if comment_body else None

        try:
            subreddit = response.url.split("/r/")[1].split("/")[0] if "/r/" in response.url else None
        except:
            subreddit = None

        comment_data = {
            "comment_id": comment_id,
            "parent_id": parent_id,
            "author": author,
            "author_id": parent_selector.xpath("./@data-author-fullname").get(),
            "subreddit": subreddit,
            "link": "https://www.reddit.com" + link if link else None,
            "created_time": parent_selector.xpath(".//time/@datetime").get(),
            "body": normalized_body,
            "body_processed": process_text_with_nltk(normalized_body) if normalized_body else None,
            "score": upvotes if upvotes else (0 if downvotes else None),
        }

        comments_list.append(comment_data)
        return comment_id

    def process_replies(what, parent_comment_id):
        """recursively process replies"""
        for reply_box in what.xpath(".//div[@data-type='comment']"):
            reply_comment_id = parse_comment(reply_box, parent_comment_id)
            process_replies(reply_box, reply_comment_id)

    selector = Selector(response.text)
    for item in selector.xpath("//div[@class='sitetable nestedlisting']/div[@data-type='comment']"):
        comment_id = parse_comment(item)
        process_replies(item, comment_id)

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


def scrape_reddit_post(url: str, sort: Union["old", "new", "top"] = "new") -> Dict:
    """scrape subreddit post and comment data"""
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
            bulk_comments_page_url = f"{old_reddit_url}&sort={sort}"
        else:
            bulk_comments_page_url = f"{old_reddit_url}?sort={sort}"

        # Add another delay before fetching comments
        time.sleep(2)
        response = session.get(bulk_comments_page_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        post_data["comments"] = parse_post_comments(response)

        return post_data

    except requests.exceptions.Timeout:
        st.error("Request timed out. The server took too long to respond.")
        return {"info": {}, "comments": []}
    except requests.exceptions.RequestException as e:
        st.error(f"Request error: {e}")
        return {"info": {}, "comments": []}
    except Exception as e:
        st.error(f"Error during scraping: {e}")
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


def scrape_google_news_with_time_ranges(query: str, country: str = "US") -> pd.DataFrame:
    """Scrape Google News with multiple time ranges to get more articles"""
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

    # Country-specific URL parameters
    country_params = {
        "US": {"hl": "en-US", "gl": "US", "ceid": "US%3Aen"},
        "IT": {"hl": "it-IT", "gl": "IT", "ceid": "IT%3Ait"}
    }

    params = country_params.get(country, country_params["US"])

    for time_range in time_ranges:
        try:
            # Add delay to avoid being blocked
            import time
            time.sleep(3)  # Increased delay between requests

            url = f"https://news.google.com/search?q={query_encoded}{time_range}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"

            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            articles = soup.find_all('article')
            links = [article.find('a')['href'] for article in articles if article.find('a')]
            links = [link.replace("./articles/", "https://news.google.com/articles/") for link in links]

            news_text = [article.get_text(separator='\n') for article in articles]
            news_text_split = [text.split('\n') for text in news_text]

            # Ensure we have enough links for all articles
            while len(links) < len(news_text_split):
                links.append('Missing')

            for i, text in enumerate(news_text_split):
                article_data = {
                    'Title': text[2] if len(text) > 2 else 'Missing',
                    'Source': text[0] if len(text) > 0 else 'Missing',
                    'Time': text[3] if len(text) > 3 else 'Missing',
                    'Author': text[4].split('By ')[-1] if len(text) > 4 else 'Missing',
                    'Link': links[i] if i < len(links) else 'Missing',
                    'Time_Range': time_range.replace('&when:', '') if time_range else 'all_time'
                }
                all_articles.append(article_data)

        except requests.exceptions.Timeout:
            st.warning(f"Request timed out for time range: {time_range}")
            continue
        except Exception as e:
            st.warning(f"Error scraping time range {time_range}: {e}")
            continue

    if not all_articles:
        return pd.DataFrame()

    # Create DataFrame and remove duplicates based on title and link
    df = pd.DataFrame(all_articles)
    df = df.drop_duplicates(subset=['Title', 'Link'], keep='first')

    return df


def scrape_google_news_with_pagination(query: str, country: str = "US", max_pages: int = 5) -> pd.DataFrame:
    """Attempt to scrape multiple pages of Google News results"""
    all_articles = []

    query_encoded = encode_special_characters(query)

    # Country-specific URL parameters
    country_params = {
        "US": {"hl": "en-US", "gl": "US", "ceid": "US%3Aen"},
        "IT": {"hl": "it-IT", "gl": "IT", "ceid": "IT%3Ait"}
    }

    params = country_params.get(country, country_params["US"])

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
        try:
            import time
            time.sleep(3)

            # Try with different start parameters
            for start in range(0, max_pages * 10, 10):
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

                for i, text in enumerate(news_text_split):
                    article_data = {
                        'Title': text[2] if len(text) > 2 else 'Missing',
                        'Source': text[0] if len(text) > 0 else 'Missing',
                        'Time': text[3] if len(text) > 3 else 'Missing',
                        'Author': text[4].split('By ')[-1] if len(text) > 4 else 'Missing',
                        'Link': links[i] if i < len(links) else 'Missing',
                        'Approach': approach if approach else 'standard',
                        'Page': start // 10 + 1
                    }
                    all_articles.append(article_data)

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


def scrape_google_news(query: str, country: str = "US", method: str = "time_ranges") -> pd.DataFrame:
    """Main Google News scraping function with multiple methods"""
    if method == "time_ranges":
        return scrape_google_news_with_time_ranges(query, country)
    elif method == "pagination":
        return scrape_google_news_with_pagination(query, country)
    else:
        # Original method as fallback
        return scrape_google_news_with_time_ranges(query, country)


def create_download_link(df, filename):
    """Create a download link for DataFrame as CSV"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download {filename}</a>'
    return href


# Streamlit App
st.title("Reddit & Google News Scraper")
st.markdown("---")

# Reddit Scraper Section
st.header("🔴 Reddit Scraper")
st.subheader("Inserisci URL del post Reddit")

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

if st.button("Scrape Reddit Post", key="reddit_button"):
    if reddit_url:
        with st.spinner("Scraping Reddit post..."):
            post_data = scrape_reddit_post(reddit_url, reddit_sort)

            if post_data and post_data.get("comments"):
                df_comments = process_comments_with_pandas(post_data["comments"])

                st.success(f"Successfully scraped {len(df_comments)} comments!")
                st.dataframe(df_comments.head())

                # Download button
                csv = df_comments.to_csv(index=False)
                st.download_button(
                    label="Download Reddit Comments CSV",
                    data=csv,
                    file_name="reddit_comments.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No comments found or error occurred during scraping.")
    else:
        st.error("Please enter a Reddit URL")

st.markdown("---")

# Google News Scraper Section
st.header("📰 Google News Scraper")
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
        "Paese:",
        ["US", "IT"],
        key="country_select"
    )

# Add scraping method selection
scraping_method = st.selectbox(
    "Metodo di scraping (per più articoli):",
    ["time_ranges", "pagination"],
    help="time_ranges: cerca in diversi periodi temporali | pagination: prova paginazione",
    key="scraping_method"
)

if st.button("Scrape Google News", key="news_button"):
    if news_query:
        with st.spinner("Scraping Google News... This may take a while to get more articles..."):
            df_news = scrape_google_news(news_query, country, scraping_method)

            if not df_news.empty:
                st.success(f"Successfully scraped {len(df_news)} news articles!")

                # Show some statistics
                if 'Time_Range' in df_news.columns:
                    st.write("**Articles by time range:**")
                    st.write(df_news['Time_Range'].value_counts())
                elif 'Approach' in df_news.columns:
                    st.write("**Articles by search approach:**")
                    st.write(df_news['Approach'].value_counts())

                st.dataframe(df_news)

                # Download button
                csv = df_news.to_csv(index=False)
                st.download_button(
                    label="Download Google News CSV",
                    data=csv,
                    file_name="google_news.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No news articles found or error occurred during scraping.")
    else:
        st.error("Please enter a search query")

st.markdown("---")
st.markdown("*Made with Streamlit*")
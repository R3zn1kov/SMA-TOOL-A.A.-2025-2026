import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from typing import Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class GoogleNewsExtractor:
    """Google News Textual Content Extractor"""

    def __init__(self, request_timeout: int = 30):
        """Initialize Google News extractor with session and configuration"""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        })
        self.request_timeout = request_timeout

        # Extended country and language parameters
        self.country_params = {
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

    @staticmethod
    def encode_special_characters(text):
        """Encode special characters in a text string"""
        encoded_text = ''
        special_characters = {'&': '%26', '=': '%3D', '+': '%2B', ' ': '%20'}
        for char in text.lower():
            encoded_text += special_characters.get(char, char)
        return encoded_text

    def extract_with_time_ranges(self, query: str, country: str = "US", max_articles: int = 100) -> pd.DataFrame:
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

        query_encoded = self.encode_special_characters(query)

        params = self.country_params.get(country, self.country_params["US"])
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

                    response = requests.get(url, timeout=self.request_timeout)
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
                log.warning(f"Request timed out for time range: {time_range}")
                continue
            except Exception as e:
                log.warning(f"Error extracting time range {time_range}: {e}")
                continue

        if not all_articles:
            return pd.DataFrame()

        # Create DataFrame and remove duplicates based on title and link
        df = pd.DataFrame(all_articles)
        df = df.drop_duplicates(subset=['Title', 'Link'], keep='first')

        return df

    def extract_with_pagination(self, query: str, country: str = "US", max_articles: int = 100) -> pd.DataFrame:
        """Attempt to extract multiple pages of Google News results"""
        all_articles = []

        query_encoded = self.encode_special_characters(query)

        params = self.country_params.get(country, self.country_params["US"])
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

                    response = requests.get(url, timeout=self.request_timeout)
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
                log.warning(f"Error with approach {approach}: {e}")
                continue

        if not all_articles:
            return pd.DataFrame()

        # Create DataFrame and remove duplicates
        df = pd.DataFrame(all_articles)
        df = df.drop_duplicates(subset=['Title', 'Link'], keep='first')

        return df

    def extract_google_news(self, query: str, country: str = "US", method: str = "time_ranges", max_articles: int = 100) -> pd.DataFrame:
        """Main Google News extraction function with multiple methods"""
        if method == "time_ranges":
            return self.extract_with_time_ranges(query, country, max_articles)
        elif method == "pagination":
            return self.extract_with_pagination(query, country, max_articles)
        else:
            # Original method as fallback
            return self.extract_with_time_ranges(query, country, max_articles)

    @staticmethod
    def create_download_link(df, filename):
        """Create a download link for DataFrame as CSV"""
        import base64
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download {filename}</a>'
        return href
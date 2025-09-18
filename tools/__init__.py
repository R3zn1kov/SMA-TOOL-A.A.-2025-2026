"""
Tools package for Reddit and Google News textual content extraction.

This package contains the core extraction classes:
- RedditExtractor: For extracting content from Reddit posts and subreddits
- GoogleNewsExtractor: For extracting content from Google News articles
"""

from .reddit import RedditExtractor
from .google_news import GoogleNewsExtractor

__all__ = ['RedditExtractor', 'GoogleNewsExtractor']
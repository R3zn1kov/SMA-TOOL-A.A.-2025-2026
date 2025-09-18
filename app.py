import streamlit as st
import pandas as pd
from reddit import RedditExtractor
from google_news import GoogleNewsExtractor

# Streamlit App
st.title("Reddit & Google News Textual Content Extractor")
st.markdown("---")

# Initialize extractors
reddit_extractor = RedditExtractor()
google_news_extractor = GoogleNewsExtractor()

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
                post_data = reddit_extractor.extract_reddit_post(reddit_url, reddit_sort)

                if post_data and post_data.get("comments"):
                    df_comments = reddit_extractor.process_comments_with_pandas(post_data["comments"])

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

                subreddit_data = reddit_extractor.extract_subreddit_comments(
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
                    df_comments = reddit_extractor.process_comments_with_pandas(subreddit_data["comments"])
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

            df_news = google_news_extractor.extract_google_news(news_query, country, extraction_method, max_articles)

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
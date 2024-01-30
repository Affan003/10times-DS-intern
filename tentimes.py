import feedparser
import sqlite3
from celery import Celery
import logging
import nltk
from nltk.tokenize import word_tokenize

# Initialize Celery
celery = Celery('news_processor', broker='redis://localhost:6379/0')

# Configure logging
logging.basicConfig(filename='news_processing.log', level=logging.INFO)

# Function to classify category based on keywords in title or description
def classify_category(title, description):
    # Convert text to lowercase for case-insensitive matching
    title = title.lower()
    description = description.lower()
    
    # Check for keywords to classify into categories
    if any(keyword in title or keyword in description for keyword in ['terrorism', 'protest', 'political unrest', 'riot']):
        return 'Terrorism / protest / political unrest / riot'
    elif any(keyword in title or keyword in description for keyword in ['positive', 'uplifting', 'inspiring']):
        return 'Positive/Uplifting'
    elif any(keyword in title or keyword in description for keyword in ['natural disaster', 'earthquake', 'flood', 'hurricane']):
        return 'Natural Disasters'
    else:
        return 'Others'

# Step 1: Feed Parser and Data Extraction
def fetch_feeds(feed_urls):
    print("Fetching feeds...")
    articles = []
    for url in feed_urls:
        print(f"Fetching articles from {url}...")
        feed = feedparser.parse(url)
        for entry in feed.entries:
            title = entry.get('title', '')
            description = entry.get('description', '')
            link = entry.get('link', '')
            published_date = entry.get('published', '')
            source = url
            category = classify_category(title, description)
            article = {
                'title': title,
                'description': description,
                'link': link,
                'published_date': published_date,
                'source': source,
                'category': category
            }
            articles.append(article)
    print("Fetching feeds completed.")
    return articles

# Step 2: Database Storage
def store_articles(articles, db_file):
    print("Storing articles in the database...")
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS articles
                 (title TEXT, description TEXT, source TEXT, published_date TEXT, category TEXT, UNIQUE(title, source))''')
    for article in articles:
        try:
            c.execute("INSERT INTO articles (title, description, source, published_date, category) VALUES (?, ?, ?, ?, ?)",
                      (article['title'], article['description'], article['source'], article['published_date'], article['category']))
        except sqlite3.IntegrityError:
            logging.info(f"Duplicate article found: {article['title']} from {article['source']}")
    conn.commit()
    conn.close()
    print("Storing articles completed.")

# Step 3: Task Queue and News Processing
@celery.task
def process_articles():
    print("Processing articles...")
    conn = sqlite3.connect("news_articles.db")
    c = conn.cursor()
    c.execute("SELECT * FROM articles WHERE category IS NULL")
    articles = c.fetchall()
    for article in articles:
        # Update category for articles that were not classified during insertion
        category = classify_category(article['title'], article['description'])
        c.execute("UPDATE articles SET category = ? WHERE title = ? AND source = ?",
                  (category, article['title'], article['source']))
    conn.commit()
    conn.close()
    print("Processing articles completed.")

# Main Function
def main():
    nltk.download('punkt')  # Download NLTK tokenizer data
    feed_urls = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]
    print("Starting main function...")
    articles = fetch_feeds(feed_urls)
    store_articles(articles, "news_articles.db")
    process_articles.delay()  # Send articles for processing asynchronously
    print("Main function completed.")

if __name__ == "__main__":
    main()

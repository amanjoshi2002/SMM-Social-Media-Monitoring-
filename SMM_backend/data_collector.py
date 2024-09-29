import os
import json
import time
import glob
import requests
from flask import Flask, jsonify, request
from threading import Thread
from transformers import pipeline, AutoTokenizer
import re
from textblob import TextBlob
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Use relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TWITTER_FOLDER_PATH = os.path.join(BASE_DIR, "twitter", "scraper", "tweets")
TWITTER_OUTPUT_FILE = os.path.join(BASE_DIR, "twitter.json")
META_INPUT_FILE = os.path.join(BASE_DIR, "crime_hashtag_results.json")
META_OUTPUT_FILE = os.path.join(BASE_DIR, "meta.json")
HASHTAGS_FILE = os.path.join(BASE_DIR, 'hashtags.json')
META_INPUT_FOLDER = os.path.join(BASE_DIR, 'meta_input')

# Ensure the META_INPUT_FOLDER exists
os.makedirs(META_INPUT_FOLDER, exist_ok=True)

# Apify API settings
API_TOKEN = os.getenv('APIFY_API_TOKEN')
BASE_URL = os.getenv('APIFY_BASE_URL')

sentiment_pipeline = None
tokenizer = None

def initialize_sentiment_analysis():
    global sentiment_pipeline, tokenizer
    print("Initializing sentiment analysis...")
    sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
    tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
    print("Sentiment analysis initialized.")

def simple_sentiment_analysis(text):
    # Clean the text
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    
    # Perform sentiment analysis
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    
    # Classify the sentiment
    if sentiment_score > 0.05:
        score = (sentiment_score + 1) / 2
        sentiment = {"label": "POSITIVE", "score": score}
    elif sentiment_score < -0.05:
        score = (-sentiment_score + 1) / 2
        sentiment = {"label": "NEGATIVE", "score": score}
        if score > 0.6:
            sentiment["alert"] = 1
    else:
        sentiment = {"label": "NEUTRAL", "score": 0.5}
    
    return sentiment

def perform_sentiment_analysis(text):
    try:
        if sentiment_pipeline is None or tokenizer is None:
            return simple_sentiment_analysis(text)
        tokens = tokenizer(text, truncation=True, max_length=512, return_tensors="pt")
        truncated_text = tokenizer.decode(tokens['input_ids'][0], skip_special_tokens=True)
        result = sentiment_pipeline(truncated_text)[0]
        sentiment = {"label": result['label'], "score": result['score']}
        if result['label'] == 'NEGATIVE' and result['score'] > 0.6:
            sentiment["alert"] = 1
        return sentiment
    except Exception as e:
        print(f"Error in sentiment analysis: {str(e)}")
        return simple_sentiment_analysis(text)

def fetch_data(url, method="GET", payload=None):
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        return response.json()
    else:
        print(f"Error fetching data from {url}. Status code: {response.status_code}")
        print(response.text)
        return None

def load_hashtags():
    if os.path.exists(HASHTAGS_FILE):
        with open(HASHTAGS_FILE, 'r') as f:
            return json.load(f)
    return []

def run_meta_scraper(hashtag):
    print(f"Starting a new run of the Meta scraper for hashtag: {hashtag}")
    start_url = f"{BASE_URL}/acts/apify~social-media-hashtag-research/runs"
    
    run_input = {
        "hashtags": [hashtag],
        "maxPerSocial": 50,
        "socials": ["facebook", "instagram"]
    }

    new_run = fetch_data(start_url, method="POST", payload=run_input)

    if not new_run:
        print("Failed to start a new run.")
        return

    run_id = new_run['data']['id']
    print(f"New run started with ID: {run_id}")

    while True:
        status_url = f"{BASE_URL}/acts/apify~social-media-hashtag-research/runs/{run_id}"
        run_status = fetch_data(status_url)
        
        if run_status['data']['status'] == "SUCCEEDED":
            print("Run completed successfully.")
            break
        elif run_status['data']['status'] in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"Run failed with status: {run_status['data']['status']}")
            return
        else:
            print("Run still in progress. Waiting...")
            time.sleep(10)

    dataset_id = run_status['data']['defaultDatasetId']
    dataset_url = f"{BASE_URL}/datasets/{dataset_id}/items"

    print("Fetching dataset items...")
    dataset_items = fetch_data(dataset_url)

    if dataset_items:
        print(f"Fetched {len(dataset_items)} items for hashtag: {hashtag}")
        
        # Generate a unique filename for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = os.path.join(META_INPUT_FOLDER, f"{hashtag}_{timestamp}.json")
        
        # Save output data to a JSON file
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(dataset_items, f, ensure_ascii=False, indent=2)
        print(f"\nOutput data saved to {json_filename}")

        print("\nSample results:")
        for item in dataset_items[:2]:
            print(json.dumps(item, indent=2))
            print("---")
    else:
        print(f"Failed to fetch dataset items for hashtag: {hashtag}")

def merge_twitter_files():
    all_tweets = []
    tweet_ids = set()

    json_files = glob.glob(os.path.join(TWITTER_FOLDER_PATH, '*.json'))

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                for tweet in data:
                    if tweet['id'] not in tweet_ids:
                        # Perform sentiment analysis on the tweet text
                        sentiment = perform_sentiment_analysis(tweet.get('text', ''))
                        if sentiment:
                            tweet['sentiment'] = sentiment
                        all_tweets.append(tweet)
                        tweet_ids.add(tweet['id'])
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file: {file}")

    all_tweets.sort(key=lambda x: x.get('creationDate', ''), reverse=True)

    with open(TWITTER_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_tweets, f, ensure_ascii=False, indent=2)

    print(f"Merged {len(all_tweets)} tweets into {TWITTER_OUTPUT_FILE}")

def merge_meta_data():
    all_meta_posts = []
    post_ids = set()

    # Read existing data from META_OUTPUT_FILE if it exists
    if os.path.exists(META_OUTPUT_FILE):
        with open(META_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            for post in existing_data:
                if 'id' in post and post['id'] not in post_ids:
                    all_meta_posts.append(post)
                    post_ids.add(post['id'])

    # Process all JSON files in the META_INPUT_FOLDER
    for filename in os.listdir(META_INPUT_FOLDER):
        if filename.endswith('.json'):
            file_path = os.path.join(META_INPUT_FOLDER, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    for post in data:
                        if 'id' in post and post['id'] not in post_ids:
                            # Perform sentiment analysis on the post text
                            sentiment = perform_sentiment_analysis(post.get('text', ''))
                            if sentiment:
                                post['sentiment'] = sentiment
                            all_meta_posts.append(post)
                            post_ids.add(post['id'])
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from file: {filename}")

    # Sort posts by postedAt date
    all_meta_posts.sort(key=lambda x: x.get('postedAt', ''), reverse=True)

    # Save merged data to META_OUTPUT_FILE
    with open(META_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_meta_posts, f, ensure_ascii=False, indent=2)

    print(f"Merged {len(all_meta_posts)} meta posts into {META_OUTPUT_FILE}")

def periodic_merge_and_scrape():
    while True:
        print("Starting periodic merge and scrape...")
        merge_twitter_files()
        
        hashtags = load_hashtags()
        if not hashtags:
            print("No hashtags found. Skipping Meta scraper run.")
        else:
            for hashtag in hashtags:
                run_meta_scraper(hashtag)
                print(f"Waiting for 1 minute before processing the next hashtag...")
                time.sleep(60)  # Wait for 1 minute between hashtags
        
        merge_meta_data()
        print("Periodic merge and scrape completed. Starting over...")

@app.route('/start_scrapers', methods=['GET', 'POST'])
def start_scrapers():
    return jsonify({"message": "Scrapers are running continuously"}), 200

@app.route('/twitter_data', methods=['GET'])
def get_twitter_data():
    try:
        with open(TWITTER_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Twitter data file not found"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Error reading Twitter data file"}), 500

@app.route('/meta_data', methods=['GET'])
def get_meta_data():
    try:
        with open(META_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Meta data file not found"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Error reading Meta data file"}), 500

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "status": "running",
        "twitter_file": os.path.exists(TWITTER_OUTPUT_FILE),
        "meta_file": os.path.exists(META_OUTPUT_FILE),
        "sentiment_analysis": sentiment_pipeline is not None
    }), 200

if __name__ == "__main__":
    # Start the periodic merge and scrape in a separate thread
    merge_scrape_thread = Thread(target=periodic_merge_and_scrape)
    merge_scrape_thread.daemon = True
    merge_scrape_thread.start()

    # Start the sentiment analysis initialization in a separate thread
    sentiment_thread = Thread(target=initialize_sentiment_analysis)
    sentiment_thread.daemon = True
    sentiment_thread.start()

    # Run the Flask app
    app.run(port=5001, debug=True, use_reloader=False)
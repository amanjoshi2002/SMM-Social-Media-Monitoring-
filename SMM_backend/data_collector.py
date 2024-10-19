import os
import json
import time
import glob
from flask import Flask, jsonify
from threading import Thread
from transformers import pipeline, AutoTokenizer
import re
from textblob import TextBlob
from dotenv import load_dotenv
from twilio_sender import send_sms_alert

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Use environment variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TWITTER_FOLDER_PATH = os.path.join(BASE_DIR, os.getenv('TWITTER_FOLDER_PATH'))
TWITTER_OUTPUT_FILE = os.path.join(BASE_DIR, os.getenv('TWITTER_OUTPUT_FILE'))
META_OUTPUT_FILE = os.path.join(BASE_DIR, os.getenv('META_OUTPUT_FILE'))
SENTIMENT_MODEL = os.getenv('SENTIMENT_MODEL')
MERGE_INTERVAL = int(os.getenv('MERGE_INTERVAL'))

sentiment_pipeline = None
tokenizer = None

def initialize_sentiment_analysis():
    global sentiment_pipeline, tokenizer
    print("Initializing sentiment analysis...")
    sentiment_pipeline = pipeline("sentiment-analysis", model=SENTIMENT_MODEL)
    tokenizer = AutoTokenizer.from_pretrained(SENTIMENT_MODEL)
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

def merge_twitter_files():
    all_tweets = []
    tweet_ids = set()

    print(f"Looking for JSON files in: {TWITTER_FOLDER_PATH}")
    json_files = glob.glob(os.path.join(TWITTER_FOLDER_PATH, '*.json'))
    print(f"Found {len(json_files)} JSON files")

    for file in json_files:
        print(f"Processing file: {file}")
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                print(f"Loaded {len(data)} tweets from {file}")
                for tweet in data:
                    tweet_id = tweet.get('Tweet ID', '').split(':')[-1]
                    if not tweet_id:
                        print(f"Warning: Tweet without id found in {file}")
                        continue
                    if tweet_id not in tweet_ids:
                        # Perform sentiment analysis on the tweet content
                        sentiment = perform_sentiment_analysis(tweet.get('Content', ''))
                        if sentiment and sentiment['label'] == 'NEGATIVE':
                            tweet['sentiment'] = sentiment
                            # Rename some fields to match expected structure
                            tweet['id'] = tweet_id
                            tweet['text'] = tweet.get('Content', '')
                            tweet['creationDate'] = tweet.get('Timestamp', '')
                            all_tweets.append(tweet)
                            tweet_ids.add(tweet_id)
                            
                            # Send SMS alert if sentiment score is greater than 0.5
                            if sentiment['score'] > 0.5:
                                tweet_url = f"https://twitter.com/user/status/{tweet_id}"
                                send_sms_alert("Twitter", tweet_url)
                    else:
                        print(f"Duplicate tweet id found: {tweet_id}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file: {file}")

    all_tweets.sort(key=lambda x: x.get('creationDate', ''), reverse=True)

    print(f"Total negative tweets after merging: {len(all_tweets)}")
    print(f"Writing merged negative tweets to: {TWITTER_OUTPUT_FILE}")

    with open(TWITTER_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_tweets, f, ensure_ascii=False, indent=2)

    print(f"Merged {len(all_tweets)} negative tweets into {TWITTER_OUTPUT_FILE}")

def merge_meta_data():
    all_meta_posts = []
    post_ids = set()

    # Read existing data from META_OUTPUT_FILE if it exists
    if os.path.exists(META_OUTPUT_FILE):
        with open(META_OUTPUT_FILE, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            for post in existing_data:
                if 'id' in post and post['id'] not in post_ids and post.get('sentiment', {}).get('label') == 'NEGATIVE':
                    all_meta_posts.append(post)
                    post_ids.add(post['id'])

    # Process all JSON files in the META_INPUT_FOLDER
    meta_input_folder = os.path.join(BASE_DIR, 'meta_input')
    for filename in os.listdir(meta_input_folder):
        if filename.endswith('.json'):
            file_path = os.path.join(meta_input_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    for post in data:
                        if 'id' in post and post['id'] not in post_ids:
                            # Perform sentiment analysis on the post text
                            sentiment = perform_sentiment_analysis(post.get('text', ''))
                            if sentiment and sentiment['label'] == 'NEGATIVE':
                                post['sentiment'] = sentiment
                                all_meta_posts.append(post)
                                post_ids.add(post['id'])
                                
                                # Send SMS alert if sentiment score is greater than 0.5
                                if sentiment['score'] > 0.5:
                                    post_url = f"https://www.facebook.com/{post['id']}"  # Adjust this URL structure if needed
                                    send_sms_alert("Meta", post_url)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from file: {filename}")

    # Sort posts by postedAt date
    all_meta_posts.sort(key=lambda x: x.get('postedAt', ''), reverse=True)

    # Save merged data to META_OUTPUT_FILE
    with open(META_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_meta_posts, f, ensure_ascii=False, indent=2)

    print(f"Merged {len(all_meta_posts)} negative meta posts into {META_OUTPUT_FILE}")

def periodic_merge():
    while True:
        print("Starting periodic merge...")
        if not os.path.exists(TWITTER_FOLDER_PATH):
            print(f"Error: Twitter folder not found at {TWITTER_FOLDER_PATH}")
        else:
            merge_twitter_files()
        merge_meta_data()
        print("Periodic merge completed. Starting over...")
        time.sleep(MERGE_INTERVAL)

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "status": "running",
        "twitter_file": os.path.exists(TWITTER_OUTPUT_FILE),
        "meta_file": os.path.exists(META_OUTPUT_FILE),
        "sentiment_analysis": sentiment_pipeline is not None
    }), 200

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

if __name__ == "__main__":
    # Start the periodic merge in a separate thread
    merge_thread = Thread(target=periodic_merge)
    merge_thread.daemon = True
    merge_thread.start()

    # Start the sentiment analysis initialization in a separate thread
    sentiment_thread = Thread(target=initialize_sentiment_analysis)
    sentiment_thread.daemon = True
    sentiment_thread.start()

    # Run the Flask app
    app.run(port=int(os.getenv('FLASK_RUN_PORT')), debug=os.getenv('FLASK_DEBUG') == '1')

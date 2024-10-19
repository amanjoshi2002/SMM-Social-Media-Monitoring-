import os
import json
import time
import requests
from flask import Flask, jsonify
from threading import Thread
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Use environment variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
META_INPUT_FILE = os.path.join(BASE_DIR, os.getenv('META_INPUT_FILE'))
META_OUTPUT_FILE = os.path.join(BASE_DIR, os.getenv('META_OUTPUT_FILE'))
HASHTAGS_FILE = os.path.join(BASE_DIR, 'hashtags.json')
META_INPUT_FOLDER = os.path.join(BASE_DIR, 'meta_input')

# Ensure the META_INPUT_FOLDER exists
os.makedirs(META_INPUT_FOLDER, exist_ok=True)

# Apify API settings
API_TOKEN = os.getenv('APIFY_API_TOKEN')
BASE_URL = os.getenv('APIFY_BASE_URL')

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

def periodic_scrape():
    while True:
        print("Starting periodic scrape...")
        hashtags = load_hashtags()
        if not hashtags:
            print("No hashtags found. Skipping Meta scraper run.")
        else:
            for hashtag in hashtags:
                run_meta_scraper(hashtag)
                print(f"Waiting for 1 minute before processing the next hashtag...")
                time.sleep(60)  # Wait for 1 minute between hashtags
        
        print("Periodic scrape completed. Starting over...")
        time.sleep(3600)  # Wait for 1 hour before the next scrape

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "status": "running",
        "meta_input_folder": os.path.exists(META_INPUT_FOLDER),
        "hashtags": load_hashtags()
    }), 200

if __name__ == "__main__":
    # Start the periodic scrape in a separate thread
    scrape_thread = Thread(target=periodic_scrape)
    scrape_thread.daemon = True
    scrape_thread.start()

    # Run the Flask app
    app.run(port=int(os.getenv('FLASK_RUN_PORT')), debug=os.getenv('FLASK_DEBUG') == '1')

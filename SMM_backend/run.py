import os
import subprocess
import sys
import multiprocessing
import signal
import json
from flask import Flask, jsonify, request, Response
from user_scraper import scrape_twitter, scrape_instagram

app = Flask(__name__)

# Use relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HASHTAGS_FILE = os.path.join(BASE_DIR, 'hashtags.json')

twitter_process = None
meta_process = None
data_collector_process = None

# Hashtag handling functions
def load_hashtags():
    if os.path.exists(HASHTAGS_FILE):
        with open(HASHTAGS_FILE, 'r') as f:
            return json.load(f)
    return []

def save_hashtags(hashtags):
    with open(HASHTAGS_FILE, 'w') as f:
        json.dump(hashtags, f)

@app.route('/hashtags', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handle_hashtags():
    if request.method == 'GET':
        hashtags = load_hashtags()
        return jsonify(hashtags), 200

    elif request.method == 'POST':
        new_hashtags = request.json.get('hashtags', [])
        if not new_hashtags:
            new_hashtag = request.json.get('hashtag')
            if new_hashtag:
                new_hashtags = [new_hashtag]
            else:
                return jsonify({"error": "No hashtags provided"}), 400

        hashtags = load_hashtags()
        added_hashtags = []
        for hashtag in new_hashtags:
            if hashtag not in hashtags:
                hashtags.append(hashtag)
                added_hashtags.append(hashtag)
        
        save_hashtags(hashtags)
        
        if added_hashtags:
            return jsonify({"message": f"Added hashtags: {', '.join(added_hashtags)}"}), 201
        else:
            return jsonify({"message": "No new hashtags added. All provided hashtags already exist."}), 200

    elif request.method == 'PUT':
        old_hashtag = request.json.get('old_hashtag')
        new_hashtag = request.json.get('new_hashtag')
        if not old_hashtag or not new_hashtag:
            return jsonify({"error": "Both old_hashtag and new_hashtag are required"}), 400

        hashtags = load_hashtags()
        if old_hashtag in hashtags:
            hashtags[hashtags.index(old_hashtag)] = new_hashtag
            save_hashtags(hashtags)
            return jsonify({"message": f"Updated hashtag '{old_hashtag}' to '{new_hashtag}'"}), 200
        else:
            return jsonify({"error": f"Hashtag '{old_hashtag}' not found"}), 404

    elif request.method == 'DELETE':
        hashtags_to_delete = request.json.get('hashtags', [])
        if not hashtags_to_delete:
            hashtag = request.json.get('hashtag')
            if hashtag:
                hashtags_to_delete = [hashtag]
            else:
                return jsonify({"error": "No hashtags provided for deletion"}), 400

        hashtags = load_hashtags()
        deleted_hashtags = []
        for hashtag in hashtags_to_delete:
            if hashtag in hashtags:
                hashtags.remove(hashtag)
                deleted_hashtags.append(hashtag)
        save_hashtags(hashtags)
        return jsonify({"message": f"Deleted hashtags: {', '.join(deleted_hashtags)}"}), 200

def run_twitter_scraper():
    print("Starting Twitter scraper...")
    try:
        twitter_path = os.path.join(BASE_DIR, 'twittter')
        scraper_path = os.path.join(twitter_path, 'scraper')
        
        if not os.path.exists(scraper_path):
            print(f"Error: {scraper_path} does not exist.")
            return

        global_python = sys.executable

        env = os.environ.copy()
        if 'VIRTUAL_ENV' in env:
            del env['VIRTUAL_ENV']
        if 'PYTHONHOME' in env:
            del env['PYTHONHOME']
        env['PYTHONPATH'] = f"{twitter_path}{os.pathsep}{scraper_path}{os.pathsep}" + env.get('PYTHONPATH', '')

        process = subprocess.Popen([global_python, '-m', 'scraper'], 
                                   cwd=twitter_path,
                                   env=env,
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE, 
                                   text=True)
        
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(f"Twitter Scraper: {output.strip()}")
        
        errors = process.stderr.read()
        if errors:
            print(f"Errors/Warnings from Twitter Scraper:")
            print(errors)

    except Exception as e:
        print(f"Error running Twitter scraper:")
        print(str(e))

def run_meta_script():
    print("Starting meta.py script...")
    try:
        meta_path = os.path.join(BASE_DIR, 'meta.py')
        
        if not os.path.exists(meta_path):
            print(f"Error: {meta_path} does not exist.")
            return

        process = subprocess.Popen([sys.executable, meta_path], 
                                   cwd=BASE_DIR,
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE, 
                                   text=True)
        
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(f"Meta Script: {output.strip()}")
        
        errors = process.stderr.read()
        if errors:
            print(f"Errors/Warnings from Meta Script:")
            print(errors)

    except Exception as e:
        print(f"Error running Meta script:")
        print(str(e))

def run_data_collector():
    data_collector_path = os.path.join(BASE_DIR, 'data_collector.py')
    if not os.path.exists(data_collector_path):
        print(f"Error: {data_collector_path} does not exist.")
        return

    process = subprocess.Popen([sys.executable, data_collector_path],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
    
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(f"Data Collector: {output.strip()}")
    
    errors = process.stderr.read()
    if errors:
        print(f"Errors/Warnings from Data Collector:")
        print(errors)

def run_identify_script():
    process = subprocess.Popen([sys.executable, 'identify.py'], 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE, 
                               text=True)
    for stdout_line in iter(process.stdout.readline, ""):
        yield stdout_line
    process.stdout.close()
    return_code = process.wait()
    if return_code:
        yield f"Error: identify.py exited with code {return_code}"

@app.route('/start', methods=['POST'])
def start_scrapers():
    global twitter_process, meta_process, data_collector_process
    
    data = request.json
    start_twitter = data.get('twitter', False)
    start_meta = data.get('meta', False)

    if not start_twitter and not start_meta:
        return jsonify({"message": "Please specify at least one scraper to start"}), 400

    message = []

    if start_twitter:
        if twitter_process is None or not twitter_process.is_alive():
            twitter_process = multiprocessing.Process(target=run_twitter_scraper)
            twitter_process.start()
            message.append("Twitter scraper started")
        else:
            message.append("Twitter scraper is already running")

    if start_meta:
        if meta_process is None or not meta_process.is_alive():
            meta_process = multiprocessing.Process(target=run_meta_script)
            meta_process.start()
            message.append("Meta scraper started")
        else:
            message.append("Meta scraper is already running")

    # Start the data collector process if it's not already running
    if data_collector_process is None or not data_collector_process.is_alive():
        data_collector_process = multiprocessing.Process(target=run_data_collector)
        data_collector_process.start()
        message.append("Data collector started")
    else:
        message.append("Data collector is already running")

    return jsonify({"message": ". ".join(message)}), 200

@app.route('/stop', methods=['GET'])
def stop_scrapers():
    global twitter_process, meta_process, data_collector_process
    
    if twitter_process is None and meta_process is None:
        return jsonify({"message": "Scrapers are not running"}), 400

    if twitter_process:
        twitter_process.terminate()
        twitter_process.join()
        twitter_process = None

    if meta_process:
        meta_process.terminate()
        meta_process.join()
        meta_process = None

    if data_collector_process:
        data_collector_process.terminate()
        data_collector_process.join()
        data_collector_process = None

    return jsonify({"message": "All processes stopped successfully"}), 200

@app.route('/status', methods=['GET'])
def get_status():
    global twitter_process, meta_process, data_collector_process
    
    status = {
        "twitter": "Not running",
        "meta": "Not running",
        "data_collector": "Not running"
    }
    
    if twitter_process and twitter_process.is_alive():
        status["twitter"] = "Running"
    
    if meta_process and meta_process.is_alive():
        status["meta"] = "Running"
    
    if data_collector_process and data_collector_process.is_alive():
        status["data_collector"] = "Running"

    return jsonify(status), 200

@app.route('/scrape', methods=['POST'])
def scrape_user():
    data = request.json
    platform = data.get('platform', '').lower()
    handle = data.get('handle', '')
    num_posts = data.get('num_posts', 10)

    if not platform or not handle:
        return jsonify({"error": "Platform and handle are required"}), 400

    if platform == "twitter":
        result = scrape_twitter(handle, num_posts)
    elif platform == "instagram":
        result = scrape_instagram(handle, num_posts)
    else:
        return jsonify({"error": "Invalid platform. Please enter either 'twitter' or 'instagram'."}), 400

    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "Scraping failed"}), 500

def signal_handler(signum, frame):
    global twitter_process, meta_process, data_collector_process
    print("Stopping processes...")
    for process in [twitter_process, meta_process, data_collector_process]:
        if process:
            process.terminate()
            process.join()
    sys.exit(0)

@app.route('/start-identify', methods=['POST'])
def start_identify():
    def generate():
        for output in run_identify_script():
            yield f"data:{output}\n\n"

    return Response(generate(), mimetype='text/event-stream')

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the data collector process immediately
    data_collector_process = multiprocessing.Process(target=run_data_collector)
    data_collector_process.start()
    
    app.run(port=5000, debug=True, use_reloader=False)
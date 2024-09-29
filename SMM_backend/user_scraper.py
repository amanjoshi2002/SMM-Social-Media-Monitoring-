import requests
import time

# Apify API keys and URLs
TWITTER_API_URL = "https://api.apify.com/v2/acts/quacker~twitter-scraper/runs?token=apify_api_YgJ4mezLdx0vHenE7eR00zi3GcxEBJ32zRnS"
INSTAGRAM_API_URL = "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs?token=apify_api_YgJ4mezLdx0vHenE7eR00zi3GcxEBJ32zRnS"
API_KEY = "apify_api_YgJ4mezLdx0vHenE7eR00zi3GcxEBJ32zRnS"

def scrape_twitter(twitter_handle, num_posts):
    payload = {
        "addUserInfo": True,
        "handles": [twitter_handle],
        "proxyConfig": {
            "useApifyProxy": True
        },
        "tweetsDesired": num_posts
    }

    response = requests.post(TWITTER_API_URL, json=payload)

    if response.status_code == 201:
        result = response.json()
        task_id = result['data']['id']
        print(f"Twitter scraping started successfully. Task ID: {task_id}")
        
        TASK_STATUS_URL = f"https://api.apify.com/v2/actor-runs/{task_id}?token={API_KEY}"
        while True:
            status_response = requests.get(TASK_STATUS_URL)
            status_data = status_response.json()
            status = status_data['data']['status']
            
            if status == "SUCCEEDED":
                print("Twitter scraping completed successfully.")
                
                dataset_id = status_data['data']['defaultDatasetId']
                DATASET_URL = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={API_KEY}&format=json"
                data_response = requests.get(DATASET_URL)
                
                if data_response.status_code == 200:
                    scraped_data = data_response.json()
                    return scraped_data
                else:
                    print(f"Error fetching scraped data: {data_response.status_code}")
                    return None
            
            elif status == "FAILED":
                print("Twitter scraping failed.")
                return None
            
            else:
                print("Twitter scraping still in progress...")
                time.sleep(10)
    else:
        print(f"Error starting the Twitter scraping task: {response.status_code}, {response.text}")
        return None

def scrape_instagram(instagram_handle, num_posts):
    payload = {
        "usernames": [instagram_handle]
    }

    response = requests.post(INSTAGRAM_API_URL, json=payload)

    if response.status_code == 201:
        result = response.json()
        task_id = result['data']['id']
        print(f"Instagram scraping started successfully. Task ID: {task_id}")
        
        TASK_STATUS_URL = f"https://api.apify.com/v2/actor-runs/{task_id}?token={API_KEY}"
        while True:
            status_response = requests.get(TASK_STATUS_URL)
            status_data = status_response.json()
            status = status_data['data']['status']
            
            if status == "SUCCEEDED":
                print("Instagram scraping completed successfully.")
                
                dataset_id = status_data['data']['defaultDatasetId']
                DATASET_URL = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={API_KEY}&format=json"
                data_response = requests.get(DATASET_URL)
                
                if data_response.status_code == 200:
                    scraped_data = data_response.json()[:num_posts]
                    return scraped_data
                else:
                    print(f"Error fetching scraped data: {data_response.status_code}")
                    return None
            
            elif status == "FAILED":
                print("Instagram scraping failed.")
                return None
            
            else:
                print("Instagram scraping still in progress...")
                time.sleep(10)
    else:
        print(f"Error starting the Instagram scraping task: {response.status_code}, {response.text}")
        return None
import pandas as pd
import random
from faker import Faker
from textblob import TextBlob

# Initialize Faker and set seed for reproducibility
fake = Faker()
random.seed(42)

# Define a list of users (simulating more realistic behavior)
users = [fake.user_name() for _ in range(50)]  # 50 users

# Generate hashtags related to various topics, some harmful
hashtags = ['#news', '#fun', '#education', '#harmfulcontent', '#alert', '#tech', '#politics', '#scam']

# Sentiment categories: Positive, Neutral, Negative
sentiment_categories = ['Positive', 'Neutral', 'Negative']

# Harmful content keywords for simulation
harmful_keywords = ['fake', 'hoax', 'scam', 'seditious', 'rumor', 'harmful']

# Function to simulate content and analyze sentiment
def generate_content():
    if random.random() < 0.3:  # 30% chance of harmful content
        content = f"This is a {random.choice(harmful_keywords)} content!"
        sentiment = 'Negative'
    else:
        content = fake.sentence()
        analysis = TextBlob(content).sentiment.polarity
        if analysis > 0:
            sentiment = 'Positive'
        elif analysis == 0:
            sentiment = 'Neutral'
        else:
            sentiment = 'Negative'
    return content, sentiment

# Generate data for social media posts
data = []
for i in range(100):  # Generate 100 posts
    post_id = i + 1
    username = random.choice(users)
    content, sentiment = generate_content()
    timestamp = fake.date_time_this_year()
    retweets = random.randint(0, 100) if sentiment == 'Negative' else random.randint(0, 50)  # More retweets for harmful content
    mentions = random.choice(users) if random.random() > 0.5 else None
    tags = random.choice(hashtags)
    is_harmful = any(keyword in content for keyword in harmful_keywords)
    
    data.append({
        'Post ID': post_id,
        'Username': username,
        'Content': content,
        'Timestamp': timestamp,
        'Retweets': retweets,
        'Mentions': mentions,
        'Tags': tags,
        'Is Harmful': is_harmful,
        'Sentiment': sentiment
    })

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV for analysis later
df.to_csv('realistic_social_media_posts.csv', index=False)

# Display sample data
print(df.head())
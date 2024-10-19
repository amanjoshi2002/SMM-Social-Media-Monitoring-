# twilio_sender.py

import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def send_sms_alert(platform, url):
    try:
        # Twilio API endpoint
        twilio_url = f"https://api.twilio.com/2010-04-01/Accounts/{os.getenv('TWILIO_ACCOUNT_SID')}/Messages.json"

        # Data to be sent in the POST request
        data = {
            "To": os.getenv('ALERT_TO_NUMBER'),
            "From": os.getenv('TWILIO_FROM_NUMBER'),
            "Body": f"High Alert Notification from {platform}: Please check the following link immediately: {url}",
        }

        # Twilio Account SID and Auth Token
        auth = HTTPBasicAuth(
            os.getenv('TWILIO_ACCOUNT_SID'),
            os.getenv('TWILIO_AUTH_TOKEN')
        )

        # Send the POST request
        response = requests.post(twilio_url, data=data, auth=auth)

        # Print the status code and response
        print(f"SMS Status Code: {response.status_code}")
        print(f"SMS Response: {response.json()}")
    except Exception as e:
        print(f"Error sending SMS alert: {str(e)}")
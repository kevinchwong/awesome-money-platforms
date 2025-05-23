import os
import json
import logging
import random
import sys
import firebase_admin
from firebase_admin import credentials, firestore
import anthropic
from datetime import datetime
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Check environment variables
required_env_vars = ["ANTHROPIC_API_KEY", "GCP_SA_KEY", "FIREBASE_PROJECT_ID", "FIREBASE_PLATFORM_COLLECTION"]
for var in required_env_vars:
    if not os.environ.get(var):
        logging.error(f"{var} environment variable is not set.")
        sys.exit(1)

# Initialize Anthropic client
api_key = os.environ.get("ANTHROPIC_API_KEY")
client = anthropic.Anthropic(api_key=api_key)

def get_platforms(as_of_date=None,  aims = "popular"):
    """Get the latest platform data from Claude"""
    
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    total_expected_platforms = 1000
    random_rank_start = random.randint(1, total_expected_platforms)
    batch_size = 18
    random_rank_end = random_rank_start + batch_size

    if aims == "latest":
        # Craft prompt for Claude
        prompt = f"""
            Give me a raw JSON array of 18 latest free platforms for making money online as of {as_of_date}
            Big randomness is allowed, so don't be afraid to include some less popular ones
            The platforms should be free,popular and useful, and the data should be accurate and up-to-date.
            Output is pure raw JSON like this:
        {{
            "results": [
                {{
                    "category": "<Category Name> (string)",
                    "cleaned_domain": "<Cleaned domain of the platform as unique identifier, without http:// or https://> (string)",
                    "name": "<Platform Name> (string)",
                    "description": "<Brief description of the platform> (string)",
                    "free_tier_details": "<Details about the free tier> (string)",
                    "url": "<The main URL of the platform> (string)",
                    "pricing_url": "<URL for pricing information> (string)",
                    "quick_start_url": "<URL for getting started quickly> (string)",
                    "key_features": "<Key features of the platform> (list of strings)",
                    "monetization_options": "<How to make money using the platform> (string)",
                    "beginner_friendly": "<A rating from 1 to 5 indicating how beginner-friendly the platform is> (int)",
                    "usefulness": "<A rating from 1 to 5 indicating the usefulness of the platform> (int)",
                    "importance": "<A rating from 1 to 5 indicating the importance of the platform> (int)",
                    "pros": "<A list of pros> (list of strings)",
                    "cons": "<A list of cons> (list of strings)",
                    "crawled_at": "{as_of_date}"
                }},
                ...
            ]
        }}
    
        Ensure the JSON is valid and concise, fact-checked and the data is accurate and up-to-date.
        """
    elif aims == "popular":
        prompt = f"""
            Give me a raw JSON array of top batch_size popular free platforms for making money online as of {as_of_date} in random category
            Just give me the platforms in rank {random_rank_start} to {random_rank_end}
            The platforms should be popular and useful, and the data should be accurate and up-to-date.
            Output is pure raw JSON like this:
        {{
            "results": [
                {{
                    "category": "<Category Name> (string)",
                    "cleaned_domain": "<Cleaned domain of the platform as unique identifier, without http:// or https://> (string)",
                    "name": "<Platform Name> (string)",
                    "description": "<Brief description of the platform> (string)",
                    "free_tier_details": "<Details about the free tier> (string)",
                    "url": "<The main URL of the platform> (string)",
                    "pricing_url": "<URL for pricing information> (string)",
                    "quick_start_url": "<URL for getting started quickly> (string)",
                    "key_features": "<Key features of the platform> (list of strings)",
                    "monetization_options": "<How to make money using the platform> (string)",
                    "beginner_friendly": "<A rating from 1 to 5 indicating how beginner-friendly the platform is> (int)",
                    "usefulness": "<A rating from 1 to 5 indicating the usefulness of the platform> (int)",
                    "importance": "<A rating from 1 to 5 indicating the importance of the platform> (int)",
                    "pros": "<A list of pros> (list of strings)",
                    "cons": "<A list of cons> (list of strings)",
                    "crawled_at": "{as_of_date}"
                }},
                ...
            ]
        }}
        """
    try:
        # Make API call to Claude
        response = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=8000,  # Adjusted to ensure response fits within token limit
            temperature=0.2,
            system="You are a helpful assistant that provides accurate, up-to-date JSON data about online platforms for making money.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Log the raw response for debugging
        logging.info(f"Raw response content: {response.content[0].text}")
        
        # Check if the response is empty
        if not response.content[0].text.strip():
            logging.error("Received empty response from Claude.")
            return []
        
        # Attempt to parse the JSON content
        try:
            platform_data = json.loads(response.content[0].text)
            logging.info("Successfully retrieved platform data from Claude.")
            return platform_data
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding error: {e}")
            return []
    except Exception as e:
        logging.error(f"Error retrieving platform data: {e}")
        return []

def initialize_firebase():
    try:
        cred = credentials.Certificate(json.loads(os.environ['GCP_SA_KEY']))
        firebase_admin.initialize_app(cred)
        logging.info("Firebase initialized successfully.")
        return firestore.client()
    except Exception as e:
        logging.error(f"Error initializing Firebase: {e}")
        sys.exit(1)

# Update platforms in Firebase

def update_platforms(db):
    for aims in ["latest", "popular"]:
        platforms = get_platforms(aims=aims)
        if not platforms:
            logging.error(f"No {aims} platform data to update.")
            return

        collection_ref = db.collection(os.environ['FIREBASE_PLATFORM_COLLECTION'])

        for platform in platforms['results']:
            try:
                # Use positional arguments for the query
                query = collection_ref.where('name_lower', '==', platform['name'].lower().replace(" ", "")).stream()
                if not any(query):
                    platform["name_lower"] = platform["name"].lower().replace(" ", "")
                    collection_ref.add(platform)
                    logging.info(f"Added platform: {aims} {platform['name']}")
                else:
                    logging.info(f"Platform already exists: {aims} {platform['name']}")
            except Exception as e:
                logging.error(f"Error updating platform {aims} {platform['name']}: {e}")

# Main function

def main():
    db = initialize_firebase()
    update_platforms(db)

if __name__ == "__main__":
    main() 
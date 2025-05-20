import os
import json
import logging
import sys
import firebase_admin
from firebase_admin import credentials, firestore
import anthropic
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Check environment variables
required_env_vars = ["ANTHROPIC_API_KEY", "GCP_SA_KEY", "FIREBASE_PROJECT_ID", "FIREBASE_COLLECTION"]
for var in required_env_vars:
    if not os.environ.get(var):
        logging.error(f"{var} environment variable is not set.")
        sys.exit(1)

# Initialize Anthropic client
api_key = os.environ.get("ANTHROPIC_API_KEY")
client = anthropic.Anthropic(api_key=api_key)

def get_latest_platforms(as_of_date=None):
    """Get the latest platform data from Claude"""
    
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    # Craft prompt for Claude
    prompt = f"""
    Give me a raw JSON array of 15+ recent popular free platforms for making money online as of {as_of_date}
    Randomness is allowed, so don't be afraid to include some less popular ones
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
    platforms = get_latest_platforms()
    if not platforms:
        logging.error("No platform data to update.")
        return

    collection_ref = db.collection(os.environ['FIREBASE_COLLECTION'])

    for platform in platforms['results']:
        try:
            # Check for duplicates
            query = collection_ref.where('name_lower', '==', platform['name'].lower().replace(" ", "")).stream()
            if not any(query):
                platform["name_lower"] = platform["name"].lower().replace(" ", "")
                collection_ref.add(platform)
                logging.info(f"Added platform: {platform['name']}")
            else:
                logging.info(f"Platform already exists: {platform['name']}")
        except Exception as e:
            logging.error(f"Error updating platform {platform['name']}: {e}")

# Main function

def main():
    db = initialize_firebase()
    update_platforms(db)

if __name__ == "__main__":
    main() 
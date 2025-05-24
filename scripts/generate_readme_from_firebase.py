#!/usr/bin/env python3
import os
import sys
import json
import logging
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from slugify import slugify
import requests
from urllib.parse import urlparse
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_environment_vars():
    """Validate that all required environment variables are set."""
    required_vars = ['GCP_SA_KEY', 'FIREBASE_PROJECT_ID', 'FIREBASE_PLATFORM_COLLECTION']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

def initialize_firebase():
    """Initialize Firebase with proper error handling."""
    try:
        # Validate required environment variables
        validate_environment_vars()
        
        # Retrieve and parse the GCP service account key
        gcp_sa_key = os.environ.get('GCP_SA_KEY')
        service_account_info = json.loads(gcp_sa_key)
        
        # Initialize Firebase
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logger.error(f"Error initializing Firebase: {str(e)}")
        sys.exit(1)

def get_platforms_from_firebase(db):
    """Retrieve all platforms from Firebase and organize them by category."""
    try:
        collection_ref = db.collection(os.environ['FIREBASE_PLATFORM_COLLECTION'])
        platforms = collection_ref.stream()
        
        # Organize platforms by category
        categorized_platforms = {}
        for platform in platforms:
            data = platform.to_dict()
            category = data.get('category', 'Uncategorized')
            if category not in categorized_platforms:
                categorized_platforms[category] = []
            categorized_platforms[category].append(data)
        
        return categorized_platforms
    except Exception as e:
        logger.error(f"Error retrieving platforms from Firebase: {str(e)}")
        sys.exit(1)

def check_url_health(url, timeout=30, max_redirects=5):
    """
    Check if a URL is accessible with robust redirect handling and error checking.
    Returns True if accessible, False otherwise.
    
    Args:
        url (str): The URL to check
        timeout (int): Request timeout in seconds
        max_redirects (int): Maximum number of redirects to follow
    """
    try:
        # Add scheme if missing
        if not urlparse(url).scheme:
            url = 'https://' + url

        # Configure session with custom settings
        session = requests.Session()
        session.max_redirects = max_redirects
        
        # Set headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        # Try different HTTP methods in order of preference
        methods = ['HEAD', 'GET']
        last_error = None
        
        for method in methods:
            try:
                response = session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=True  # Verify SSL certificates
                )
                
                # Check if we got a successful response
                if response.status_code < 500:
                    return True
                                        
                # For other status codes, log the issue
                logger.warning(f"URL {url} returned status code {response.status_code}")
                return False
                
            except requests.exceptions.TooManyRedirects:
                logger.warning(f"Too many redirects for {url}")
                return False
            except requests.exceptions.SSLError as e:
                logger.warning(f"SSL Error for {url}: {str(e)}")
                return False
            except requests.exceptions.ConnectionError as e:
                last_error = e
                continue
            except requests.exceptions.Timeout as e:
                last_error = e
                continue
            except requests.exceptions.RequestException as e:
                last_error = e
                continue
        
        # If we get here, all methods failed
        if last_error:
            logger.warning(f"All methods failed for {url} - Last error: {str(last_error)}")
        return False
        
    except Exception as e:
        logger.warning(f"Unexpected error checking URL {url}: {str(e)}")
        return False

def remove_invalid_platforms(db, platforms_by_category):
    """
    Check URLs and remove platforms with invalid URLs from Firebase.
    Returns updated platforms_by_category dictionary.
    """
    collection_ref = db.collection(os.environ['FIREBASE_PLATFORM_COLLECTION'])
    updated_platforms = {}
    removed_count = 0

    for category, platforms in platforms_by_category.items():
        valid_platforms = []
        for platform in platforms:
            url = platform.get('url')
            if not url:
                logger.warning(f"Platform {platform.get('name')} has no URL, skipping...")
                continue

            if check_url_health(url):
                valid_platforms.append(platform)
            else:
                # Remove from Firebase
                try:
                    platform_id = platform.get('id')
                    if platform_id:
                        collection_ref.document(platform_id).delete()
                        removed_count += 1
                        logger.info(f"Removed invalid platform: {platform.get('name')} ({url})")
                except Exception as e:
                    logger.error(f"Error removing platform {platform.get('name')}: {str(e)}")

        if valid_platforms:
            updated_platforms[category] = valid_platforms

    if removed_count > 0:
        logger.info(f"Removed {removed_count} platforms with invalid URLs")
    
    return updated_platforms

def generate_readme_content(platforms_by_category):
    """Generate the README.md content from the platform data."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Start with the header
    content = [
        "# Awesome Free Platforms for Money Making",
        "",
        "[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)",
        "",
        f"Last updated: {current_time}",
        "",
        "[![Update README](https://github.com/kevinchwong/awesome-money-platforms/actions/workflows/generate-readme-from-firebase.yml/badge.svg)](https://github.com/kevinchwong/awesome-money-platforms/actions/workflows/generate-readme-from-firebase.yml)",
        "",
        "A curated list of free platforms for making money online.",
        "",
        "## Table of Contents",
        ""
    ]
    
    # Add table of contents
    for category in sorted(platforms_by_category.keys()):
        anchor = slugify(category.replace('&', '_amp_'))
        anchor = anchor.replace('-amp-', '--')
        content.append(f"- [{category}](#{anchor})")
    
    content.append("")
    
    # Add each category section
    for category, platforms in sorted(platforms_by_category.items()):
        content.extend([
            f"## {category}",
            "",
            "| Platform | Description | Free Tier | Key Features | Pros | Cons | Usefulness | Importance | Beginner Rating | Monetization |",
            "|----------|-------------|-----------|--------------|------|------|------------|------------|-----------------|--------------|"
        ])
        
        for platform in platforms:
            # Format key features as a bullet list
            key_features = platform.get('key_features', [])
            if isinstance(key_features, list):
                key_features = "<br>• ".join([""] + key_features)
            else:
                key_features = str(key_features)
            
            # Format pros as a bullet list
            pros = platform.get('pros', [])
            if isinstance(pros, list):
                pros = "<br>• ".join([""] + pros)
            else:
                pros = str(pros)
            
            # Format cons as a bullet list
            cons = platform.get('cons', [])
            if isinstance(cons, list):
                cons = "<br>• ".join([""] + cons)
            else:
                cons = str(cons)
            
            # Create the table row
            platform_name = platform['name']
            platform_url = platform['url'].replace('&', '&amp;')  # Encode ampersand in URL
            
            row = [
                f"[{platform_name}]({platform_url})",
                platform.get('description', '') + " " + "/".join(
                    [x for x in [
                        f"[(pricing)]({platform['pricing_url']})" if platform['pricing_url'] else '',
                        f"[(quick start)]({platform['quick_start_url']})" if platform['quick_start_url'] else ''
                    ] if x]
                ),
                platform.get('free_tier_details', ''),
                key_features,
                pros,
                cons,
                (str(platform.get('usefulness'))+"/5") if platform.get('usefulness') else 'N/A',
                (str(platform.get('importance'))+"/5") if platform.get('importance') else 'N/A',
                (str(platform.get('beginner_friendly'))+"/5") if platform.get('beginner_friendly') else 'N/A',
                platform.get('monetization_options', ''),
            ]
            
            # Escape pipe characters in the content
            row = [str(cell).replace('|', '\\|') for cell in row]
            content.append("| " + " | ".join(row) + " |")
        
        content.append("")
    
    return "\n".join(content)

def update_readme(content):
    """Update the README.md file."""
    try:
        with open("README.md", "w") as f:
            f.write(content)
        logger.info("README.md has been updated successfully!")
    except Exception as e:
        logger.error(f"Error updating README.md: {str(e)}")
        sys.exit(1)

def main():
    """Main function to generate README from Firebase data."""
    db = initialize_firebase()
    platforms_by_category = get_platforms_from_firebase(db)
    
    # Check URLs and remove invalid platforms
    # platforms_by_category = remove_invalid_platforms(db, platforms_by_category)
    
    readme_content = generate_readme_content(platforms_by_category)
    update_readme(readme_content)

if __name__ == "__main__":
    main() 
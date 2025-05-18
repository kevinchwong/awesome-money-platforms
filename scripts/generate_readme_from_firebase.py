#!/usr/bin/env python3
import os
import sys
import json
import logging
from datetime import datetime

import pytz
import firebase_admin
from firebase_admin import credentials, firestore
from slugify import slugify
import requests
from urllib.parse import urlparse
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
REQUIRED_ENV_VARS = ['GCP_SA_KEY', 'FIREBASE_PROJECT_ID', 'FIREBASE_PLATFORM_COLLECTION']
DEFAULT_TIMEOUT = 30
MAX_REDIRECTS = 5
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

def validate_environment_vars():
    """Validate that all required environment variables are set."""
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

def initialize_firebase():
    """Initialize Firebase with proper error handling."""
    try:
        validate_environment_vars()
        service_account_info = json.loads(os.environ['GCP_SA_KEY'])
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logger.error(f"Error initializing Firebase: {str(e)}")
        raise

def get_platforms_from_firebase(db):
    """Retrieve all platforms from Firebase and organize them by category."""
    try:
        collection_ref = db.collection(os.environ['FIREBASE_PLATFORM_COLLECTION'])
        platforms = collection_ref.stream()
        
        categorized_platforms = {}
        for platform in platforms:
            data = platform.to_dict()
            category = data.get('category', 'Uncategorized')
            categorized_platforms.setdefault(category, []).append(data)
        
        return categorized_platforms
    except Exception as e:
        logger.error(f"Error retrieving platforms from Firebase: {str(e)}")
        raise

def check_url_health(url, timeout=DEFAULT_TIMEOUT, max_redirects=MAX_REDIRECTS):
    """
    Check if a URL is accessible with robust redirect handling and error checking.
    Returns True if accessible, False otherwise.
    """
    try:
        if not urlparse(url).scheme:
            url = 'https://' + url

        session = requests.Session()
        session.max_redirects = max_redirects
        
        headers = {
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        for method in ['HEAD', 'GET']:
            try:
                response = session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=True
                )
                
                if response.status_code < 500:
                    return True
                    
                if response.status_code == 405:
                    continue
                    
                logger.warning(f"URL {url} returned status code {response.status_code}")
                return False
                
            except (requests.exceptions.TooManyRedirects, requests.exceptions.SSLError) as e:
                logger.warning(f"Error for {url}: {str(e)}")
                return False
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                   requests.exceptions.RequestException) as e:
                continue
        
        logger.warning(f"All methods failed for {url}")
        return False
        
    except Exception as e:
        logger.warning(f"Unexpected error checking URL {url}: {str(e)}")
        return False

def remove_invalid_platforms(db, platforms_by_category):
    """Check URLs and remove platforms with invalid URLs from Firebase."""
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

def format_list_items(items):
    """Format a list of items as HTML bullet points."""
    if isinstance(items, list):
        return "<br>• ".join([""] + items)
    return str(items)

    

def generate_readme_content(platforms_by_category):
    """Generate the README.md content from the platform data."""
    current_est_time = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")

    # Calculate item counts for each category
    category_counts = {category: len(platforms) for category, platforms in platforms_by_category.items()}

    # Calculate the total number of platforms
    total_platforms = sum(category_counts.values())
    total_platforms_rounded_to_nearest_ceiling_hundred = round(total_platforms / 100) * 100

    # Update badge with the real count
    badges = f"""
[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
[![Platforms](https://img.shields.io/badge/platforms-{total_platforms_rounded_to_nearest_ceiling_hundred}+-brightgreen)](https://github.com/kevinchwong/awesome-money-platforms)
[![Last Updated](https://img.shields.io/badge/updated-daily-blue)](https://github.com/kevinchwong/awesome-money-platforms)
[![Update README](https://github.com/kevinchwong/awesome-money-platforms/actions/workflows/generate-readme-from-firebase.yml/badge.svg)](https://github.com/kevinchwong/awesome-money-platforms/actions/workflows/generate-readme-from-firebase.yml)
    """
    
    # Update description with the real count
    description = f"""
🚀 **The Ultimate Collection of {total_platforms} Free Money-Making Platforms**

Discover legitimate ways to earn money online through freelancing, content creation, 
AI services, e-commerce, and more. Updated daily with new opportunities!
    """


    content = [
        "# Awesome Free Platforms for Money Making",
        "",
        f"{badges}",
        "",
        f"Last updated: {current_est_time} EST",
        "",
        f"{description}",
        "",
        "## Table of Contents",
        ""
    ]
    
    # Sort categories by item count
    sorted_categories = sorted(category_counts.items(), key=lambda x: (x[1], x[0]), reverse=True)

    # Add table of contents with item counts
    for category, item_count in sorted_categories:
        anchor = slugify(category.replace('&', '_amp_')).replace('-amp-', '--')
        content.append(f"- [{category} ({item_count})](#{anchor})")
    
    content.append("")
    
    # Add each category section
    for category, item_count in sorted(sorted_categories):
        platforms = platforms_by_category[category]
        content.extend([
            f"## {category}",
            "",
            "| Platform | Description | Free Tier | Key Features | Pros | Cons | Usefulness | Importance | Beginner Rating | Monetization |",
            "|----------|-------------|-----------|--------------|------|------|------------|------------|-----------------|--------------|"
        ])

        # Sort platforms by metrics
        platforms = sorted(
            platforms,
            key=lambda x: (
                x.get('usefulness', 0),
                x.get('importance', 0),
                x.get('beginner_friendly', 0)
            ),
            reverse=True
        )
        
        for platform in platforms:
            # Format lists
            key_features = format_list_items(platform.get('key_features', []))
            pros = format_list_items(platform.get('pros', []))
            cons = format_list_items(platform.get('cons', []))
            
            # Format rating
            def format_rating(rating):
                return f"{rating}/5" if rating else 'N/A'
            
            # Create the table row
            platform_name = platform['name']
            platform_url = platform['url'].replace('&', '&amp;')
            
            # Format additional URLs
            additional_urls = []
            if platform.get('pricing_url'):
                additional_urls.append(f"[(pricing)]({platform['pricing_url']})")
            if platform.get('quick_start_url'):
                additional_urls.append(f"[(quick start)]({platform['quick_start_url']})")
            
            row = [
                f"[{platform_name}]({platform_url})",
                platform.get('description', '') + " " + "/".join(additional_urls),
                platform.get('free_tier_details', ''),
                key_features,
                pros,
                cons,
                format_rating(platform.get('usefulness')),
                format_rating(platform.get('importance')),
                format_rating(platform.get('beginner_friendly')),
                platform.get('monetization_options', ''),
            ]
            
            # Escape pipe characters and join
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
        raise

def main():
    """Main function to generate README from Firebase data."""
    try:
        db = initialize_firebase()
        platforms_by_category = get_platforms_from_firebase(db)
        
        # Uncomment to enable URL health checking
        # platforms_by_category = remove_invalid_platforms(db, platforms_by_category)
        
        readme_content = generate_readme_content(platforms_by_category)
        update_readme(readme_content)
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
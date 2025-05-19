#!/usr/bin/env python3
import os
import anthropic
from datetime import datetime

# Initialize Anthropic client
api_key = os.environ.get("ANTHROPIC_API_KEY")
client = anthropic.Anthropic(api_key=api_key)

def get_latest_platforms():
    """Get the latest platform data from Claude"""
    
    # Craft prompt for Claude
    prompt = f"""
    Create a comprehensive, fact-checked list of free platforms for making money online in {datetime.now().year}.
    

    
    At the first few lines
    - # Awesome Free Platforms for Money Making
    - # [![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
    - Show the latest update date timestamp {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    - And show the github action workflow badge with link to https://github.com/kevinchwong/awesome-money-platforms/actions/workflows/update-readme.yml
    
    The list should be organized into these categories (or more if needed):
    - No-Code & Low-Code Platforms
    - AI Application Platforms
    - Web Hosting & Deployment
    - Content Creation & Publishing
    - Online Education & Courses
    - Digital Product Sales
    - Freelancing & Services
    - Community Building & Memberships
    - E-Commerce & Marketplace
    - Social Media Monetization
    - Specialized AI Services
    - Automation & Productivity
    - Stock Media & Creative Assets
    - Mobile App Monetization
    - Affiliate Marketing
    
    For each platform, include:
    - Name with URL link
    - One line description the usage of the platform
    - Free tier details
    - Key features
    - Monetization options
    - Beginner-friendliness rating (1-5)
    
    Format the response as a complete GitHub README.md with proper markdown formatting, including:
    - Title: "# Awesome Free Platforms for Money Making"
    - Table of contents with links to each section
    - Each category as a section with a table of platforms
    - 8+ rows of platforms for each category
    - Proper markdown tables with headers for Platform, Description, Free Tier, Key Features, Monetization Options, and Beginner Rating
    
    Verify all information is accurate and up-to-date based on your knowledge. Include only platforms that have legitimate free tiers (not just free trials).
    
    Make sure the content is pure markdown and not markdown with code blocks.
    """
    
    # Make API call to Claude
    response = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=8000,
        temperature=0.2,
        system="You are a helpful assistant that creates accurate, well-formatted GitHub README files about online platforms for making money. You focus on fact-checking and providing up-to-date information.",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    # Extract the markdown content
    markdown_content = response.content[0].text
    
    return markdown_content

def update_readme(content):
    """Update the README.md file"""
    with open("README.md", "w") as f:
        f.write(content)
    print("README.md has been updated successfully!")

if __name__ == "__main__":
    content = get_latest_platforms()
    update_readme(content)
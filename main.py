#!/usr/bin/env python3
"""
GitHub User Email Scraper for Texas Users

This script scrapes GitHub users in Texas and extracts their email addresses:
1. Search users by location (Texas/TX) and creation date
2. For each user, try to get public email
3. If no public email, search commit metadata from their repos
4. Skip noreply emails and validate addresses
5. Output to CSV for Google Sheets
"""

from dotenv import load_dotenv
import os
import re
import csv
import time
import json
import base64
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import requests
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

# Google Sheets config
GOOGLE_SHEET_NAME = "GitHub Leads"
LAST_REQUEST_TIME = 0
REQUEST_INTERVAL = 0.2  # 5 requests/sec safe

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

creds_dict = json.loads(os.getenv("GOOGLE_CREDS_JSON"))

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=SCOPES
)

gc = gspread.authorize(creds)
sheet = gc.open(GOOGLE_SHEET_NAME).sheet1

# GitHub API configuration
GITHUB_TOKEN = os.getenv("TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN environment variable is required")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "GitHub-Email-Scraper"
}

API_BASE = "https://api.github.com"

# Output file
OUTPUT_FILE = "texas_github_users.csv"

# Configuration - can use TX or Texas as location
LOCATIONS = ["TX", "Texas"]

# Email regex pattern for validation
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

# noreply email pattern to skip
NOREPLY_PATTERN = re.compile(r'noreply@github\.com', re.IGNORECASE)

def github_request(url, params=None, retries=3):
    global LAST_REQUEST_TIME

    # throttle
    now = time.time()
    elapsed = now - LAST_REQUEST_TIME
    if elapsed < REQUEST_INTERVAL:
        time.sleep(REQUEST_INTERVAL - elapsed)

    LAST_REQUEST_TIME = time.time()

    for attempt in range(retries):
        response = requests.get(url, params=params)

        remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
        reset_time = int(response.headers.get("X-RateLimit-Reset", 0))

        if remaining <= 0:
            sleep_time = max(reset_time - int(time.time()), 0) + 5
            print(f"⚠️ Low quota ({remaining}). Sleeping {sleep_time}s...")
            time.sleep(sleep_time)

        if response.status_code == 200:
            return response

        if response.status_code == 403 and remaining == 0:
            sleep_time = max(reset_time - int(time.time()), 0) + 5
            print(f"⛔ Rate limit hit. Sleeping {sleep_time}s...")
            time.sleep(sleep_time)

        time.sleep(2 ** attempt)

    return None

def init_google_sheet():
    headers = ["name", "email", "github_url"]

    existing = sheet.get_all_values()
    if len(existing) == 0:
        sheet.append_row(headers)


def search_users_by_location_and_created(location: str, created_filter: str):
    users = []
    page = 1

    while True:
        query = f"location:{location} created:{created_filter}"

        url = f"{API_BASE}/search/users"
        params = {
            "q": query,
            "page": page,
            "per_page": 100,
            "sort": "created",
            "order": "asc"
        }

        response = github_request(url, params=params)
        if not response:
            return []
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])

        if not items:
            break

        users.extend(items)

        # STOP if last page
        if len(items) < 100:
            break

        page += 1
        time.sleep(1)  # avoid rate limit

    return users


def get_user_details(username: str) -> Optional[Dict]:
    """Get detailed user information."""
    url = f"{API_BASE}/users/{username}"
    response = github_request(url)
    if not response:
        return []
    
    if response.status_code == 200:
        return response.json()
    return None


def get_user_public_email(username: str) -> Optional[str]:
    """Get user's public email address."""
    user = get_user_details(username)
    if user and user.get("email"):
        email = user["email"]
        if is_valid_email(email) and not is_noreply_email(email):
            return email
    return None


def get_user_repositories(username: str, max_repos: int = 100) -> List[Dict]:
    """Get user's public repositories sorted by latest update first."""

    repos = []
    page = 1

    while len(repos) < max_repos:
        url = f"{API_BASE}/users/{username}/repos"
        params = {
            "type": "owner",
            "sort": "created",
            "direction": "desc",
            "per_page": 100,
            "page": page
        }

        response = github_request(url, params=params)
        if not response:
            return []
        if response.status_code != 200:
            break

        batch = response.json()
        if not batch:
            break

        repos.extend(batch)

        if len(batch) < 100:
            break

        page += 1

    # filter AFTER full collection
    repos = [
        r for r in repos
        if not r.get("fork", False)
        and r.get("size", 0) > 0
        and not r.get("archived", False)
    ]

    # already sorted by GitHub (latest updated first)
    return repos[:max_repos]

def get_commit_emails_from_repo(owner: str, repo: str, max_commits: int = 30) -> List[str]:
    """Get emails from commits belonging to repo owner (optimized)."""

    emails = set()

    url = f"{API_BASE}/repos/{owner}/{repo}/commits"
    params = {"per_page": min(100, max_commits)}

    try:
        response = github_request(url, params=params)
        if not response:
            return []

        commits = response.json()

        for commit in commits:
            # 🔹 Check GitHub-linked author (best signal)
            author_login = commit.get("author", {}).get("login")

            if author_login and author_login != owner:
                continue

            # 🔹 Extract email directly from commit payload (NO extra API call)
            commit_info = commit.get("commit", {})
            author_info = commit_info.get("author", {})

            email = author_info.get("email")

            if email and is_valid_email(email) and not is_noreply_email(email):
                emails.add(email)

        return list(emails)[:5]  # limit smaller for efficiency

    except Exception as e:
        print(f"  Error getting commits from {owner}/{repo}: {e}")
        return []


def is_valid_email(email: str) -> bool:
    """Validate email format."""
    if not email:
        return False
    return bool(EMAIL_PATTERN.match(email))


def is_noreply_email(email: str) -> bool:
    """Check if email is a noreply address."""
    if not email:
        return False
    return bool(NOREPLY_PATTERN.search(email))


def generate_date_ranges(start_date: str, end_date: str) -> List[str]:
    """Generate monthly date ranges for searching."""
    ranges = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    while current <= end:
        next_month = current + timedelta(days=32)
        next_month = next_month.replace(day=1)
        
        if next_month > end:
            next_month = end + timedelta(days=1)
        
        # Format: created:YYYY-MM-DD..YYYY-MM-DD
        range_str = f"{current.strftime('%Y-%m-%d')}..{(next_month - timedelta(days=1)).strftime('%Y-%m-%d')}"
        ranges.append(range_str)
        
        current = next_month
    
    return ranges


def scrape_user_email(username: str) -> Optional[str]:
    """Scrape email from a user's profile or repositories."""
    # First try to get public email
    public_email = get_user_public_email(username)
    if public_email:
        return public_email
    
    # If no public email, search through repositories
    repos = get_user_repositories(username, max_repos=20)
    
    for repo in repos:
        owner = repo.get("owner", {}).get("login")
        repo_name = repo.get("name")
        
        if not owner or not repo_name:
            continue
        
        emails = get_commit_emails_from_repo(owner, repo_name, max_commits=50)
        
        if emails:
            return emails[0]
    
    return None


def process_users_batch(users: List[Dict], processed_usernames: set) -> List[Dict]:
    """Process a batch of users and collect their information."""
    results = []
    
    for user in users:
        username = user.get("login")
        
        if username in processed_usernames:
            continue
        
        print(f"Processing user: {username}")
        
        try:
            # Get public email first
            email = get_user_public_email(username)
            
            # If no public email, try from commits
            if not email:
                email = scrape_user_email(username)
            
            if email:
                user_details = get_user_details(username)
                name = user_details.get("name", username) if user_details else username
                github_url = user_details.get("html_url", f"https://github.com/{username}") if user_details else f"https://github.com/{username}"
                
                lead = {
                    "name": name,
                    "email": email,
                    "github_url": github_url
                }

                append_to_sheet(lead)
                print(f"  Saved to sheet: {email}")
                print(f"  Found email: {email}")
            
            processed_usernames.add(username)
            
            # Rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  Error processing {username}: {e}")
            processed_usernames.add(username)
            continue
    
    return results


def save_to_csv(data: List[Dict], filename: str):
    """Save data to CSV file."""
    if not data:
        print("No data to save")
        return
    
    fieldnames = ["name", "email", "github_url"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Saved {len(data)} records to {filename}")


def append_to_csv(data: List[Dict], filename: str):
    """Append data to CSV file."""
    if not data:
        return
    
    fieldnames = ["name", "email", "github_url"]
    file_exists = os.path.exists(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)


def get_total_user_count(location: str, created_date: str) -> int:
    """Get total count of users matching search criteria."""
    query = f"location:{location} created:{created_date}"
    url = f"{API_BASE}/search/users"
    params = {
        "q": query,
        "per_page": 1
    }
    
    response = github_request(url, params=params)
    if not response:
        return []
    response.raise_for_status()
    data = response.json()
    
    return data.get("total_count", 0)

def append_to_sheet(row: Dict):
    try:
        sheet.append_row([
            row["name"],
            row["email"],
            row["github_url"]
        ])
    except Exception as e:
        print(f"Google Sheets error: {e}")


def main():
    init_google_sheet()

    print("GitHub Email Scraper for Texas Users")
    print("=" * 50)

    end_date = "2022-12-31"
    start_date = "2008-01-01"

    print(f"Location: Texas and TX")
    print(f"Date range: {start_date} to {end_date}")
    print()

    processed_usernames = set()

    # Phase 1
    print("\n=== Phase 1: Users created before 2009-01-01 ===")
    for location in LOCATIONS:
        print(f"\n--- Searching {location} users created before 2009-01-01 ---")
        try:
            users = search_users_by_location_and_created(location, "<2009-01-01")
            print(f"  Found {len(users)} users")

            if users:
                process_users_batch(users, processed_usernames)

        except Exception as e:
            print(f"  Error: {e}")

    # Phase 2
    print("\n=== Phase 2: Users created by month (2009-01 to 2022-12) ===")
    current = datetime(2009, 1, 1)
    end = datetime(2022, 12, 31)

    month_count = 0
    total_months = (2022 - 2009) * 12 + 12

    while current <= end:

        next_month = current + timedelta(days=32)
        next_month = next_month.replace(day=1)

        if next_month > end:
            next_month = end + timedelta(days=1)

        month_str = current.strftime("%Y-%m")
        date_filter = f"{current.strftime('%Y-%m-%d')}..{(next_month - timedelta(days=1)).strftime('%Y-%m-%d')}"

        month_count += 1

        for location in LOCATIONS:
            try:
                print(f"\n  [{month_count}/{total_months}] {location} - {month_str}")

                users = search_users_by_location_and_created(location, date_filter)

                if users:
                    print(f"    Found {len(users)} users")

                    if len(users) >= 1000:
                        print(f"    WARNING: More than 1000 users, processing first 100")
                        users = users[:100]

                    process_users_batch(users, processed_usernames)

                else:
                    print(f"    No users found")

                time.sleep(0.5)

            except Exception as e:
                print(f"    Error: {e}")

        current = next_month

    print(f"\n=== Done ===")
    print(f"Total users processed: {len(processed_usernames)}")


if __name__ == "__main__":
    main()
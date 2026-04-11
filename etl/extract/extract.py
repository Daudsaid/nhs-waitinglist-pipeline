import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()

# 5 years of NHS England A&E data
NHS_AE_URLS = [
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2025-26/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2024-25/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2023-24/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2022-23/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2021-22/",
]

DOWNLOAD_DIR = "data/raw"


def get_csv_links(url: str) -> list[str]:
    """Scrape NHS England page and return all CSV download links."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.endswith(".csv"):
            links.append(href)

    return links


def download_csv(url: str, dest_dir: str) -> str:
    """Download a single CSV file and save to dest_dir."""
    os.makedirs(dest_dir, exist_ok=True)
    filename = url.split("/")[-1]
    filepath = os.path.join(dest_dir, filename)

    # Skip if already downloaded
    if os.path.exists(filepath):
        print(f"Already exists, skipping: {filename}")
        return filepath

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(response.content)

    print(f"Downloaded: {filename}")
    return filepath


def run():
    total_files = 0

    for url in NHS_AE_URLS:
        print(f"\nFetching CSV links from: {url}")
        links = get_csv_links(url)

        if not links:
            print("No CSV links found.")
            continue

        print(f"Found {len(links)} CSV files.")
        for link in links:
            download_csv(link, DOWNLOAD_DIR)
            total_files += 1

    print(f"\nExtract complete. Total files processed: {total_files}")


if __name__ == "__main__":
    run()

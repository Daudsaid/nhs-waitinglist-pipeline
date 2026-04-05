import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()

NHS_AE_URLS = [
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2025-26/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2024-25/",
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2023-24/",
]


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

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(response.content)

    print(f"Downloaded: {filename}")
    return filepath


def run():
    print("Fetching CSV links from NHS England...")
    links = get_csv_links(NHS_AE_URL)

    if not links:
        print("No CSV links found.")
        return

    print(f"Found {len(links)} CSV files.")
    for link in links:
        download_csv(link, DOWNLOAD_DIR)

    print("Extract complete.")


if __name__ == "__main__":
    run()
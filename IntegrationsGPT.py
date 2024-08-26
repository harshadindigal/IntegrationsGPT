import re
from urllib.parse import urlparse
from typing import List, Dict, Any

# Assuming these imports are available
from googlesearch import GoogleSearch
from firecrawl import FirecrawlApp
from h2ogpte import H2OGPTE

class SnowflakeAPIDocCrawler:
    def __init__(self, google_api_key: str, h2ogpte_api_key: str, firecrawl_api_key: str):
        self.google_api_key = google_api_key
        self.h2ogpte_client = H2OGPTE(
            address='https://h2ogpte.dev.h2o.ai',
            api_key=h2ogpte_api_key
        )
        self.firecrawl_app = FirecrawlApp(api_key=firecrawl_api_key)

    def google_search(self, query: str, location: str) -> Dict[str, Any]:
        params = {
            "q": query,
            "location": location,
            "hl": "en",
            "gl": "us",
            "google_domain": "google.com",
            "api_key": self.google_api_key
        }
        search = GoogleSearch(params)
        return search.get_dict()

    def get_best_link(self, search_results: Dict[str, Any]) -> str:
        system_prompt = f"""You are an expert developer in python who specializes in writing integration code between open source softwares. Take a look at the following google search results and pick what you think is the best link for what you think would be the API documentation. Only output the URL. Take a look at the search results:

        {search_results}

        Analyze these results and select the best link. The goal is to pass the link into firecrawl and put all the embedded links into a collection, therefore the main page of the API documentation is the best one."""

        chat_session_id = self.h2ogpte_client.create_chat_session()

        with self.h2ogpte_client.connect(chat_session_id) as session:
            reply = session.query(
                "Make sure you only put a valid url no other text....What is the best link from the search results? (Only Output the URL)",
                system_prompt=system_prompt,
                timeout=60,
            )
            link = reply.content.strip()

        if (link.startswith('"') and link.endswith('"')) or (link.startswith("'") and link.endswith("'")):
            link = link[1:-1]
        return link

    def crawl_url(self, url: str) -> Dict[str, Any]:
        crawl_params = {
            'crawlerOptions': {
                'excludes': [],
                'includes': [],
                'limit': 20,
            }
        }
        return self.firecrawl_app.crawl_url(url, params=crawl_params)

    def get_api_doc_links(self, crawl_result: Dict[str, Any]) -> List[str]:
        system_prompt = f"""You are an expert developer in python who specializes in writing integration code between open source softwares. Take a look at the following google search results and pick what you think is the best link for what you think would be the API documentation. Only output the URL. Take a look at the search results:

        {crawl_result}

        Analyze these results and select the best link. The goal is to pass the link into firecrawl and put all the embedded links into a collection, therefore the main page of the API documentation is the best one."""

        chat_session_id = self.h2ogpte_client.create_chat_session()

        with self.h2ogpte_client.connect(chat_session_id) as session:
            reply = session.query(
                "What are all the links that contain anything to do with the snowflake API documentation from the search results? (Only Output a list of valid URLs with no whitespace or quotes)",
                system_prompt=system_prompt,
                timeout=60,
            )
            links = reply.content.strip()

        return links.split(",")

    @staticmethod
    def clean_and_validate_url(url: str) -> bool:
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        url = re.sub(r'[^a-zA-Z0-9-._~:/?#[\]@!$&\'()*+,;=]', '', url)
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def create_collection(self, name: str, description: str) -> str:
        return self.h2ogpte_client.create_collection(
            name=name,
            description=description,
        )

    def ingest_websites(self, collection_id: str, urls: List[str]):
        for url in urls:
            try:
                self.h2ogpte_client.ingest_website(
                    collection_id=collection_id,
                    url=url.strip().strip("'[]"),
                    gen_doc_summaries=True,
                    gen_doc_questions=True,
                    follow_links=True,
                    max_depth=1,
                    audio_input_language='auto',
                    ocr_model='auto',
                    tesseract_lang=None,
                    timeout=180
                )
                print(f"Successfully ingested website: {url}")
            except Exception as e:
                print(f"Failed to ingest website {url}: {str(e)}")
        print("Ingestion process completed.")

    def query_collection(self, collection_id: str, query: str) -> str:
        chat_session_id = self.h2ogpte_client.create_chat_session(collection_id)
        with self.h2ogpte_client.connect(chat_session_id) as session:
            reply = session.query(query, timeout=180)
        return reply.content

def main():
    # Initialize the crawler
    crawler = SnowflakeAPIDocCrawler(
        google_api_key='71a353fddaa12467f450c8cfd48f1383a61d01729f37d08904d1b737cd8525ef',
        h2ogpte_api_key='sk-REFgS5bl31lXiyaW8YKfCgNh0O8Or0BIm3AtlRf9gPi1UFSR',
        firecrawl_api_key='fc-5ae75e85ab6643cfbb4f4fbc15c41340'
    )

    # Perform Google search
    search_results = crawler.google_search("Snowflake API documentation", "Austin, Texas, United States")

    # Get the best link
    best_link = crawler.get_best_link(search_results)
    print(f"Best link: {best_link}")

    # Crawl the best link
    crawl_result = crawler.crawl_url(best_link)

    # Get API documentation links
    api_doc_links = crawler.get_api_doc_links(crawl_result)

    # Clean and validate URLs
    valid_urls = [url for url in api_doc_links if crawler.clean_and_validate_url(url)]

    # Create a collection
    collection_id = crawler.create_collection(
        name='Snowflake API docs',
        description='Content ingested from Snowflake API + embedded links'
    )

    # Ingest websites
    crawler.ingest_websites(collection_id, valid_urls)

    # Query the collection
    query_result = crawler.query_collection(
        collection_id,
        'What are all the api endpoints and their expected inputs? Try to list as many as you can and for each API, list out as many endpoints (at least 7) and give examples of how to call each end point with expected parameters'
    )
    print(query_result)

if __name__ == "__main__":
    main()

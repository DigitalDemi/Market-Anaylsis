import asyncio
import aiohttp
import logging
import json
from selectolax.parser import HTMLParser
from typing import Optional
from src.utils.data_lake import AsyncDataLakeManager
from src.utils.parse import AsyncParser, PaginationConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DaftAsyncScraper:
    def __init__(self, data_lake: AsyncDataLakeManager):
        self.data_lake = data_lake
        self.base_url = "https://www.daft.ie/property-for-rent/dublin/houses"
        self.session = None

    async def fetch_total_results(self, url: str) -> int:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        
        async with self.session.get(url, headers=headers) as response:
            if response.status == 200:
                text = await response.text()
                html = HTMLParser(text)
                data = html.css("script[type='application/json']")
                
                for script in data:
                    try:
                        parsed = json.loads(script.text())
                        return parsed['props']['pageProps']['paging']['totalResults']
                    except (json.JSONDecodeError, KeyError) as e:
                        continue
                        
            raise ValueError("Could not fetch total results")

    async def scrape_page(self, url: str, max_retries: int = 3) -> Optional[list]:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }

        for attempt in range(max_retries):
            try:
                logger.info(f"Scraping page: {url} (attempt {attempt + 1})")
                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        text = await response.text()
                        html = HTMLParser(text)
                        data = html.css("script[type='application/json']")
                        
                        for script in data:
                            try:
                                parsed = json.loads(script.text())
                                listings = parsed.get('props', {}).get('pageProps', {}).get('listings', [])
                                
                                if listings:
                                    return listings
                            except (json.JSONDecodeError, KeyError) as e:
                                logger.error(f"Error parsing JSON: {e}")
                                continue
                                
                await asyncio.sleep(2 ** attempt)
                
            except Exception as e:
                logger.error(f"Error scraping page {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                    
        return []

    async def scrape_all_pages(self, total_results: int) -> list:
        all_listings = []
        page_size = 20
        
        # Calculate pages like the original code
        sub = total_results % 10
        total_pages = (total_results - sub + 20) // page_size
        
        batch_size = 3
        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            tasks = []
            
            for page in range(batch_start, batch_end):
                page_url = f"{self.base_url}?pageSize=20&from={page*page_size}"
                tasks.append(self.scrape_page(page_url))
            
            batch_results = await asyncio.gather(*tasks)
            for result in batch_results:
                if result:
                    all_listings.extend(result)
                    logger.info(f"Added {len(result)} listings")
            
            await asyncio.sleep(3)
            
        return all_listings

    async def main(self):
        """Main execution flow."""
        try:
            async with aiohttp.ClientSession() as self.session:
                # Fetch total results from the first page
                total_results = await self.fetch_total_results(self.base_url)
                logger.info(f"Total results: {total_results}")

                # Scrape all pages
                data = await self.scrape_all_pages(total_results)
                logger.info(f"Scraped {len(data)} total listings")

                if data:  # Only save if we have data
                    # Store raw and processed data
                    raw_path = await self.data_lake.store_raw(data, source="daft")
                    processed_path = await self.data_lake.process_and_store(raw_path, source="daft")

                    logger.info(f"Raw data stored at: {raw_path}")
                    logger.info(f"Processed data stored at: {processed_path}")
                    return processed_path  # Return the path to indicate success
                else:
                    logger.error("No data was scraped")
                    return None
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    data_lake = AsyncDataLakeManager(base_path="housing_data")
    scraper = DaftAsyncScraper(data_lake)
    asyncio.run(scraper.main())


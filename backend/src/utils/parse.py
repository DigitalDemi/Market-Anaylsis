from selectolax.parser import HTMLParser as parser
import json
import aiohttp
import asyncio
from typing import List, Tuple, Optional, Dict, Any, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class PaginationConfig:
    page_selector: str
    url_pattern: str = 'p_'  

class AsyncParser:
    def __init__(
        self, 
        session: aiohttp.ClientSession,
        url: Optional[str] = None,
        parse_type: str = 'html',
        selectors: Optional[Dict[str, Union[str, Dict]]] = None,
        pagination_config: Optional[PaginationConfig] = None
    ) -> None:
        self.session = session
        self.url = url
        self.parse_type = parse_type
        self.selectors = selectors or {}
        self.pagination_config = pagination_config
        self.script_selectors = [
            "script[type='application/json']",
            "script#__NEXT_DATA__",
            "script[type='text/javascript']",
            "script[type='application/ld+json']"
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    async def get_html(self) -> Optional[str]:
        """Fetch HTML content asynchronously"""
        if not self.url:
            raise ValueError("URL is not set")
            
        try:
            async with self.session.get(self.url, headers=self.headers) as response:
                match response.status:
                    case 200:
                        return await response.text()
                    case 404:
                        raise Exception("Page not found")
                    case 403:
                        raise Exception("Access forbidden - might need different headers")
                    case 429:
                        raise Exception("Too many requests - rate limited")
                    case _:
                        raise Exception(f"Request failed with status code: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching HTML: {str(e)}")
            return None

    async def parse_response(self) -> Optional[parser]:
        """Parse HTML response into parser object"""
        html_content = await self.get_html()
        return parser(html_content) if html_content else None

    async def selector(self, html: parser) -> Optional[Union[str, List[Dict[str, Any]]]]:
        """Find and return content based on parse_type"""
        if not html:
            raise ValueError("HTML content not available")
            
        if self.parse_type == 'script':
            return await self._parse_script(html)
        else:
            return await self._parse_html(html)

    async def _parse_script(self, html: parser) -> Optional[str]:
        """Parse script content"""
        for selector in self.script_selectors:
            scripts = html.css(selector)
            if scripts and scripts[0].text():
                try:
                    data = json.loads(scripts[0].text())
                    # Extract listings from the props.pageProps.listings path
                    listings = data.get('props', {}).get('pageProps', {}).get('listings', [])
                    return [{
                        'title': item.get('listing', {}).get('title', 'No Title'),
                        'price': item.get('listing', {}).get('price', 'No Price'),
                        'beds': item.get('listing', {}).get('numBedrooms', 'N/A'),
                        'baths': item.get('listing', {}).get('numBathrooms', 'N/A')
                    } for item in listings]
                except Exception as e:
                    logger.error(f"Error parsing script: {str(e)}")
                    continue

        raise Exception("No matching script tags found")

    async def _parse_html(self, html: parser) -> List[Dict[str, Any]]:
        """Parse HTML elements"""
        if not self.selectors:
            raise ValueError("Selectors not available")
        
        results = []
        parent_selector = self.selectors.get('parent', '')
        parents = html.css(parent_selector) if parent_selector else [html]
        
        for parent in parents:
            item_data = {}
            
            for key, selector_info in self.selectors.items():
                if key == 'parent':
                    continue
                    
                if isinstance(selector_info, dict):
                    selector = selector_info.get('selector', '')
                    attribute = selector_info.get('attribute', 'text')
                    
                    element = parent.css_first(selector)
                    if element:
                        if attribute == 'text':
                            item_data[key] = element.text().strip()
                        else:
                            item_data[key] = element.attributes.get(attribute, '')
                else:
                    element = parent.css_first(selector_info)
                    item_data[key] = element.text().strip() if element else ''
            
            if item_data:
                results.append(item_data)
        
        return results

    async def find_last_page(self, html: parser) -> int:
        """Find the last page number from pagination links"""
        if not html or not self.pagination_config:
            return 1
            
        page_links = html.css(self.pagination_config.page_selector)
        last_page = 1
        
        for link in page_links:
            href = link.attributes.get('href', '')
            try:
                page_num = int(href.split(self.pagination_config.url_pattern)[1].strip('/'))
                last_page = max(last_page, page_num)
            except (ValueError, IndexError):
                continue
                
        logger.info(f"Found last page: {last_page}")
        return last_page

    async def process_all_pages(self) -> List[Dict[str, Any]]:
        """Process all pages"""
        all_results = []
        html = await self.parse_response()
        if not html:
            return all_results

        # Get first page results
        try:
            results = await self.selector(html)
            if results:
                all_results.extend(results if isinstance(results, list) else [results])
        except Exception as e:
            logger.error(f"Error processing first page: {str(e)}")
            return all_results

        if self.pagination_config:
            last_page = await self.find_last_page(html)
            semaphore = asyncio.Semaphore(3)  # Limit concurrent requests
            
            async def process_page(page: int):
                async with semaphore:
                    base_url = self.url.split(self.pagination_config.url_pattern)[0].rstrip('/')
                    url = f"{base_url}/{self.pagination_config.url_pattern}{page}/"
                    self.url = url
                    html = await self.parse_response()
                    if not html:
                        return None
                    
                    try:
                        results = await self.selector(html)
                        await asyncio.sleep(1)  # Rate limiting
                        return results
                    except Exception as e:
                        logger.error(f"Error processing page {page}: {str(e)}")
                        return None

            tasks = [process_page(page) for page in range(2, last_page + 1)]
            results = await asyncio.gather(*tasks)
            
            for page_results in results:
                if page_results:
                    all_results.extend(page_results if isinstance(page_results, list) else [page_results])
        
        return all_results

    async def main(self) -> Optional[List[Dict[str, Any]]]:
        """Main execution flow"""
        try:
            return await self.process_all_pages()
        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
            return None

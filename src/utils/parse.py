import asyncio
import aiohttp
import logging
import json
from typing import Optional, Dict, Any, List
from selectolax.parser import HTMLParser

class AsyncParser:
    def __init__(self, session: aiohttp.ClientSession, **kwargs):
        self.session = session
        self.url = kwargs.get('url')
        self.parse_type = kwargs.get('parse_type', 'script')
        self.selectors = kwargs.get('selectors', {})
        self.script_selectors = [
            "script[type='application/json']",
            "script#__NEXT_DATA__",
            "script[type='text/javascript']",
            "script[type='application/ld+json']"
        ]
        
    async def get_html(self) -> Optional[str]:
        """Fetch HTML content asynchronously"""
        if not self.url:
            raise ValueError("URL is not set")
            
        try:
            async with self.session.get(
                self.url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                    'Accept-Language': 'en-US,en;q=0.5',
                }
            ) as response:
                match response.status:
                    case 200:
                        return await response.text()
                    case 404:
                        raise Exception("Page not found")
                    case 403:
                        raise Exception("Access forbidden")
                    case 429:
                        raise Exception("Rate limited")
                    case _:
                        raise Exception(f"Request failed with status {response.status}")
        except Exception as e:
            logging.error(f"Error fetching HTML: {str(e)}")
            return None

    async def main(self) -> Optional[List[Dict[str, Any]]]:
        """Main parsing logic"""
        html_content = await self.get_html()
        if not html_content:
            return None
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._parse_content,
            html_content
        )
    
    def _parse_content(self, html_content: str) -> Optional[List[Dict[str, Any]]]:
        """Synchronous parsing logic - runs in executor"""
        html = HTMLParser(html_content)
        
        if self.parse_type == 'script':
            script_content = self._parse_script(html)
            if script_content:
                try:
                    data = json.loads(script_content)
                    listings = data.get('props', {}).get('pageProps', {}).get('listings', [])
                    
                    return [{
                        'title': item.get('listing', {}).get('title'),
                        'price': item.get('listing', {}).get('price'),
                        'bedrooms': item.get('listing', {}).get('numBedrooms'),
                        'bathrooms': item.get('listing', {}).get('numBathrooms'),
                        'address': item.get('listing', {}).get('address'),
                        'daft_id': item.get('listing', {}).get('id'),
                        'property_type': item.get('listing', {}).get('propertyType'),
                        'date_added': item.get('listing', {}).get('dateTimeAdded'),
                        'ber_rating': item.get('listing', {}).get('berRating'),
                        'category': item.get('listing', {}).get('category'),
                        'facilities': item.get('listing', {}).get('facilities', []),
                        'images': [img.get('url') for img in item.get('listing', {}).get('images', [])],
                    } for item in listings]
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing JSON from script: {e}")
                    return None
                except Exception as e:
                    logging.error(f"Error processing script data: {e}")
                    return None
        else:
            return self._parse_html(html)
    
    def _parse_script(self, html: HTMLParser) -> Optional[str]:
        """Parse script content"""
        for selector in self.script_selectors:
            scripts = html.css(selector)
            if scripts and scripts[0].text():
                return scripts[0].text()
        return None
    
    def _parse_html(self, html: HTMLParser) -> List[Dict[str, Any]]:
        """Parse HTML elements"""
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
                    if element:
                        item_data[key] = element.text().strip()
            
            if item_data:
                results.append(item_data)
        
        return results

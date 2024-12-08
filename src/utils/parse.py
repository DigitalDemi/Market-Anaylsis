from selectolax.parser import HTMLParser as parser
import json
import requests as r
from typing import Callable, List, Tuple, Optional, Dict, Any, Union
from dataclasses import dataclass


@dataclass
class PaginationConfig:
    pagination_div: str      
    page_selector: str        
    attribute: str = 'href'    
    pattern: str = 'p_'         


class Parser:
    def __init__(
        self, 
        url: Optional[str] = None, 
        filename: Optional[str] = None, 
        scripts: Optional[str] = None,
        selectors: Optional[Dict[str, Union[str, Dict]]] = None,
        url_constructor: Optional[Callable[[int], List[Tuple[str, str]]]] = None,
        parse_type: str = 'script',
        pagination_config: Optional[PaginationConfig] = None, 
    ) -> None:
        self.url = url
        self.response = None
        self.filename = filename
        self.scripts = scripts
        self.url_constructor = url_constructor
        self.values: Optional[Dict[str, Any]] = None
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
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        self.html = self.parseResponse() if url else None
    
    def parseResponse(self) -> Optional[parser]:
        """Parse HTML response into parser object"""
        html_content = self.getHTML()
        return parser(html_content) if html_content else None

    def getHTML(self) -> Optional[str]:
        """Fetch HTML content from URL"""
        if not self.url:
            raise ValueError("URL is not set")
            
        try:
            self.response = r.get(self.url, headers=self.headers)
            match self.response.status_code:
                case 200:
                    return self.response.text
                case 404:
                    raise Exception("Page not found")
                case 403:
                    raise Exception("Access forbidden - might need different headers")
                case 429:
                    raise Exception("Too many requests - rate limited")
                case _:
                    raise Exception(f"Request failed with status code: {self.response.status_code}")
        except Exception as e:
            print(f"Error fetching HTML: {str(e)}")
            return None

    def selector(self) -> Optional[Union[str, List[Dict[str, Any]]]]:
        """Find and return content based on parse_type"""
        if not self.html:
            raise ValueError("HTML content not available")
            
        if self.parse_type == 'script':
            return self._parse_script()
        else:
            return self._parse_html()

    def _parse_script(self) -> Optional[str]:
        """Parse script content"""
        for selector in self.script_selectors:
            scripts = self.html.css(selector)
            if scripts and scripts[0].text():
                print(f"Found scripts with {selector}")
                return scripts[0].text()
        
        # Debug information
        if self.html.html:
            print("Debug - First 500 chars of HTML:")
            print(self.html.html[:500])
        
        all_scripts = self.html.css("script")
        print("\nAll script tags found:")
        if not all_scripts:
            print("No script tags found at all - page might not be loading properly")
        
        for script in all_scripts:
            print("\nScript details:")
            print(f"Type: {script.attributes.get('type', 'no type')}")
            print(f"ID: {script.attributes.get('id', 'no id')}")
            print(f"First 100 chars: {script.text()[:100] if script.text() else 'No text'}")
            print("---")
        
        raise Exception("No matching script tags found")

    def _parse_html(self) -> List[Dict[str, Any]]:
        """Parse HTML elements"""
        if not self.selectors:
            raise ValueError("Selectors not available for HTML parsing")
        
        results = []
        parent_selector = self.selectors.get('parent', '')
        parents = self.html.css(parent_selector) if parent_selector else [self.html]
        
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

    def process_all_pages(self) -> List[Dict[str, Any]]:
        """Process all pages if pagination config exists, otherwise process single page"""
        all_results = []
        
        if not self.pagination_config:
            # Process single page
            try:
                results = self.selector()
                if results:
                    if isinstance(results, list):
                        all_results.extend(results)
                    else:
                        all_results.append(results)
            except Exception as e:
                print(f"Error processing page: {str(e)}")
            return all_results
        
        # Process multiple pages
        last_page = self.find_last_page()
        urls = self.construct_urls(last_page)
        
        for url, filename in urls:
            self.url = url
            self.filename = filename
            self.html = self.parseResponse()
            
            if not self.html:
                print(f"Failed to parse HTML for {url}")
                continue
                
            try:
                results = self.selector()
                if results:
                    if isinstance(results, list):
                        all_results.extend(results)
                    else:
                        all_results.append(results)
                print(f"Processed {url}")
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")
                continue
        
        return all_results

    def find_last_page(self) -> int:
        """Find the last page number from pagination links"""
        if not self.html or not self.pagination_config:
            return 1
            
        pagination_div = self.html.css_first(self.pagination_config.pagination_div)
        if not pagination_div:
            return 1
            
        page_links = pagination_div.css(self.pagination_config.page_selector)
        last_page = 1
        
        for link in page_links:
            try:
                title = link.attributes.get('title', '')
                if title.startswith('Page '):
                    page_num = int(title.split('Page ')[-1])
                    last_page = max(last_page, page_num)
            except ValueError:
                continue
                
        print(f"Found last page: {last_page}")
        return last_page

    def construct_urls(self, last_page: int) -> List[Tuple[str, str]]:
        """Create URLs for all pages"""
        if not self.url:
            return []
            
        base_url = self.url.split('p_')[0]
        if base_url.endswith('/'):
            base_url = base_url[:-1]
            
        urls = []
        for page in range(1, last_page + 1):
            page_url = f"{base_url}/p_{page}/"
            filename = f"properties_page_{page}.json"
            urls.append((page_url, filename))
            
        return urls

    # Rest of the methods remain unchanged
    def getScript(self) -> None:
        """Parse JSON from script content"""
        script_content = self.selector()
        if script_content:
            try:
                self.values = json.loads(script_content)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {str(e)}")
                self.values = None


    def save_json(self, data: List[Dict], filename: Optional[str] = None) -> None:
        """Save data to JSON file"""
        # Use provided filename or default to self.filename
        save_filename = filename or self.filename
        if not save_filename:
            raise ValueError("No filename provided")
            
        try:
            with open(save_filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Data saved to {save_filename}")
        except Exception as e:
            print(f"Error saving JSON: {str(e)}")

    def loadJson(self) -> Optional[List[Dict[str, Any]]]:
        """Extract listing information from parsed data"""
        if not self.values:
            return None
            
        try:
            if self.parse_type == 'html':
                return self.values
                
            listings = self.values.get('props', {}).get('pageProps', {}).get('listings', [])
            results = [{
                'title': item.get('listing', {}).get('title', 'No Title'),
                'price': item.get('listing', {}).get('price', 'No Price'),
                'beds': item.get('listing', {}).get('numBedrooms', 'N/A'),
                'baths': item.get('listing', {}).get('numBathrooms', 'N/A')
            } for item in listings]
            self.values = results
            return results
        except Exception as e:
            print(f"Error processing JSON: {str(e)}")
            return None

    def main(self) -> Optional[List[Dict[str, Any]]]:
        """Main execution flow"""
        try:
            if self.parse_type == 'script':
                self.getScript()
                return self.loadJson()
            else:
                return self.selector()
        except Exception as e:
            print(f"Error in main execution: {str(e)}")
            return None

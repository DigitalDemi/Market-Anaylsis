from selectolax.parser import HTMLParser as parser
import json
import requests as r
from typing import Callable, List, Tuple, Optional

class Parser:
    def __init__(self, url: Optional[str] = None, filename: Optional[str] = None, scripts: Optional[str] = None, url_constructor = None) -> None:
        self.url = url
        self.response = None
        self.filename = filename
        self.scripts = scripts
        self.url_constructor = url_constructor
        self.values = None
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


    
    def set_url_constructor(self, constructor_func: Callable[[int], List[Tuple[str, str]]]) -> None:
        self.url_constructor = constructor_func
    
    def process_urls(self, total: int) -> list:
        all_results = []
        urls = self.url_constructor(total)
        
        for url, filename in urls:
            self.url = url
            self.filename = filename
            self.html = self.parseResponse()
            results = self.main()
            all_results.extend(results)
            print(f"Processed {url}")
        
        return all_results
    
    def getHTML(self):
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

    def parseResponse(self):
        return parser(self.getHTML()) if self.url else None

    def selector(self):
       selectors = [
           "script[type='application/json']",
           "script#__NEXT_DATA__", 
           "script[type='text/javascript']",
           "script[type='application/ld+json']"
       ]
       
       for selector in selectors:
           scripts = self.html.css(selector)
           if scripts:
               print(f"Found scripts with {selector}")
               return scripts[0].text()
       
       print("Debug - First 500 chars of HTML:")
       print(self.html.html[:500])
       
       print("\nAll script tags found:")
       all_scripts = self.html.css("script")
       if not all_scripts:
           print("No script tags found at all - page might not be loading properly")
       
       for script in all_scripts:
           print("\nScript details:")
           print(f"Type: {script.attributes.get('type', 'no type')}")
           print(f"ID: {script.attributes.get('id', 'no id')}")
           print(f"First 100 chars: {script.text()[:100] if script.text() else 'No text'}")
           print("---")
       
       raise Exception("No matching script tags found")

    def getScript(self):
        self.values = json.loads(self.selector())

    def loadJson(self):
        listings = self.values.get('props', {}).get('pageProps', {}).get('listings', [])
        self.values = [{
            'title': item.get('listing', {}).get('title', 'No Title'),
            'price': item.get('listing', {}).get('price', 'No Price'),
            'beds': item.get('listing', {}).get('numBedrooms', 'N/A'),
            'baths': item.get('listing', {}).get('numBathrooms', 'N/A')
        } for item in listings]
        return self.values
        

    def main(self):
        self.getScript()
        self.loadJson()
        return self.values

import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional

class AsyncApi:
    def __init__(self, session: aiohttp.ClientSession, **kwargs):
        self.session = session
        self.base_api_url = kwargs.get('base_api_url')
        self.payload_api_url = kwargs.get('payload_api_url')
        self.api_key = kwargs.get('api_key')
        self.correlation_id = kwargs.get('correlation_id')

    async def get_data(self, page_size: int = 20) -> List[Dict[str, Any]]:
        """Get data asynchronously"""
        all_results = []
        
        tasks = []
        for i in range(page_size):
            payload = {
                "ApiKey": self.api_key,
                "CorrelationId": self.correlation_id,
                "RequestTypeId": 2,
                "RequestVerb": "GET",
                "Endpoint": self.payload_api_url,
                "Page": i,
                "PageSize": page_size,
                "SortColumn": 2,
                "SortDirection": 2,
                "Url": self.payload_api_url
            }
            
            tasks.append(self._fetch_page(payload))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error fetching page: {str(result)}")
                continue
            if result:
                all_results.extend(result.get("SearchResults", []))
                
        return all_results

    async def _fetch_page(self, payload: Dict) -> Optional[Dict]:
        """Fetch a single page of data"""
        try:
            async with self.session.get(self.base_api_url, params=payload) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    logging.warning("Rate limited")
                    return None
                else:
                    logging.error(f"Failed to fetch page: status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching page: {str(e)}")
            return None

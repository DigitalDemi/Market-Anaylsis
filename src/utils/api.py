import requests as r
from typing import List,Dict,Any, Optional
import json


class Api:
    def __init__(self, base_api_url: str, payload_api_url: str, api_key: str, correlation_id: str):
        self.api_key = api_key
        self.correlation_id = correlation_id
        self.base_api_url = base_api_url
        self.payload_api_url = payload_api_url
        self.values = None
        self.response = None
        self.parser = None

    def get_data(self, page_size: int = 20) -> List[Dict[str, Any]]:
        all_results = []
        
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

            try:
                self.response = r.get(self.base_api_url, params=payload)
                
                match self.response.status_code:
                    case 200:
                        self.values = self.response.json()
                        if self.values:
                            all_results.extend(self.values.get("SearchResults", []))
                        print(f"Successfully fetched page {i + 1}/{page_size}")
                    case 429:
                        print(f"Rate limited at page {i + 1}")
                        break
                    case _:
                        print(f"Failed to fetch page {i + 1}: status code {self.response.status_code}")
                        continue
                        
            except r.RequestException as e:
                print(f"Error fetching page {i + 1}: {str(e)}")
                continue

        return all_results

    def save_to_json(self, filename: str) -> None:
        with open(filename, 'w') as f:
            json.dump(self.values, f, indent=2)

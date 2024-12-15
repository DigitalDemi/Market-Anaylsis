import json
from typing import List, Dict, Any

class DataFilter:
    def __init__(self, keys_to_keep: List[str] = None):
        self.keys_to_keep = keys_to_keep or [
            'DisplayAddress', 
            'PriceAsString', 
            'BedsString', 
            'SeoUrl', 
            'MainPhoto', 
            'Photos'
        ]

    def filter_data(self, data: List[Dict]) -> List[Dict]:
        filtered_data = []
        
        for item in data:
            filtered_item = {}
            for key in self.keys_to_keep:
                if key in item:
                    filtered_item[key] = item[key]
            filtered_data.append(filtered_item)
            
        return filtered_data

    def load_json(self, filename: str) -> List[Dict]:
        with open(filename) as f:
            return json.load(f)

    def save_json(self, data: List[Dict], filename: str) -> None:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

    def process_file(self, input_file: str, output_file: str) -> None:
        data = self.load_json(input_file)
        filtered_data = self.filter_data(data)
        
        self.save_json(filtered_data, output_file)
        print(f"Filtered data saved to {output_file}")


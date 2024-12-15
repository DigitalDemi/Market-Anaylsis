import json 
import pandas as pd
from glob import glob
from typing import List,Dict,Union
import os


class Processor:
    def __init__(self, keys_to_keep):
        self.keys_to_keep = keys_to_keep

    def process_file(self, file_path):
        with open(file_path) as f:
            data = json.load(f)
            return self.filter_data(data)

    def process_directory(self, directory, pattern="*.json"):
        all_results = []
        path_pattern = os.path.join(directory, pattern)

        for file_path in glob(path_pattern):
            filtered_data = self.process_file(file_path)
            all_results.extend(filtered_data)
            print(f"Processed {file_path}")

        return all_results
    
    def filter_data(self, data):
        if isinstance(data, list):
            items = data
        else:
            items = data.get('SearchResults', [])
            
        return [{
            key: item.get(key)
            for key in self.keys_to_keep
            if key in item
        } for item in items]

    
    def save_results(self, data, base_filename ):
        with open(f"{base_filename}.json", 'w') as f:
            json.dump(data, f, indent=2)
            
        df = pd.DataFrame(data)
        df.to_csv(f"{base_filename}.csv", index=False)

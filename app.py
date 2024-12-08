# Hi here are different websites we can scrap ireland
# draft, home, property, rent, find a home, hosuing anywhere, dng
from src.utils.parse import Parser
from src.utils.api import Api
from src.utils.filter import DataFilter
import json
from src.urls import daft
from typing import List, Dict, Tuple
# from src.urls import daft 

#TODO: Time to do property tommrow

# Need to pull the information from somewhere, different between worldcitydb and wikipidia
if __name__ == "__main__":

    # Initialize parser with initial Daft URL to get total results
    initial_url = "https://www.daft.ie/property-for-rent/dublin/houses"
    parser = Parser(url=initial_url)

    try:
        parser.getScript()
        total_results = parser.values['props']['pageProps']['paging']['totalResults']
        print(f"Found {total_results} total listings")
        
        parser.set_url_constructor(daft.draft)
        results = parser.process_urls(total_results)
        
        output_file = "daft_listings.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"Successfully saved {len(results)} listings to {output_file}")
        
    except Exception as e:
        print(f"Error during scraping: {e}")



    api = Api(
        base_api_url="https://api.myhome.ie/search",  
        payload_api_url="https://www.myhome.ie/rentals/dublin/property-to-rent",  
        api_key="4284149e-13da-4f12-aed7-0d644a0b7adb",
        correlation_id="22fade32-8266-4c26-9ea7-6aa470a30f07"
    )

    results = api.get_data(page_size=20)
    api.save_to_json('property_listings.json')


    data_filter = DataFilter()
    filtered_results = data_filter.filter_data(results)
    data_filter.save_json(filtered_results, 'filtered_listings.json')


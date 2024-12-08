# Hi here are different websites we can scrap ireland
# draft, home, property, rent, find a home, hosuing anywhere, dng
from src.utils.parse import Parser
from src.utils.api import Api
from src.utils.filter import DataFilter
import json
from src.urls import daft
from typing import List, Dict, Tuple
from dataclasses import dataclass
# from src.urls import daft 

#TODO: Time to do property tommrow

# Need to pull the information from somewhere, different between worldcitydb and wikipidia
if __name__ == "__main__":

    @dataclass
    class PaginationConfig:
        pagination_div: str
        page_selector: str

    pagination_config = PaginationConfig(
        pagination_div='div#pages',
        page_selector='a[title^="Page"]'
    )

    script_parser = Parser(
        url="https://www.daft.ie/property-for-rent/dublin/houses?pageSize=20",
        parse_type='script'
    )

    results = script_parser.main()
    script_parser.save_json(results, "daft.json")

    pagination_config = PaginationConfig(
        page_selector='a[title^="Page"]',  
        pagination_div='div#pages',  
    )

    html_parser = Parser(
            url='https://www.property.ie/property-to-let/dublin/price_international_rental-onceoff_standard/',
            parse_type='html',
            selectors = {
    'parent': '.search_result',  
    'ber': {
        'selector': '.ber-search-results img',
        'attribute': 'src'
    },
    'address': {
        'selector': '.sresult_address h2 a',
        'attribute': 'text'
    },
    'url': {
        'selector': '.sresult_address h2 a',
        'attribute': 'href'
    },
    'image': {
        'selector': 'img.thumb',
        'attribute': 'src'
    },
    'price': {
        'selector': '.sresult_description h3',
        'attribute': 'text'
    },
    'details': {
        'selector': '.sresult_description h4',
        'attribute': 'text'
    },
    'availability': {
        'selector': '.sresult_available_from',
        'attribute': 'text'
    },
    'description': {
        'selector': '.sresult_description p',
        'attribute': 'text'
    }
},
            filename="property.json",
            pagination_config=pagination_config
    
    )

    results = html_parser.process_all_pages()
    html_parser.save_json(results, "property_all.json")

    selectors = {
    'parent': '.search_result',
    'address': {
        'selector': '.sresult_address h2 a', 
        'attribute': 'text'
    },
    'url': {
        'selector': '.sresult_address h2 a', 
        'attribute': 'href'
    },
    'ber': {
        'selector': '.ber-search-results img',
        'attribute': 'src'
    },
    'image': {
        'selector': 'img.sresult_thumb',
        'attribute': 'src'
    },
    'details': {
        'selector': '.sresult_description',
        'attribute': 'text'
    }
}

    pagination_config = PaginationConfig(
        pagination_div='div#pages',
        page_selector='a[title^="Page"]'  
    )

    parser = Parser(
        url="https://www.rent.ie/houses-to-let/renting_dublin/",
        filename="dublin_rentals.json",
        parse_type='html',
        selectors=selectors,
        pagination_config=pagination_config
    )

    parser.save_json(results, "dublin_rentals.json")

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


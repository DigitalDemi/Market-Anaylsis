# Hi here are different websites we can scrap ireland
# draft, home, property, rent, find a home, hosuing anywhere, dng
from src.utils.parse import Parser
import json
from src.urls import daft
# from src.urls import daft 

#TODO: Home ie scrap alot of the code you have already

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


    parser = Parser(url="https://www.myhome.ie/rentals/dublin/property-to-rent", 
                   filename="output.json", 
                   scripts="listings")
    result = parser.main()

    results = parser.process_urls(100)
   
    with open("output.json", "w") as f:
        json.dump(result, f, indent=2)
        
    print(f"Saved {len(result)} listings to {parser.filename}")




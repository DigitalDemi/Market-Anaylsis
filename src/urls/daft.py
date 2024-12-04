from typing import Tuple, List

def draft(total: int) -> List[Tuple[str, str]]:
    urls = []
    try:
        sub = total % 20
        pages_needed = total - sub + 20
        offset = 20
        
        while offset < pages_needed:
            url = f"https://www.daft.ie/property-for-rent/dublin/houses?numBeds_to=3&numBeds_from=3&pageSize=20&from={offset}"
            filename = f'results_daft_{offset}.json'
            urls.append((url, filename))
            offset += 20
            
        return urls
        
    except Exception as e:
        print(f"Error generating URLs: {e}")
        return []

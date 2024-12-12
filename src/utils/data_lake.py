from dataclasses import dataclass
from src.urls.daft import DAFT_CONFIG  
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import logging
import pandas as pd
import json
from .parse import Parser
from .api import Api

@dataclass
class ScraperConfig:
    name: str
    scraper_class: Any
    scraper_args: Dict[str, Any]

def create_directory_path(base_path: Path, source: str, date: datetime) -> Path:
    """Create hierarchical directory path"""
    return base_path / source / str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"

class DataLakeManager:
    def __init__(self, base_path: str = "housing_data"):
        self.base_path = Path(base_path)
        self.raw_path = self.base_path / "raw"
        self.processed_path = self.base_path / "processed"
        
        for path in [self.raw_path, self.processed_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def store_raw(self, data: Any, source: str) -> Path:
        """Store raw scraped data"""
        timestamp = datetime.now()
        directory = create_directory_path(self.raw_path, source, timestamp)
        directory.mkdir(parents=True, exist_ok=True)
        
        filepath = directory / f"{source}_{timestamp.strftime('%H%M%S')}.json"
        with open(filepath, 'w') as f:
            json.dump(data, f)
        return filepath

    def process_and_store(self, raw_filepath: Path, source: str) -> Optional[Path]:
        """Process raw data and store as parquet"""
        try:
            with open(raw_filepath) as f:
                raw_data = json.load(f)
            
            processed_df = pd.DataFrame(raw_data)
            timestamp = datetime.now()
            
            proc_dir = create_directory_path(self.processed_path, source, timestamp)
            proc_dir.mkdir(parents=True, exist_ok=True)
            
            proc_filepath = proc_dir / f"{source}_{timestamp.strftime('%H%M%S')}.parquet"
            processed_df.to_parquet(proc_filepath)
            return proc_filepath
            
        except Exception as e:
            logging.error(f"Error processing data for {source}: {str(e)}")
            return None

class HousingCollector:
    def __init__(self, data_lake: DataLakeManager):
        self.data_lake = data_lake
        self.scrapers = self._initialize_scrapers()

    def _initialize_scrapers(self) -> Dict[str, ScraperConfig]:
        """Initialize scraper configurations"""
        return {
            'daft': ScraperConfig(
                name='daft',
                scraper_class=Parser,
                scraper_args=DAFT_CONFIG
            ),
            'property': ScraperConfig(
                name='property',
                scraper_class=Parser,
                scraper_args={
                    'url': 'https://www.property.ie/property-to-let/dublin/',
                    'parse_type': 'html',
                    'selectors': {
                        'parent': '.search_result',
                        'address': {'selector': '.sresult_address h2 a', 'attribute': 'text'},
                        'price': {'selector': '.sresult_description h3', 'attribute': 'text'}
                    }
                }
            ),
            'myhome': ScraperConfig(
                name='myhome',
                scraper_class=Api,
                scraper_args={
                    'base_api_url': "https://api.myhome.ie/search",
                    'payload_api_url': "https://www.myhome.ie/rentals/dublin/property-to-rent",
                    'api_key': "4284149e-13da-4f12-aed7-0d644a0b7adb",
                    'correlation_id': "22fade32-8266-4c26-9ea7-6aa470a30f07"
                }
            )
        }

    def collect_source(self, source: str) -> Optional[Path]:
        """Collect data from a specific source with type-specific handling"""
        try:
            config = self.scrapers.get(source)
            if not config:
                raise ValueError(f"Unknown source: {source}")
            
            scraper = config.scraper_class(**config.scraper_args)
            
            # Handle different scraper types
            if isinstance(scraper, Api):
                data = scraper.get_data(page_size=20)  # Api uses get_data method
            else:
                data = scraper.main()  # Parser uses main method
                
            if not data:
                logging.warning(f"No data retrieved from {source}")
                return None
                
            raw_path = self.data_lake.store_raw(data, source)
            return self.data_lake.process_and_store(raw_path, source)
            
        except Exception as e:
            logging.error(f"Error collecting from {source}: {str(e)}", exc_info=True)
            return None
    
    def collect_all(self) -> Dict[str, Optional[Path]]:
        """Collect from all configured sources"""
        return {
            source: self.collect_source(source)
            for source in self.scrapers.keys()
        }

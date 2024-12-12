from src.utils.data_lake import DataLakeManager, HousingCollector
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    data_lake = DataLakeManager()
    collector = HousingCollector(data_lake)

    try:
        results = collector.collect_all()
        
        for source, filepath in results.items():
            if filepath:
                logging.info(f"Successfully collected and processed data from {source}")
                logging.info(f"Stored at: {filepath}")
            else:
                logging.warning(f"Failed to collect data from {source}")
                
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()

import asyncio
import logging
from src.utils.data_lake import AsyncDataLakeManager, AsyncHousingCollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def main():
    data_lake = AsyncDataLakeManager()
    
    async with AsyncHousingCollector(data_lake) as collector:
        results = await collector.collect_all()
        
        for source, filepath in results.items():
            if isinstance(filepath, Exception):
                logging.error(f"Error collecting from {source}: {str(filepath)}")
            elif filepath:
                logging.info(f"Successfully collected and processed data from {source}")
                logging.info(f"Stored at: {filepath}")
            else:
                logging.warning(f"Failed to collect data from {source}")

if __name__ == "__main__":
    asyncio.run(main())

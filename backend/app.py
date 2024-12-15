import argparse
import asyncio
import logging
from src.utils.data_lake import AsyncDataLakeManager, AsyncHousingCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Housing Data Lake CLI')
    parser.add_argument('action', choices=['collect', 'query'], 
                       help='Action to perform: collect new data or query existing data')
    parser.add_argument('--source', help='Data source to query')
    parser.add_argument('--list-sources', action='store_true', 
                       help='List available data sources')
    parser.add_argument('--address', help='Pattern to match in address')
    parser.add_argument('--price-max', type=float, help='Maximum price')
    parser.add_argument('--price-min', type=float, help='Minimum price')
    parser.add_argument('--bedrooms', help='Pattern to match number of bedrooms')
    parser.add_argument('--property-type', help='Pattern to match property type')
    parser.add_argument('--ber-rating', help='Pattern to match BER rating')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    return parser

async def collect_data(debug: bool = False):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting data collection...")
    data_lake = AsyncDataLakeManager()
    
    try:
        async with AsyncHousingCollector(data_lake) as collector:
            logger.info("Initialized collector, starting collection...")
            results = await collector.collect_all()
            
            logger.info("Collection completed. Processing results...")
            for source, filepath in results.items():
                if isinstance(filepath, Exception):
                    logger.error(f"Error collecting from {source}: {str(filepath)}")
                elif filepath:
                    logger.info(f"Successfully collected and processed data from {source}")
                    logger.info(f"Stored at: {filepath}")
                else:
                    logger.warning(f"Failed to collect data from {source}")
    except Exception as e:
        logger.error(f"Error during collection: {str(e)}", exc_info=True)
        raise

def handle_query(args):
    from src.query.query import DataLakeQuery, display_results
    query = DataLakeQuery()
    
    if args.list_sources:
        sources = query.get_available_sources()
        print("Available sources:")
        for source in sources:
            print(f"- {source}")
        return

    if not args.source:
        print("Error: --source is required for query action")
        return

    if args.source not in query.get_available_sources():
        print(f"Error: Source '{args.source}' not found")
        print("Available sources:", query.get_available_sources())
        return

    df = query.query_latest(args.source)
    
    # Build search patterns
    patterns = {}
    if args.address:
        patterns['address'] = args.address
    if args.property_type:
        patterns['property_type'] = args.property_type
    if args.bedrooms:
        patterns['bedrooms'] = args.bedrooms
    if args.ber_rating:
        patterns['ber_rating'] = args.ber_rating

    # Apply search and filters
    df = query.search_properties(
        df, 
        patterns, 
        price_min=args.price_min, 
        price_max=args.price_max
    )

    display_results(df, args.source)

from pathlib import Path
import pandas as pd
from typing import Dict, Optional, List
import logging

class DataLakeQuery:
    def __init__(self, base_path: str = "housing_data"):
        self.base_path = Path(base_path)
        self.processed_path = self.base_path / "processed"
        self.logger = logging.getLogger(__name__)

    def get_available_sources(self) -> List[str]:
        """Get list of available data sources"""
        return [p.name for p in self.processed_path.glob("*") if p.is_dir()]

    def query_latest(self, source: str) -> pd.DataFrame:
        """Query the most recent data file for a source"""
        source_path = self.processed_path / source
        if not source_path.exists():
            return pd.DataFrame()

        files = []
        for year_dir in sorted(source_path.glob("*"), reverse=True):
            for month_dir in sorted(year_dir.glob("*"), reverse=True):
                for day_dir in sorted(month_dir.glob("*"), reverse=True):
                    files.extend(list(day_dir.glob("*.parquet")))
                    if files:
                        break
                if files:
                    break
            if files:
                break

        if not files:
            return pd.DataFrame()

        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        df = pd.read_parquet(latest_file)
        
        # Debug info
        self.logger.info(f"Loaded data shape: {df.shape}")
        self.logger.info(f"Columns available: {df.columns.tolist()}")
        return df

    def search_properties(self, df: pd.DataFrame, patterns: Dict[str, str], 
                         price_min: Optional[float] = None, 
                         price_max: Optional[float] = None) -> pd.DataFrame:
        """
        Search properties using pattern matching and price filtering
        """
        if df.empty:
            return df

        # Start with all rows
        mask = pd.Series([True] * len(df))
        
        # Map common column names to possible variations
        column_mapping = {
            'address': ['address', 'DisplayAddress', 'location', 'title'],
            'price': ['price', 'PriceAsString', 'price_string'],
            'bedrooms': ['bedrooms', 'BedsString', 'num_bedrooms'],
            'property_type': ['property_type', 'PropertyType', 'type'],
            'ber_rating': ['ber_rating', 'BerRating', 'ber']
        }

        # Apply pattern matching
        for search_key, pattern in patterns.items():
            possible_columns = column_mapping.get(search_key, [search_key])
            column_found = False
            
            for col in possible_columns:
                if col in df.columns:
                    self.logger.info(f"Searching in column '{col}' for pattern '{pattern}'")
                    column_data = df[col].astype(str)
                    column_mask = column_data.str.contains(pattern, case=False, na=False, regex=True)
                    mask = mask & column_mask
                    column_found = True
                    self.logger.info(f"Found {column_mask.sum()} matches in column '{col}'")
                    break
            
            if not column_found:
                self.logger.warning(f"No matching column found for {search_key}. Available columns: {df.columns.tolist()}")

        result_df = df[mask]
        self.logger.info(f"After pattern matching: {len(result_df)} results")

        # Handle price filtering
        price_column = None
        for col in ['price', 'PriceAsString', 'price_string']:
            if col in result_df.columns:
                price_column = col
                break

        if price_column:
            # Convert price strings to numeric values
            if price_min is not None or price_max is not None:
                # Extract numeric values from price strings (remove '€', ',', etc.)
                price_series = pd.to_numeric(
                    result_df[price_column].str.extract(r'(\d+(?:,\d+)?(?:\.\d+)?)', expand=False)
                    .str.replace(',', ''),
                    errors='coerce'
                )

                if price_min is not None:
                    result_df = result_df[price_series >= price_min]
                if price_max is not None:
                    result_df = result_df[price_series <= price_max]

                self.logger.info(f"After price filtering: {len(result_df)} results")

        return result_df

def display_results(df: pd.DataFrame, source: str):
    """Display query results in a formatted way"""
    if df.empty:
        print(f"No matching data found for source: {source}")
        return

    print(f"\nResults for {source}:")
    print(f"Total matching records: {len(df)}")
    
    # Map and standardize column names for display
    column_mapping = {
        'DisplayAddress': 'Address',
        'PriceAsString': 'Price',
        'BedsString': 'Beds',
        'PropertyType': 'Type',
        'BerRating': 'BER'
    }
    
    # Identify available columns and map them
    display_cols = []
    for orig_col in df.columns:
        if orig_col in column_mapping:
            df = df.rename(columns={orig_col: column_mapping[orig_col]})
            display_cols.append(column_mapping[orig_col])
        elif orig_col.lower() in ['address', 'price', 'bedrooms', 'property_type', 'ber_rating']:
            display_cols.append(orig_col)

    if display_cols:
        print("\nMatching properties:")
        print(df[display_cols].to_string())
    else:
        print("\nMatching properties:")
        print(df.to_string())

    # Show price statistics if available
    price_col = next((col for col in ['Price', 'price'] if col in df.columns), None)
    if price_col:
        print("\nPrice Statistics for matches:")
        # Convert price strings to numbers for statistics
        price_series = pd.to_numeric(
            df[price_col].str.extract(r'(\d+(?:,\d+)?(?:\.\d+)?)', expand=False)
            .str.replace(',', ''),
            errors='coerce'
        )
        print(price_series.describe().to_string())

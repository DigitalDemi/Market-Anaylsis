import logging
import aiohttp
import asyncio
from typing import Dict, List

logger = logging.getLogger(__name__)

class PropertyAlertManager:
    def __init__(self, api_base_url: str = "http://127.0.0.1:3000"):
        self.api_base_url = api_base_url

    async def fetch_properties(self, user_prefs: dict) -> list:
        """Fetch properties from Rust API matching user preferences"""
        async with aiohttp.ClientSession() as session:
            params = {
                'max_price': user_prefs.get('max_price'),
                'min_price': user_prefs.get('min_price'),
                'bedrooms': user_prefs.get('amount'),
                'property_type': user_prefs.get('property_type'),
                'ber_rating': user_prefs.get('ber_rating'),
                'source': user_prefs.get('source')
            }
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}

            try:
                async with session.get(
                    f"{self.api_base_url}/api/rentals/search",
                    params=params
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"API request failed with status {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching properties: {str(e)}")
                return []

    def matches_location_preferences(self, property: dict, locations: list) -> bool:
        """Check if property matches location preferences"""
        if not locations:
            return True
        address = property.get('address', {}).get('display_address', '').lower()
        return any(loc.lower() in address for loc in locations)

    async def format_message(self, properties: list) -> str:
        """Format properties into a readable message"""
        if not properties:
            return ""
        
        message = ["🏠 Found matching properties!\n"]
        
        for property in properties[:5]:  # Limit to 5 properties
            message.append(f"📍 {property['address'].get('display_address', 'No address')}")
            price = property.get('price', {})
            message.append(f"💰 €{price.get('amount', 0):,.2f}")
            if property.get('bedrooms'):
                message.append(f"🛏️ {property['bedrooms']} bed(s)")
            if property.get('source'):
                message.append(f"🔍 Source: {property['source']}")
            message.append("───────────────")
        
        if len(properties) > 5:
            message.append(f"\n...and {len(properties) - 5} more properties")
        
        return "\n".join(message)

    async def process_updates(self, bot, user_store: Dict) -> None:
        """Process updates and send alerts to users"""
        for user_id, preferences in user_store.items():
            try:
                properties = await self.fetch_properties(preferences)
                
                if 'locations' in preferences and preferences['locations']:
                    properties = [
                        prop for prop in properties
                        if self.matches_location_preferences(prop, preferences['locations'])
                    ]
                
                if properties:
                    message = await self.format_message(properties)
                    if message:
                        try:
                            await bot.send_message(
                                chat_id=user_id,
                                text=message
                            )
                        except Exception as e:
                            logger.error(f"Error sending alert to user {user_id}: {str(e)}")

            except Exception as e:
                logger.error(f"Error processing updates for user {user_id}: {str(e)}")

async def start_alert_checker(bot, user_store: Dict):
    """Start the alert checking loop"""
    alert_manager = PropertyAlertManager()
    logger.info("Starting alert checker...")
    
    while True:
        try:
            await alert_manager.process_updates(bot, user_store)
            await asyncio.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logger.error(f"Error in alert checker: {str(e)}")
            await asyncio.sleep(60)  # Wait a minute before retrying

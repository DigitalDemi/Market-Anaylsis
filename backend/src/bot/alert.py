import logging
import re
import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class PropertySource:
    name: str
    display_name: str
    aliases: List[str]
    base_url: str
    url_pattern: str

class PropertyAlertManager:
    def __init__(self, api_base_url: str = "http://127.0.0.1:3000"):
        self.api_base_url = api_base_url
        self.sources = {
                'daft': PropertySource(
                    name='daft',
                    display_name='Daft.ie',
                    aliases=['daft', 'daft.ie', 'daftie'],
                    base_url='https://www.daft.ie',
                    url_pattern='{}/{}',
                    ),
                'myhome': PropertySource(
                name='myhome',
                display_name='MyHome.ie',
                aliases=['myhome', 'myhome.ie', 'my home'],
                base_url='https://www.myhome.ie',
                url_pattern='https://www.myhome.ie/rentals/brochure/{}/{}',
            ),
                'property': PropertySource(
                    name='property',
                    display_name='Property.ie',
                    aliases=['property', 'property.ie', 'propertyie'],
                    base_url='https://www.property.ie',
                    url_pattern='https://www.property.ie/property-to-let/{}/{}'  # Modified to handle name/id format
                    ),
        }
        self.source_aliases = {
            alias: source.name
            for source_name, source in self.sources.items()
            for alias in source.aliases
        }

    def normalize_source(self, source: str) -> Optional[str]:
        """Normalize source name using aliases"""
        if not source:
            return None
        return self.source_aliases.get(source.lower())

    def get_property_url(self, property: dict) -> Optional[str]:
        """Get the correct URL for a property listing"""
        try:
            source = property.get('source', '').lower()
            source_id = property.get('source_id')
            
            if not source or not source_id:
                return None
                
            # For property.ie, the source_id is already the complete URL
            if source == 'property':
                return source_id

            if source == 'daft':
            # Use the seo_url directly if available
                if property.get('seo_url'):
                    logging.info( f"{self.sources['daft'].base_url}{property['seo_url']}")
                    return f"{self.sources['daft'].base_url}{property['seo_url']}"
                return None
            elif source == 'myhome':
                # Get the SEO URL from the property data
                seo_address = property.get('seo_url', '') or \
                             property.get('address', {}).get('seo_address', '') or \
                             self.create_seo_address(property)
                return self.sources['myhome'].url_pattern.format(seo_address, source_id)
            
            return None
        except Exception as e:
            logger.error(f"Error generating property URL: {str(e)}")
            return None

    def create_seo_address(self, property: dict) -> str:
        """Create a SEO-friendly address slug if not provided"""
        try:
            address = property.get('address', {}).get('display_address', '')
            if not address:
                return ''
            
            # Convert to lowercase and replace spaces/special chars with hyphens
            seo_address = address.lower()
            seo_address = re.sub(r'[^a-z0-9]+', '-', seo_address)
            seo_address = seo_address.strip('-')
            
            return seo_address
        except Exception as e:
            logger.error(f"Error creating SEO address: {str(e)}")
            return ''

    def format_address(self, property: dict) -> str:
        """Format address with area and county if available"""
        address = property.get('address', {}).get('display_address', 'No address')
        location = property.get('location', {})
        
        parts = [address]
        
        if location:
            if location.get('area'):
                parts.append(location['area'])
            if location.get('county'):
                parts.append(location['county'])
                
        return ", ".join(filter(None, parts))

    def get_main_image(self, property: dict) -> Optional[str]:
        """Extract main image URL from property data with null checking"""
        try:
            if not property or not isinstance(property, dict):
                return None

            photos = property.get('photos', [])
            if photos and isinstance(photos, list):

                main_photo = next(
                    (p for p in photos if isinstance(p, dict) and p.get('is_main')),
                    next((p for p in photos if isinstance(p, dict) and p.get('url')), None)
                )
                if main_photo and isinstance(main_photo, dict):
                    return main_photo.get('url')

            main_photo = property.get('main_photo')
            if isinstance(main_photo, str) and main_photo.startswith(('http://', 'https://')):
                return main_photo

            photo_url = property.get('photo_url')
            if isinstance(photo_url, str) and photo_url.startswith(('http://', 'https://')):
                return photo_url

            return None
        except Exception as e:
            logger.debug(f"Error getting main image: {e}")
            return None

    async def fetch_properties(self, user_prefs: dict) -> list:
        """Fetch properties from Rust API matching user preferences"""
        async with aiohttp.ClientSession() as session:
            params = {}
            
            # Source handling
            if 'source' in user_prefs:
                source = self.normalize_source(user_prefs['source'])
                if source:
                    params['source'] = source

            # Price handling
            for price_type in ['max_price', 'min_price']:
                if price_type in user_prefs:
                    try:
                        price = float(user_prefs[price_type])
                        params[price_type] = price
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {price_type}: {user_prefs[price_type]}")

            # Bedrooms handling
            if 'bedrooms' in user_prefs:
                try:
                    bedrooms = int(user_prefs['bedrooms'])
                    params['bedrooms'] = bedrooms
                except (ValueError, TypeError):
                    logger.warning(f"Invalid bedrooms: {user_prefs['bedrooms']}")

            # Property type handling
            if 'property_type' in user_prefs:
                params['property_type'] = user_prefs['property_type']

            # BER rating handling
            if 'ber_rating' in user_prefs:
                params['ber_rating'] = user_prefs['ber_rating']

            try:
                logger.info(f"Fetching properties with params: {params}")
                async with session.get(
                    f"{self.api_base_url}/api/rentals/search",
                    params=params,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Found {len(data)} properties")
                        return data
                    else:
                        logger.error(f"API request failed with status {response.status}")
                        return []
            except asyncio.TimeoutError:
                logger.error("API request timed out")
                return []
            except Exception as e:
                logger.error(f"Error fetching properties: {str(e)}")
                return []

    def matches_location_preferences(self, property: dict, locations: List[str]) -> bool:
        """Check if property matches location preferences"""
        if not locations:
            return True
            
        address = self.format_address(property).lower()
        return any(loc.lower() in address for loc in locations)

    async def format_message(self, properties: list) -> List[Tuple[str, Optional[str]]]:
        """Format properties into messages with images and robust null checks"""
        if not properties:
            return [("No matching properties found at this time. I'll keep searching! 🔍", None)]
        
        messages = []
        for idx, property in enumerate(properties[:5]):  # Limit to 5 properties
            try:
                # Skip if property is None or not a dict
                if not property or not isinstance(property, dict):
                    logger.warning(f"Invalid property at index {idx}")
                    continue

                message_parts = []
                
                # Get image URL with null check
                image_url = None
                try:
                    image_url = self.get_main_image(property)
                except Exception as e:
                    logger.debug(f"Error getting image URL: {e}")

                # Get address with null check
                address = "Address not available"
                try:
                    if isinstance(property.get('address'), dict):
                        address = property['address'].get('display_address', 'Address not available')
                    elif property.get('address'):
                        address = str(property['address'])
                except Exception as e:
                    logger.debug(f"Error getting address: {e}")

                # Get property URL with null check
                property_url = None
                try:
                    property_url = self.get_property_url(property)
                except Exception as e:
                    logger.debug(f"Error getting property URL: {e}")

                # Add address (with or without link)
                if property_url:
                    message_parts.append(f"📍 <a href='{property_url}'>{address}</a>")
                else:
                    message_parts.append(f"📍 {address}")
                
                # Price with null check
                try:
                    price = property.get('price', {})
                    if isinstance(price, dict) and price.get('amount') is not None:
                        message_parts.append(f"💰 €{float(price['amount']):,.2f}")
                        if price.get('frequency'):
                            message_parts.append(f"   per {price['frequency']}")
                except Exception as e:
                    logger.debug(f"Error formatting price: {e}")
                
                # Property details with null checks
                details = []
                try:
                    if property.get('bedrooms') is not None:
                        details.append(f"🛏️ {property['bedrooms']} bed(s)")
                    if property.get('bathrooms') is not None:
                        details.append(f"🚿 {property['bathrooms']} bath(s)")
                    
                    size = property.get('size', {})
                    if isinstance(size, dict) and size.get('value') is not None:
                        details.append(f"📐 {size['value']}{size.get('unit', 'm²')}")
                except Exception as e:
                    logger.debug(f"Error formatting details: {e}")
                
                if details:
                    message_parts.append(" | ".join(details))
                
                # BER rating with null check
                if property.get('ber_rating'):
                    message_parts.append(f"🏷️ BER: {property['ber_rating']}")
                
                # Source with null check
                try:
                    source = property.get('source', '').lower()
                    if source:
                        source_obj = self.sources.get(source)
                        if source_obj:
                            source_name = source_obj.display_name
                            source_url = source_obj.base_url
                            message_parts.append(
                                f"🔍 Source: <a href='{source_url}'>{source_name}</a>"
                            )
                        else:
                            message_parts.append(f"🔍 Source: {source.title()}")
                except Exception as e:
                    logger.debug(f"Error formatting source: {e}")
                
                # Agent information with null check
                try:
                    agent = property.get('agent', {})
                    if isinstance(agent, dict):
                        agent_details = []
                        if agent.get('name'):
                            agent_details.append(agent['name'])
                        if agent.get('phone'):
                            agent_details.append(f"☎️ {agent['phone']}")
                        if agent.get('email'):
                            agent_details.append(f"📧 {agent['email']}")
                        if agent_details:
                            message_parts.append("👤 " + " | ".join(agent_details))
                except Exception as e:
                    logger.debug(f"Error formatting agent info: {e}")
                
                # Only add the property if we have some content
                if len(message_parts) > 1:  # More than just the address
                    message_parts.append("───────────────")
                    messages.append(("\n".join(message_parts), image_url))
                
            except Exception as e:
                logger.error(f"Error formatting property: {str(e)}", exc_info=True)
                continue

        # Add summary message
        if len(properties) > 5:
            messages.append((
                f"\n...and {len(properties) - 5} more properties\n"
                "Use /settings to adjust your search preferences.",
                None
            ))
        
        return messages or [("No valid properties to display at this time.", None)]


    async def process_updates(self, bot, db_manager) -> None:
        """Process updates and send alerts to users"""
        active_users = db_manager.get_all_active_users()
        
        for user_id in active_users:
            try:
                preferences = db_manager.get_preferences(user_id)
                if not preferences:
                    continue

                properties = await self.fetch_properties(preferences)
                
                if 'locations' in preferences and preferences['locations']:
                    properties = [
                        prop for prop in properties
                        if self.matches_location_preferences(prop, preferences['locations'])
                    ]
                
                if properties:
                    messages = await self.format_message(properties)
                    for message_text, image_url in messages:
                        try:
                            if image_url:
                                # Send image with caption
                                await bot.send_photo(
                                    chat_id=user_id,
                                    photo=image_url,
                                    caption=message_text,
                                    parse_mode='HTML'
                                )
                            else:
                                # Send text only
                                await bot.send_message(
                                    chat_id=user_id,
                                    text=message_text,
                                    parse_mode='HTML',
                                    disable_web_page_preview=False
                                )
                            logger.info(f"Sent alert to user {user_id}")
                        except Exception as e:
                            logger.error(f"Error sending message to user {user_id}: {str(e)}")
                            # If image fails, try sending without image
                            if image_url:
                                try:
                                    await bot.send_message(
                                        chat_id=user_id,
                                        text=message_text,
                                        parse_mode='HTML',
                                        disable_web_page_preview=False
                                    )
                                except Exception as e:
                                    logger.error(f"Error sending fallback message: {str(e)}")

            except Exception as e:
                logger.error(f"Error processing updates for user {user_id}: {str(e)}")

async def start_alert_checker(bot, db_manager, check_interval: int = 60):
    """Start the alert checking loop with proper cancellation handling"""
    alert_manager = PropertyAlertManager()
    logger.info("Starting alert checker...")
    
    try:
        while True:
            try:
                await alert_manager.process_updates(bot, db_manager)
                logger.info(f"Sleeping for {check_interval} seconds...")
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                logger.info("Alert checker cancelled")
                raise
            except Exception as e:
                logger.error(f"Error in alert checker: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying
    except asyncio.CancelledError:
        logger.info("Alert checker shutdown complete")
    finally:
        logger.info("Alert checker cleanup complete")

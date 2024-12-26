import logging
import signal
import asyncio
import sys
from os import getenv
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    KeyboardButton, 
    Message, 
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from alert import start_alert_checker
from storage import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# State machine for setup flow
class SetupStates(StatesGroup):
    waiting_for_price = State()
    waiting_for_min_price = State()
    waiting_for_location = State()
    waiting_for_bedroom = State()
    waiting_for_ber = State()
    waiting_for_property_type = State()
    waiting_for_source = State()

# Load environment variables
load_dotenv(".env")
TOKEN = getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("No token provided!")
    sys.exit(1)

# Initialize dispatcher and database
dp = Dispatcher()
db_manager = DatabaseManager()

# List of authorized user IDs
AUTH_IDS = {6485596222}

def auth(message: Message) -> bool:
    """Check if user is authorized"""
    return message.from_user.id in AUTH_IDS

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """Handle the /start command"""
    if not auth(message):
        await message.answer("Sorry, you're not authorized to use this bot.")
        return

    await message.answer(
        f"Hello, {html.bold(message.from_user.full_name)}!\n"
        "I can help you find properties in Ireland. Use /setup to get started.\n\n"
        "Available commands:\n"
        "/setup - Configure your property search\n"
        "/settings - View current settings\n"
        "/help - Show this help message"
    )

@dp.message(Command("setup"))
async def command_setup_handler(message: Message):
    """Handle the /setup command"""
    if not auth(message):
        return

    kb = [
        [
            KeyboardButton(text="Set Max Price"),
            KeyboardButton(text="Set Min Price")
        ],
        [
            KeyboardButton(text="Set Location"),
            KeyboardButton(text="Set Bedrooms")
        ],
        [
            KeyboardButton(text="Set Property Type"),
            KeyboardButton(text="Set BER Rating")
        ],
        [
            KeyboardButton(text="Set Source"),
            KeyboardButton(text="Done")
        ]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(
        "What would you like to setup?\n\n"
        "You can configure:\n"
        "• Price range\n"
        "• Preferred locations\n"
        "• Number of bedrooms\n"
        "• Property type\n"
        "• BER rating\n"
        "• Property source (Daft.ie, MyHome.ie, etc.)",
        reply_markup=keyboard
    )

@dp.message(Command("settings"))
async def view_settings(message: Message):
    """Handle the /settings command"""
    if not auth(message):
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id)
    
    if not prefs:
        await message.answer(
            "No preferences set yet. Use /setup to configure your search."
        )
        return
    
    settings_text = ["Your current search settings:"]
    
    if prefs.get('min_price'):
        settings_text.append(f"• Minimum price: €{prefs['min_price']:,.2f}")
    if prefs.get('max_price'):
        settings_text.append(f"• Maximum price: €{prefs['max_price']:,.2f}")
    if prefs.get('locations'):
        settings_text.append(f"• Locations: {', '.join(prefs['locations'])}")
    if prefs.get('bedrooms'):
        settings_text.append(f"• Bedrooms: {prefs['bedrooms']}")
    if prefs.get('property_type'):
        settings_text.append(f"• Property type: {prefs['property_type']}")
    if prefs.get('ber_rating'):
        settings_text.append(f"• BER rating: {prefs['ber_rating']}")
    if prefs.get('source'):
        settings_text.append(f"• Source: {prefs['source'].title()}")
        
    settings_text.append("\nUse /setup to modify these settings.")
    
    await message.answer("\n".join(settings_text))

@dp.message(lambda message: message.text == "Set Max Price")
async def set_max_price(message: Message, state: FSMContext):
    """Handle maximum price setting"""
    if not auth(message):
        return

    await message.answer(
        "Please enter your maximum price:\n"
        "Example: 2000 for €2,000 per month\n\n"
        "Or type 'back' to return to setup menu",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Back")]],
            resize_keyboard=True
        )
    )
    await state.set_state(SetupStates.waiting_for_price)

@dp.message(SetupStates.waiting_for_price)
async def process_max_price(message: Message, state: FSMContext):
    """Process maximum price input"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    try:
        price = float(message.text)
        if price <= 0:
            raise ValueError("Price must be positive")

        user_id = message.from_user.id
        prefs = db_manager.get_preferences(user_id) or {}
        prefs['max_price'] = price
        db_manager.save_preferences(user_id, prefs)

        await state.clear()
        await message.answer(
            f"Successfully set maximum price to: €{price:,.2f}\n"
            f"Use /settings to view all your preferences"
        )
        await command_setup_handler(message)
    except ValueError:
        await message.answer(
            "Please enter a valid positive number (e.g., 2000 for €2,000)"
        )

@dp.message(lambda message: message.text == "Set Min Price")
async def set_min_price(message: Message, state: FSMContext):
    """Handle minimum price setting"""
    if not auth(message):
        return

    await message.answer(
        "Please enter your minimum price:\n"
        "Example: 1500 for €1,500 per month\n\n"
        "Or type 'back' to return to setup menu",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Back")]],
            resize_keyboard=True
        )
    )
    await state.set_state(SetupStates.waiting_for_min_price)

@dp.message(SetupStates.waiting_for_min_price)
async def process_min_price(message: Message, state: FSMContext):
    """Process minimum price input"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    try:
        price = float(message.text)
        if price <= 0:
            raise ValueError("Price must be positive")

        user_id = message.from_user.id
        prefs = db_manager.get_preferences(user_id) or {}
        prefs['min_price'] = price
        db_manager.save_preferences(user_id, prefs)

        await state.clear()
        await message.answer(
            f"Successfully set minimum price to: €{price:,.2f}\n"
            f"Use /settings to view all your preferences"
        )
        await command_setup_handler(message)
    except ValueError:
        await message.answer(
            "Please enter a valid positive number (e.g., 1500 for €1,500)"
        )

@dp.message(lambda message: message.text == "Set Bedrooms")
async def set_bedrooms(message: Message, state: FSMContext):
    """Handle bedroom setting"""
    if not auth(message):
        return

    await message.answer(
        "Please enter the number of bedrooms you're looking for:\n"
        "Example: 2 for a two-bedroom property\n\n"
        "Or type 'back' to return to setup menu",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Back")]],
            resize_keyboard=True
        )
    )
    await state.set_state(SetupStates.waiting_for_bedroom)

@dp.message(SetupStates.waiting_for_bedroom)
async def process_bedrooms(message: Message, state: FSMContext):
    """Process bedrooms input"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    try:
        bedrooms = int(message.text)
        if bedrooms <= 0:
            raise ValueError("Number of bedrooms must be positive")

        user_id = message.from_user.id
        prefs = db_manager.get_preferences(user_id) or {}
        prefs['bedrooms'] = bedrooms
        db_manager.save_preferences(user_id, prefs)

        await state.clear()
        await message.answer(
            f"Successfully set bedrooms to: {bedrooms}\n"
            f"Use /settings to view all your preferences"
        )
        await command_setup_handler(message)
    except ValueError:
        await message.answer(
            "Please enter a valid number of bedrooms (e.g., 2)"
        )

@dp.message(lambda message: message.text == "Set Property Type")
async def set_property_type(message: Message, state: FSMContext):
    """Handle property type setting"""
    kb = [
        [
            KeyboardButton(text="House"),
            KeyboardButton(text="Apartment")
        ],
        [
            KeyboardButton(text="Studio"),
            KeyboardButton(text="Flat")
        ],
        [
            KeyboardButton(text="Back")
        ]
    ]
    await message.answer(
        "Please select the type of property you're looking for:",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )
    await state.set_state(SetupStates.waiting_for_property_type)

@dp.message(SetupStates.waiting_for_property_type)
async def process_property_type(message: Message, state: FSMContext):
    """Process property type selection"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    valid_types = ['house', 'apartment', 'studio', 'flat']
    if message.text.lower() not in valid_types:
        await message.answer("Please select a valid property type from the keyboard options")
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id) or {}
    prefs['property_type'] = message.text.lower()
    db_manager.save_preferences(user_id, prefs)

    await state.clear()
    await message.answer(f"Successfully set property type to: {message.text}")
    await command_setup_handler(message)

@dp.message(lambda message: message.text == "Set BER Rating")
async def set_ber_rating(message: Message, state: FSMContext):
    """Handle BER rating setting"""
    kb = [
        [
            KeyboardButton(text="A1"), KeyboardButton(text="A2"), KeyboardButton(text="A3")
        ],
        [
            KeyboardButton(text="B1"), KeyboardButton(text="B2"), KeyboardButton(text="B3")
        ],
        [
            KeyboardButton(text="C1"), KeyboardButton(text="C2"), KeyboardButton(text="C3")
        ],
        [
            KeyboardButton(text="Back")
        ]
    ]
    await message.answer(
        "Please select the minimum BER rating you're looking for:",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )
    await state.set_state(SetupStates.waiting_for_ber)

@dp.message(SetupStates.waiting_for_ber)
async def process_ber_rating(message: Message, state: FSMContext):
    """Process BER rating selection"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    valid_ratings = [
        'A1', 'A2', 'A3', 'B1', 'B2', 'B3', 
        'C1', 'C2', 'C3', 'D1', 'D2', 'E1', 'E2', 'F', 'G'
    ]
    
    if message.text.upper() not in valid_ratings:
        await message.answer("Please select a valid BER rating from the keyboard options")
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id) or {}
    prefs['ber_rating'] = message.text.upper()
    db_manager.save_preferences(user_id, prefs)

    await state.clear()
    await message.answer(f"Successfully set minimum BER rating to: {message.text.upper()}")
    await command_setup_handler(message)

@dp.message(lambda message: message.text == "Set Source")
async def set_source(message: Message, state: FSMContext):
    """Handle source selection"""
    kb = [
        [
            KeyboardButton(text="Daft.ie"),
            KeyboardButton(text="MyHome.ie"),
            KeyboardButton(text="Property.ie")
        ],
        [KeyboardButton(text="All Sources")],
        [KeyboardButton(text="Back")]
    ]
    await message.answer(
        "Select your preferred property source:\n\n"
        "• Daft.ie - Ireland's largest property site\n"
        "• MyHome.ie - Comprehensive property portal\n"
        "• Property.ie - Additional listings\n"
        "• All Sources - Search everywhere",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )
    await state.set_state(SetupStates.waiting_for_source)

@dp.message(SetupStates.waiting_for_source)
async def process_source(message: Message, state: FSMContext):
    """Process source selection"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    source_mapping = {
        'daft.ie': 'daft',
        'myhome.ie': 'myhome',
        'property.ie': 'property',
        'all sources': None
    }

    selected_source = message.text.lower()
    source_id = source_mapping.get(selected_source)

    if source_id is None and selected_source != 'all sources':
        await message.answer(
            "Please select a valid source from the keyboard options.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Back")]],
                resize_keyboard=True
            )
        )
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id) or {}

    if source_id:
        prefs['source'] = source_id
        await message.answer(f"Successfully set source to: {message.text}")
    else:
        prefs.pop('source', None)
        await message.answer("Will search across all available sources")

    db_manager.save_preferences(user_id, prefs)
    await state.clear()
    await command_setup_handler(message)

@dp.message(lambda message: message.text == "Set Location")
async def set_location(message: Message, state: FSMContext):
    """Handle location setting"""
    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id) or {}
    current_locations = prefs.get('locations', [])
    
    locations_text = "Current locations: " + ", ".join(current_locations) if current_locations else "No locations set"
    
    await message.answer(
        f"{locations_text}\n\n"
        "Please enter a location to add to your search:\n"
        "Example: Dublin 2, Rathmines, etc.\n\n"
        "Or type 'done' when finished\n"
        "Type 'clear' to remove all locations\n"
        "Type 'back' to return to setup menu",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Done")],
                [KeyboardButton(text="Clear")],
                [KeyboardButton(text="Back")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(SetupStates.waiting_for_location)

@dp.message(SetupStates.waiting_for_location)
async def process_location(message: Message, state: FSMContext):
    """Process location input"""
    if message.text.lower() in ['back', '/back']:
        await state.clear()
        await command_setup_handler(message)
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id) or {}
    current_locations = prefs.get('locations', [])

    if message.text.lower() == 'done':
        if not current_locations:
            await message.answer("Please add at least one location before finishing")
            return
        await state.clear()
        await message.answer(f"Locations saved: {', '.join(current_locations)}")
        await command_setup_handler(message)
        return

    if message.text.lower() == 'clear':
        prefs['locations'] = []
        db_manager.save_preferences(user_id, prefs)
        await message.answer("All locations cleared")
        return

    location = message.text.strip()
    if location in current_locations:
        await message.answer(f"Location '{location}' is already in your list")
        return

    current_locations.append(location)
    prefs['locations'] = current_locations
    db_manager.save_preferences(user_id, prefs)
    await message.answer(
        f"Added location: {location}\n"
        f"Current locations: {', '.join(current_locations)}\n\n"
        "Add another location or type 'done' when finished"
    )

@dp.message(lambda message: message.text == "Done")
async def handle_done(message: Message):
    """Handle completion of setup"""
    if not auth(message):
        return

    user_id = message.from_user.id
    prefs = db_manager.get_preferences(user_id)
    
    if not prefs:
        await message.answer(
            "No preferences set yet. Please set at least one preference.",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    await message.answer(
        "Setup complete! I'll start searching for properties matching your preferences.\n"
        "You'll receive notifications when matching properties are found.\n\n"
        "Use /settings to view or modify your preferences.",
        reply_markup=ReplyKeyboardRemove()
    )

async def shutdown(signal_type, bot=None):
    """Handle graceful shutdown"""
    logger.info(f"Received exit signal {signal_type}")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} tasks")
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("All tasks cancelled")
        if bot:
            await bot.session.close()
            logger.info("Bot session closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

async def main() -> None:
    """Main function to run the bot"""
    bot = None
    try:
        bot = Bot(
            token=TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        
        polling_task = dp.start_polling(bot)
        alert_task = start_alert_checker(bot, db_manager)
        
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(s, bot))
            )
        
        await asyncio.gather(polling_task, alert_task)
        
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        if bot:
            try:
                await bot.session.close()
                logger.info("Bot session closed")
            except Exception as e:
                logger.error(f"Error closing bot session: {str(e)}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped due to error: {str(e)}")

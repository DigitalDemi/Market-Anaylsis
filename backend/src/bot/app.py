import logging
import asyncio
import sys

from os import getenv
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, audio
from dotenv import load_dotenv
from aiogram.fsm.state import State,  StatesGroup
from aiogram.fsm.context import FSMContext
from alert import start_alert_checker


class SetupSates(StatesGroup):
    waiting_for_price = State()
    waiting_for_location = State()
    waiting_for_bedroom = State()
    waiting_for_ber = State()
    waiting_for_property_type = State()
    waiting_for_source = State()


load_dotenv(".env")

dp = Dispatcher()

auth_id = {
        6485596222
        }

user_store = {}
location = []


TOKEN = getenv("TELEGRAM_BOT_TOKEN")


def auth(message: Message) -> bool:
    return message.from_user.id in auth_id


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    This handler receives messages with /start command
    """
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message(Command("setup"))
async def command_setup_handler(message: Message):
    kb = [
    [KeyboardButton(text="Set Max Price"), KeyboardButton(text="Set Min Price")],
    [KeyboardButton(text="Set Location"), KeyboardButton(text="Set Min Bedroom")],
    [KeyboardButton(text="Set Property Type"), KeyboardButton(text="Set BER Rating")],
    [KeyboardButton(text="Set Source"), KeyboardButton(text="Done")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("What would you like to setup?", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Set Max Price")
async def set_max_price_handler(message: Message, state: FSMContext):
    await message.answer(
            "Please enter your maximum price:\n"
            "Or type 'back' to return to setup menu",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Back")]],
                resize_keyboard=True
                )
            )
    await state.set_state(SetupSates.waiting_for_price)

@dp.message(SetupSates.waiting_for_price)
async def process_price_handler(message: Message, state: FSMContext):
    if message.text.lower() in ['back', '/back', 'cancel', '/cancel']:
        await state.clear()
        await command_setup_handler(message, state)
        return

    try:
        price = float(message.text)
        user_id = message.from_user.id

        if user_id not in user_store:
            user_store[user_id] = {}

        user_store[user_id]["max_price"] = price
        await state.clear()

        await message.answer(f"Successfully set up monitoring for maximum price: €{price:.2f}")

    except ValueError:
        await message.answer(
                "Please enter a valid number (e.g., 1000)\n"
                "Or type 'back' to return to the setup menu"
                )


@dp.message(lambda message: message.text == "Set Min Bedroom")
async def set_min_bedrooms(message: Message, state: FSMContext):
    await message.answer(
            "Please enter the minium ammount of bed rooms you want in a house:\n"
            "Or type 'back' to return to setup menu",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Back")]],
                resize_keyboard=True
                )
            )
    await state.set_state(SetupSates.waiting_for_bedroom)

@dp.message(SetupSates.waiting_for_bedroom)
async def process_number_bedrooms(message: Message, state: FSMContext):
    if message.text.lower() in ['back', '/back', 'cancel', '/cancel']:
        await state.clear()
        await command_setup_handler(message, state)
        return

    try:
        amount = int(message.text)
        user_id = message.from_user.id

        if user_id not in user_store:
            user_store[user_id] = {}

        user_store[user_id]["amount"] = amount
        await state.clear()

        await message.answer(f"Successfully set up monitoring for minium bedrooms: {amount}")

    except ValueError:
        await message.answer(
                "Please enter a valid number (e.g., 1)\n"
                "Or type 'back' to return to the setup menu"
                )


@dp.message(lambda message: message.text == "Set Location")
async def set_locations(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current_locations = user_store.get(user_id, {}).get("locations", [])
    await message.answer(
            "Enter all the locations you want to look for hosuing or rooms:\n"
            "Or type 'back' to return to setup menu",
            )
    await state.update_data(locations=current_locations)
    await state.set_state(SetupSates.waiting_for_location)

@dp.message(SetupSates.waiting_for_location)
async def process_location(message: Message, state: FSMContext):
    if message.text.lower() in ['back', '/back', 'cancel', '/cancel']:
        await state.clear()
        await command_setup_handler(message)
        return

    if message.text.lower() == 'done':
        data = await state.get_data()
        locations = data.get("locations", [])

        if not locations:
            await message.answer("Please add at least one location before finishing.")
            return

        user_id = message.from_user.id
        if user_id not in user_store:
            user_store[user_id] = {}

        user_store[user_id]["locations"] = locations  
        await state.clear()

        await message.answer(f"Successfully saved locations: {', '.join(locations)}")
        await command_setup_handler(message)
        return

    new_location = message.text.strip().title()

    data = await state.get_data()
    locations = data.get("locations", [])

    if new_location in locations:
        await message.answer(
                f"Location '{new_location}' is already in your list.\n"
                f"Current locations: {', '.join(locations)}"
                )
        return

    locations.append(new_location)  # Append to maintain order
    await state.update_data(locations=locations)

    await message.answer(
            f"Added location: {new_location}\n"
            f"Current locations: {', '.join(locations)}\n\n"
            "Enter another location or type 'done' when finished"
            )


@dp.message(lambda message: message.text == "Done")
async def set_locations(message: Message):
    user_id = message.from_user.id
    await message.answer(
            "Ok everything is setup you will altered when houses go on and off market and whats in your range with links"
            )

@dp.message(lambda message: message.text == "Set Property Type")
async def set_property_type(message: Message, state: FSMContext):
    await message.answer(
        "Enter the property type you're looking for.\n\n"
        "Common types include:\n"
        "- House\n"
        "- Apartment\n"
        "- Studio\n"
        "- Flat\n"
        "- Duplex\n"
        "- Penthouse\n\n"
        "Or type 'back' to return",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Back")]], resize_keyboard=True)
    )
    await state.set_state(SetupSates.waiting_for_property_type)

@dp.message(lambda message: message.text == "Set BER Rating")
async def set_ber_rating(message: Message, state: FSMContext):
    await message.answer(
        "Enter the BER (Building Energy Rating) you're looking for.\n\n"
        "Ratings range from:\n"
        "A1-A3 (Most efficient)\n"
        "B1-B3\n"
        "C1-C3\n"
        "D1-D2\n"
        "E1-E2\n"
        "F\n"
        "G (Least efficient)\n\n"
        "Or type 'back' to return",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Back")]], resize_keyboard=True)
    )
    await state.set_state(SetupSates.waiting_for_ber)

@dp.message(lambda message: message.text == "Set Source")
async def set_source(message: Message, state: FSMContext):
    kb = [
        [KeyboardButton(text="Daft"), KeyboardButton(text="MyHome")],
        [KeyboardButton(text="Property.ie"), KeyboardButton(text="All")],
        [KeyboardButton(text="Back")]
    ]
    await message.answer(
        "Select where to search from:\n\n"
        "- Daft.ie: Largest Irish property site\n"
        "- MyHome.ie: Second largest property portal\n"
        "- Property.ie: Additional listings\n"
        "- All: Search across all sources",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )
    await state.set_state(SetupSates.waiting_for_source)

async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await asyncio.gather(
            dp.start_polling(bot),
            start_alert_checker(bot,user_store)


    )
    await dp.start_polling(bot)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())

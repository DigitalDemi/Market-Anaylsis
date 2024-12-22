import logging
import asyncio
import sys

from os import getenv
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from dotenv import load_dotenv


load_dotenv(".env")

dp = Dispatcher()

auth_id = {
    298564435
}


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
    kb = [[
        KeyboardButton(text="Set Max Price"),
        KeyboardButton(text="Set Location")
    ],
    [
        KeyboardButton(text="Set Min Bedroom"),
        KeyboardButton(text="Done")
    ]
    ]
    keyboard = ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True
    )
    await message.answer("What would you like to setup?", reply_markup=keyboard)


async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())

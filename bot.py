# © 2025 Kaustav Ray. All rights reserved.
# Licensed under the MIT License.

import logging
import asyncio
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError
from fuzzywuzzy import fuzz
import math
import random
import re
import io
import time
import httpx
import uuid
import datetime
import html
from flask import Flask
from threading import Thread
import os
import sys
from functools import lru_cache
from werkzeug.serving import make_server

# ========================
# CONFIG
# ========================
BOT_TOKEN = "7657898593:AAEqWdlNE9bAVikWAnHRYyQyj0BCXy6qUmc"  # Bot Token
DB_CHANNEL = -1002975831610  # Database channel
LOG_CHANNEL = -1002988891392  # Channel to log user queries
# Channels users must join for access
JOIN_CHECK_CHANNEL = [-1002692055617, -1002551875503, -1002839913869]
ADMINS = [6705618257]        # Admin IDs
PM_SEARCH_ENABLED = True   # Controls whether non-admins can search in PM

# Custom promotional message (Simplified as per the last request)
REACTIONS = ["👀", "😱", "🔥", "😍", "🎉", "🥰", "😇", "⚡"]
PROMO_CHANNELS = [
    {"name": "@filestore4u", "link": "https://t.me/filestore4u", "id": -1002692055617},
    {"name": "@code_boost", "link": "https://t.me/code_boost", "id": -1002551875503},
    {"name": "@KRBOOK_official", "link": "https://t.me/KRBOOK_official", "id": -1002839913869},
]
CUSTOM_PROMO_MESSAGE = (
    "Credit to Prince Kaustav Ray\n\n"
    "Join our main channel: @filestore4u\n"
    "Join our channel: @code_boost\n"
    "Join our channel: @krbook_official"
)

HELP_TEXT = (
    "**Here is a list of available commands:**\n\n"
    "**User Commands:**\n"
    "• `/start` - Start the bot.\n"
    "• `/help` - Show this help message.\n"
    "• `/info` - Get bot information.\n"
    "• `/refer` - Get your referral link to earn premium access.\n"
    "• `/request <name>` - Request a file.\n"
    "• `/request_index` - Request a file or channel to be indexed.\n"
    "• Send any text to search for a file (admins only in private chat).\n\n"
    "**Admin Commands:**\n"
    "• `/log` - Show recent error logs.\n"
    "• `/total_users` - Get the total number of users.\n"
    "• `/total_files` - Get the total number of files in the current DB.\n"
    "• `/stats` - Get bot and database statistics.\n"
    "• `/findfile <name>` - Find a file's ID by name.\n"
    "• `/deletefile <id>` - Delete a file from the database.\n"
    "• `/deleteall` - Delete all files from the current database.\n"
    "• `/ban <user_id>` - Ban a user.\n"
    "• `/unban <user_id>` - Unban a user.\n"
    "• `/freeforall` - Grant 12-hour premium access to all users.\n"
    "• `/broadcast <msg>` - Send a message to all users.\n"
    "• `/grp_broadcast <msg>` - Send a message to all connected groups where the bot is an admin.\n"
    "• `/index_channel <channel_id> [skip]` - Index files from a channel.\n"
    "• Send a file to me in a private message to index it."
)

# A list of MongoDB URIs to use. Add as many as you need.
MONGO_URIS = [
    "mongodb+srv://bf44tb5_db_user:RhyeHAHsTJeuBPNg@cluster0.lgao3zu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://28c2kqa_db_user:IL51mem7W6g37mA5@cluster0.np0ffl0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
]
GROUPS_DB_URIS = ["mongodb+srv://6p5e2y8_db_user:MxRFLhQ534AI3rfQ@cluster0.j9hcylx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
REFERRAL_DB_URI = "mongodb+srv://qy8gjiw_db_user:JjryWhQV4CYtzcYo@cluster0.lkkvli8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
current_uri_index = 0

# Centralized connection manager
mongo_clients = {} # Will store MongoClient instances for each URI

# Pointers to the collections of the *currently active* database
db = None
files_col = None
users_col = None
banned_users_col = None
groups_col = None
referrals_col = None
referred_users_col = None


# In-memory caches for performance
banned_user_cache = {}    # {user_id: bool}


# Logging setup with an in-memory buffer for the /log command
log_stream = io.StringIO()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO, stream=log_stream
)
logger = logging.getLogger(__name__)

# Flask web server for Render health checks
app = Flask(__name__)

@app.route('/')
def home():
    """A simple route to confirm the web server is running."""
    return "Bot is alive and running!"


# ========================
# HELPERS
# ========================

async def get_random_file_from_db():
    """
    Fetches a single random file document from any of the available file databases
    using the centralized connection pool.
    """
    # Shuffle URIs to distribute the load for random queries
    shuffled_uris = random.sample(MONGO_URIS, len(MONGO_URIS))

    for uri in shuffled_uris:
        client = mongo_clients.get(uri)
        if not client:
            logger.warning(f"Skipping disconnected DB for random file fetch: ...{uri[-20:]}")
            continue

        try:
            db = client["telegram_files"]
            files_col = db["files"]
            # Use $sample for efficient random document retrieval
            pipeline = [{"$sample": {"size": 1}}]
            result = list(files_col.aggregate(pipeline))
            if result:
                return result[0]  # Return the first document found
        except Exception as e:
            logger.error(f"DB Error while fetching random file from ...{uri[-20:]}: {e}")
            continue # Try the next URI

    return None # Return None if no file is found in any DB

def escape_markdown(text: str) -> str:
    """Helper function to escape special characters in Markdown V2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return "".join('\\' + char if char in escape_chars else char for char in text)

def format_size(size_in_bytes: int) -> str:
    """Converts a size in bytes to a human-readable format."""
    if size_in_bytes is None:
        return "N/A"

    if size_in_bytes == 0:
        return "0 B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_in_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_in_bytes / p, 2)
    return f"{s} {size_name[i]}"


def format_filename_for_display(filename: str) -> str:
    """Splits a long filename into two lines for better display."""
    if len(filename) < 40:
        return filename

    mid = len(filename) // 2
    split_point = -1

    # Try to find a space near the midpoint
    for i in range(mid, 0, -1):
        if filename[i] == ' ':
            split_point = i
            break

    if split_point == -1:
        for i in range(mid, len(filename)):
            if filename[i] == ' ':
                split_point = i
                break

    if split_point != -1:
        return filename[:split_point] + '\n' + filename[split_point+1:]
    else:
        # Fallback if no space is found (e.g., a single long word)
        return filename[:mid] + '\n' + filename[mid:]

async def check_member_status(user_id, context: ContextTypes.DEFAULT_TYPE):
    """Check if the user is a member of ALL required promotional channels."""
    for channel in PROMO_CHANNELS:
        try:
            member = await context.bot.get_chat_member(chat_id=channel['id'], user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except TelegramError as e:
            logger.error(f"Error checking member status for user {user_id} in channel {channel['id']}: {e}")
            return False # If we can't check one, we assume they are not a member.
    return True

async def is_banned(user_id):
    """Check if the user is banned, with in-memory caching."""
    # 1. Check cache first
    if user_id in banned_user_cache:
        return banned_user_cache[user_id]

    # 2. If not in cache, check DB
    if banned_users_col is not None:
        is_banned_status = banned_users_col.find_one({"_id": user_id}) is not None
        # 3. Store result in cache
        banned_user_cache[user_id] = is_banned_status
        return is_banned_status

    # Default to not banned if DB is unavailable
    return False

async def bot_can_respond(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if the bot should respond in a group chat.
    - Allows all private chats.
    - In groups, responds only if the bot is an administrator.
    """
    chat = update.effective_chat

    if chat.type == "private":
        return True

    if chat.type in ["group", "supergroup"]:
        try:
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status in ["administrator", "creator"]:
                return True
            else:
                logger.info(f"Bot is not an admin in group {chat.id}, ignoring message.")
                return False
        except TelegramError as e:
            logger.error(f"Could not check bot status in group {chat.id}: {e}")
            return False

    return False



async def send_and_delete_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup=None,
    parse_mode=None,
    reply_to_message_id=None
):
    """Sends a message and schedules its deletion after 5 minutes."""
    try:
        if reply_to_message_id:
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id
            )
        else:
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )

        # Schedule deletion
        deletion_task = asyncio.create_task(delete_message_after_delay(context, chat_id, sent_message.message_id, 5 * 60))
        return sent_message, deletion_task
    except TelegramError as e:
        logger.error(f"Error in send_and_delete_message to chat {chat_id}: {e}")
        return None, None

async def delete_message_after_delay(context, chat_id, message_id, delay):
    """Awaits a delay and then deletes a message."""
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Auto-deleted message {message_id} from chat {chat_id}.")
    except TelegramError as e:
        logger.warning(f"Failed to auto-delete message {message_id} from chat {chat_id}: {e}")


def connect_to_mongo():
    """
    Initializes connection pools for all database URIs specified in the config.
    It also sets the initial active database connection.
    """
    global mongo_clients, db, files_col, users_col, banned_users_col, groups_col, referrals_col, referred_users_col, current_uri_index

    # Consolidate all unique URIs
    all_uris = set(MONGO_URIS + GROUPS_DB_URIS)
    if REFERRAL_DB_URI:
        all_uris.add(REFERRAL_DB_URI)

    for uri in all_uris:
        try:
            # Create a client with connection pooling
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # The ismaster command is cheap and forces the client to check the connection.
            client.admin.command('ismaster')
            mongo_clients[uri] = client
            logger.info(f"Successfully created connection pool for ...{uri[-20:]}")
        except PyMongoError as e:
            logger.critical(f"FATAL: Could not connect to MongoDB at {uri}. Error: {e}")
            mongo_clients[uri] = None # Mark as failed

    # Set the initial active database for file operations
    initial_uri = MONGO_URIS[current_uri_index]
    initial_client = mongo_clients.get(initial_uri)

    if initial_client:
        db = initial_client["telegram_files"]
        files_col = db["files"]
        users_col = db["users"]
        banned_users_col = db["banned_users"]
        # groups_col is managed separately as it's in a different database

        # Connect to the referral database
        if REFERRAL_DB_URI:
            referral_client = mongo_clients.get(REFERRAL_DB_URI)
            if referral_client:
                referral_db = referral_client["referral_db"]
                referrals_col = referral_db["referrals"]
                referred_users_col = referral_db["referred_users"]
                logger.info("Successfully connected to Referral MongoDB.")
            else:
                logger.critical("Failed to connect to the Referral MongoDB URI. Referral system will not function.")
        else:
            logger.warning("REFERRAL_DB_URI not set. Referral system will be disabled.")

        logger.info(f"Successfully connected to initial MongoDB at index {current_uri_index}.")
        return True
    else:
        logger.critical(f"Failed to connect to the initial MongoDB URI at index {current_uri_index}. Bot may not function correctly.")
        return False

async def save_user_info(user: Update.effective_user):
    """Saves user information to the database if not already present."""
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": user.id},
                {
                    "$set": {
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                        "username": user.username,
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving user info for {user.id}: {e}")


# ========================
# TASK FUNCTIONS (FOR BACKGROUND EXECUTION)
# ========================

async def react_to_message_task(update: Update):
    """Background task to react to a message without blocking."""
    try:
        message = update.effective_message
        if message:
            await message.react(reaction=random.choice(REACTIONS))
    except TelegramError as e:
        logger.warning(f"Could not react to message: {e}")


async def send_file_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_data: dict, user_mention: str):
    """Background task to send a single file to the user's private chat and auto-delete it."""
    try:
        caption_text = file_data.get("file_name", "").strip()
        if not caption_text:
            caption_text = "Download File"

        final_caption_text = caption_text
        new_caption = ""
        while True:
            temp_caption = f'<a href="https://t.me/filestore4u">{html.escape(final_caption_text)}</a>'
            if len(temp_caption.encode('utf-8')) <= 1024:
                new_caption = temp_caption
                break
            final_caption_text = final_caption_text[:-1]
            if not final_caption_text:
                new_caption = '<a href="https://t.me/filestore4u">Download File</a>'
                break

        logger.info(f"Attempting to send file with caption: {new_caption}")
        sent_message = await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=file_data["channel_id"],
            message_id=file_data["file_id"],
            caption=new_caption,
            parse_mode="HTML"
        )

        if sent_message:
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            confirmation_text = f"✅ {user_mention}, I have sent the file to you in a private message. It will be deleted automatically in 5 minutes."
            await send_and_delete_message(context, source_chat_id, confirmation_text, parse_mode="HTML")

            await asyncio.sleep(5 * 60)
            await context.bot.delete_message(chat_id=user_id, message_id=sent_message.message_id)
            logger.info(f"Deleted message {sent_message.message_id} from chat {user_id}.")

    except TelegramError as e:
        if "Forbidden: bot can't initiate conversation with a user" in str(e):
             await send_and_delete_message(context, source_chat_id, f"❌ {user_mention}, I can't send you the file because you haven't started a private chat with me. Please start the bot privately and try again.", parse_mode="HTML")
        else:
            logger.error(f"Failed to send file to user {user_id}: {e}")
            await send_and_delete_message(context, source_chat_id, "❌ File not found or could not be sent.")
    except Exception:
        logger.exception(f"An unexpected error occurred in send_file_task for user {user_id}")
        await send_and_delete_message(context, source_chat_id, "❌ An unexpected error occurred. Please try again later.")


async def send_all_files_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_list: list, user_mention: str):
    """Background task to send multiple files to the user's private chat and auto-delete them."""
    sent_messages = []
    try:
        for file in file_list:
            caption_text = file.get("file_name", "").strip()
            if not caption_text:
                caption_text = "Download File"

            final_caption_text = caption_text
            new_caption = ""
            while True:
                temp_caption = f'<a href="https://t.me/filestore4u">{html.escape(final_caption_text)}</a>'
                if len(temp_caption.encode('utf-8')) <= 1024:
                    new_caption = temp_caption
                    break
                final_caption_text = final_caption_text[:-1]
                if not final_caption_text:
                    new_caption = '<a href="https://t.me/filestore4u">Download File</a>'
                    break

            logger.info(f"Attempting to send file in batch with caption: {new_caption}")
            sent_message = await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=file["channel_id"],
                message_id=file["file_id"],
                caption=new_caption,
                parse_mode="HTML"
            )
            sent_messages.append(sent_message.message_id)
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            await asyncio.sleep(0.5)

        confirmation_text = f"✅ {user_mention}, I have sent all files to you in a private message. They will be deleted automatically in 5 minutes."
        await send_and_delete_message(
            context,
            source_chat_id,
            confirmation_text,
            parse_mode="HTML"
        )

        await asyncio.sleep(5 * 60)
        for message_id in sent_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                logger.info(f"Deleted message {message_id} from chat {user_id}.")
            except TelegramError as e:
                logger.warning(f"Failed to delete message {message_id} for user {user_id}: {e}")

    except TelegramError as e:
        if "Forbidden: bot can't initiate conversation with a user" in str(e):
             await send_and_delete_message(context, source_chat_id, f"❌ {user_mention}, I can't send you the files because you haven't started a private chat with me. Please start the bot privately and try again.", parse_mode="HTML")
        else:
            logger.error(f"Failed to send one or more files to user {user_id}: {e}")
            await send_and_delete_message(context, source_chat_id, "❌ One or more files could not be sent.")
    except Exception:
        logger.exception(f"An unexpected error occurred in send_all_files_task for user {user_id}")
        await send_and_delete_message(context, source_chat_id, "❌ An unexpected error occurred. Please try again later.")

# ========================
# COMMAND HANDLERS
# ========================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command, including verification and referral deep links."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    user = update.effective_user

    # Handle deep links
    if context.args:
        payload = context.args[0]

        # 1. Referral Link Handling
        if payload.startswith("ref_"):
            try:
                referrer_id = int(payload.split("_", 1)[1])

                # Check if the user has already been referred
                is_already_referred = referred_users_col is not None and referred_users_col.find_one({"_id": user.id}) is not None

                if referrer_id != user.id and not is_already_referred:
                    if referrals_col is not None and referred_users_col is not None:
                        # Increment referrer's count
                        referrals_col.update_one(
                            {"_id": referrer_id},
                            {"$inc": {"referral_count": 1}},
                            upsert=True
                        )

                        # Mark the user as referred
                        referred_users_col.insert_one({"_id": user.id})

                        # Check if they hit the target
                        referrer_data = referrals_col.find_one({"_id": referrer_id})
                        if referrer_data and referrer_data.get("referral_count", 0) >= 10:
                            # Grant premium and reset count
                            referrals_col.update_one(
                                {"_id": referrer_id},
                                {"$set": {
                                    "premium_until": datetime.datetime.utcnow() + datetime.timedelta(days=30),
                                    "referral_count": 0
                                }}
                            )
                            # Notify referrer
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text="🎉 **Congratulations!**\n\nYou've successfully referred 10 users and earned **1 month of premium access**! Enjoy the bot without ads or verification."
                                )
                            except TelegramError as e:
                                logger.warning(f"Could not notify referrer {referrer_id} about premium status: {e}")
            except (IndexError, ValueError) as e:
                logger.error(f"Could not parse referral link payload: {payload} - {e}")
            # The new user will fall through to the standard welcome message.

        # 2. Verification Link Handling (REMOVED)
        pass

    # Save user info now. If they were referred, they are now marked as "existing".
    await save_user_info(user)

    # Standard start message if no deep link or after referral processing
    bot_username = context.bot.username
    owner_id = ADMINS[0] if ADMINS else None

    welcome_text = (
        f"<b>Hey, {user.mention_html()}!</b>\n\n"
        "This is an advanced and powerful filter bot.\n\n"
        "<b><u>Your Details:</u></b>\n"
        f"<b>First Name:</b> {user.first_name}\n"
        f"<b>Last Name:</b> {user.last_name or 'N/A'}\n"
        f"<b>User ID:</b> <code>{user.id}</code>\n"
        f"<b>Username:</b> @{user.username or 'N/A'}"
    )

    keyboard = [
        [
            InlineKeyboardButton("About Bot", callback_data="start_about"),
            InlineKeyboardButton("Help", callback_data="start_help")
        ],
        [
            InlineKeyboardButton("➕ Add Me To Your Group ➕", url=f"https://t.me/{bot_username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("Owner", url=f"tg://user?id={owner_id}") if owner_id else InlineKeyboardButton("Owner", callback_data="no_owner")
        ],
        [
            InlineKeyboardButton("Close", callback_data="start_close")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_and_delete_message(
        context,
        update.effective_chat.id,
        welcome_text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the help message and available commands."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return
    await send_and_delete_message(context, update.effective_chat.id, HELP_TEXT, parse_mode="Markdown")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows information about the bot."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return
    info_message = (
        "**About this Bot**\n\n"
        "This bot helps you find and share files on Telegram.\n"
        "• Developed by Kaustav Ray."
    )
    await send_and_delete_message(context, update.effective_chat.id, info_message, parse_mode="Markdown")


async def rand_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /rand command to send a random file."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
        keyboard = InlineKeyboardMarkup(buttons)
        await send_and_delete_message(context, update.effective_chat.id, "❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    user_id = update.effective_user.id
    await send_and_delete_message(context, update.effective_chat.id, "⏳ Fetching a random file for you...")

    file_data = await get_random_file_from_db()

    if file_data:
        asyncio.create_task(send_file_task(user_id, update.effective_chat.id, context, file_data, update.effective_user.mention_html()))
    else:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Could not find a random file. The database might be empty.")


async def request_index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Allows any user to request a channel to be indexed, or to request a specific file to be indexed by replying to it.
    """
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    user = update.effective_user
    replied_message = update.message.reply_to_message

    # Workflow for replying to a file to request its index
    if replied_message and (replied_message.document or replied_message.video or replied_message.audio):
        if ADMINS:
            primary_admin_id = ADMINS[0]
            requester_mention = user.mention_html()

            file_to_send = replied_message.document or replied_message.video or replied_message.audio

            try:
                # Send a new message with the file to the admin, handling different file types
                if replied_message.document:
                    sent_file_message = await context.bot.send_document(
                        chat_id=primary_admin_id,
                        document=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                elif replied_message.video:
                    sent_file_message = await context.bot.send_video(
                        chat_id=primary_admin_id,
                        video=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                elif replied_message.audio:
                    sent_file_message = await context.bot.send_audio(
                        chat_id=primary_admin_id,
                        audio=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                else:
                    # Should not happen due to the check above, but as a fallback
                    await send_and_delete_message(context, update.effective_chat.id, "❌ Unsupported file type for indexing.")
                    return

                # Now send the approval instructions
                approval_caption = (
                    f"**File Index Request**\n\n"
                    f"**From User:** {requester_mention}\n"
                    f"**User ID:** `{user.id}`\n\n"
                    "Reply to the file above with `/done` to index it, or `/cancel` to reject it."
                )

                await context.bot.send_message(
                    chat_id=primary_admin_id,
                    text=approval_caption,
                    parse_mode="HTML"
                )
                await send_and_delete_message(context, update.effective_chat.id, "✅ Your request to index this file has been sent to the admin for approval.")
            except TelegramError as e:
                logger.error(f"Failed to send file for indexing approval: {e}")
                await send_and_delete_message(context, update.effective_chat.id, "❌ Could not send the file to the admin for approval. Please try again later.")
        else:
            await send_and_delete_message(context, update.effective_chat.id, "❌ No admin configured to approve requests.")
        return

    # Original workflow for requesting a channel index
    if not context.args:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "**Usage:**\n"
            "1. Reply to a file with `/request_index` to request it to be indexed.\n"
            "2. Use `/request_index <channel_link>` to request a channel to be indexed.",
            parse_mode="Markdown"
        )
        return

    request_text = " ".join(context.args)
    log_message = (
        f"🙏 **New Channel Index Request**\n\n"
        f"**From User:** {user.mention_html()}\n"
        f"**User ID:** `{user.id}`\n"
        f"**Username:** @{user.username or 'N/A'}\n\n"
        f"**Channel to Index:**\n`{request_text}`"
    )

    try:
        await context.bot.send_message(chat_id=LOG_CHANNEL, text=log_message, parse_mode="HTML")
        confirmation_text = f"✅ {user.mention_html()}, your request to index the channel has been sent to the admins."
        await send_and_delete_message(context, update.effective_chat.id, confirmation_text, parse_mode="HTML")
    except TelegramError as e:
        logger.error(f"Failed to process /request_index command for a channel: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Sorry, there was an error sending your request.")

async def refer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /refer command to get a referral link."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    user_id = update.effective_user.id
    bot_username = context.bot.username
    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    if referrals_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ The referral system is currently unavailable. Please try again later.")
        return

    try:
        user_referral_data = referrals_col.find_one({"_id": user_id})
        referral_count = user_referral_data.get("referral_count", 0) if user_referral_data else 0

        referral_message = (
            "**Earn Free Premium Access!**\n\n"
            "Share your unique referral link with your friends. For every 10 users who join using your link, you'll receive **1 month of premium access** (no ads, no verification)!\n\n"
            f"**Your Referral Link:**\n`{referral_link}`\n\n"
            f"**Your Current Referral Count:** {referral_count}/10"
        )
        await send_and_delete_message(context, update.effective_chat.id, referral_message, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error in /refer command for user {user_id}: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ An error occurred while fetching your referral data.")


async def request_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /request command for users to request files."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    user = update.effective_user
    if not context.args:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "Please provide a movie or file name to request.\n\nUsage: `/request <name>`",
            parse_mode="Markdown"
        )
        return

    request_text = " ".join(context.args)

    # Format the message for the log channel
    log_message = (
        f"🙏 **New Request**\n\n"
        f"**From User:** {user.mention_html()}\n"
        f"**User ID:** `{user.id}`\n"
        f"**Username:** @{user.username or 'N/A'}\n\n"
        f"**Request:**\n`{request_text}`"
    )

    try:
        # Forward the request to the log channel
        await context.bot.send_message(
            chat_id=LOG_CHANNEL,
            text=log_message,
            parse_mode="HTML"
        )
        # Confirm to the user
        confirmation_text = f"✅ {user.mention_html()}, your request has been sent to the admins. They will be notified."
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            confirmation_text,
            parse_mode="HTML"
        )
    except TelegramError as e:
        logger.error(f"Failed to process /request command: {e}")
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "❌ Sorry, there was an error sending your request. Please try again later."
        )


async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show recent error logs."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    # Retrieve all logs from the in-memory stream
    log_stream.seek(0)
    logs = log_stream.readlines()

    # Filter for ERROR and CRITICAL logs and get the last 20
    error_logs = [log.strip() for log in logs if "ERROR" in log or "CRITICAL" in log]
    recent_errors = error_logs[-20:]

    if not recent_errors:
        await send_and_delete_message(context, update.effective_chat.id, "✅ No recent errors found in the logs.")
    else:
        log_text = "```\nRecent Error Logs:\n\n" + "\n".join(recent_errors) + "\n```"
        await send_and_delete_message(context, update.effective_chat.id, log_text, parse_mode="MarkdownV2")

    # Clear the log buffer to prevent it from growing too large
    log_stream.seek(0)
    log_stream.truncate(0)


async def total_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of users."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Database not connected.")
        return

    try:
        user_count = users_col.count_documents({})
        await send_and_delete_message(context, update.effective_chat.id, f"📊 **Total Users:** {user_count}")
    except Exception as e:
        logger.error(f"Error getting user count: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Failed to retrieve user count. Please check the database connection.")


async def total_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of files."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Database not connected.")
        return

    try:
        # NOTE: This only gives the count from the CURRENT active database.
        file_count = files_col.count_documents({})
        await send_and_delete_message(context, update.effective_chat.id, f"🗃️ **Total Files (Current DB):** {file_count}")
    except Exception as e:
        logger.error(f"Error getting file count: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Failed to retrieve file count. Please check the database connection.")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get bot statistics, including per-URI file counts. (MODIFIED)"""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    await send_and_delete_message(context, update.effective_chat.id, "🔄 Collecting statistics, please wait...")

    user_count = 0
    total_file_count_all_db = 0 # Accumulator for total files across all URIs
    uri_stats = {}

    try:
        # 1. Get Total Users (from the currently connected DB)
        if users_col is not None:
            user_count = users_col.count_docum

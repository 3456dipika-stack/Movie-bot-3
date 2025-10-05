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
from flask import Flask
from threading import Thread
import os
import sys
from functools import lru_cache

# ========================
# CONFIG
# ========================
BOT_TOKEN = "7657898593:AAEqWdlNE9bAVikWAnHRYyQyj0BCXy6qUmc"  # Bot Token
DB_CHANNEL = -1002975831610  # Database channel
LOG_CHANNEL = -1002988891392  # Channel to log user queries
# Channels users must join for access
JOIN_CHECK_CHANNEL = [-1002692055617, -1002551875503, -1002839913869]
ADMINS = [6705618257]        # Admin IDs

# Custom promotional message (Simplified as per the last request)
REACTIONS = ["👀", "😱", "🔥", "😍", "🎉", "🥰", "😇", "⚡"]
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
    "• `/broadcast <msg>` - Send a message to all users.\n"
    "• `/grp_broadcast <msg>` - Send a message to all connected groups where the bot is an admin.\n"
        "• `/index_channel <channel_id> [skip]` - Index files from a channel.\n"
        "• `/addlinkshort <api_url> <api_key>` - Set the link shortener details.\n"
    "• Send a file to me in a private message to index it."
)

# A list of MongoDB URIs to use. Add as many as you need.
MONGO_URIS = [
    "mongodb+srv://bf44tb5_db_user:RhyeHAHsTJeuBPNg@cluster0.lgao3zu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://28c2kqa_db_user:IL51mem7W6g37mA5@cluster0.np0ffl0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://l6yml41j_db_user:2m5HFR6CTdSb46ck@cluster0.nztdqdr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://7afcwd6_db_user:sOthaH9f53BDRBoj@cluster0.m9d2zcy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://x05bq9p_db_user:gspcMp5M0NQnu9zt@cluster0.bhxd7dp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://fxexlqy_db_user:O5HiYEZee2pyUyGK@cluster0.ugozkfc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://vnitm0p_db_user:rz1Szy1U9fwJMkis@cluster0.apaqaef.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://mezojs2_db_user:gvT09wd648MfGP5W@cluster0.c5hejzo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://wspprp42_db_user:Mac4xZJVHOxkKzK0@cluster0.cgxjhpt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://r7fyvtce_db_user:5HSZsUd5TTQSpU5V@cluster0.9l4g28a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://kvr0j2wk_db_user:wH8jseEyDSHcm35L@cluster0.mwhmepa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://4yxduh8_db_user:45Lyw2zgcCUhxTQd@cluster0.afxbyeo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://zdqmu6ir_db_user:gNGahCtkshRz0T6i@cluster0.ihuljbb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
]
GROUPS_DB_URIS = ["mongodb+srv://6p5e2y8_db_user:MxRFLhQ534AI3rfQ@cluster0.j9hcylx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
VERIFICATION_DB_URIS = ["mongodb+srv://7eqsiq8_db_user:h6nYmRKbgHJDALUA@cluster0.wuntcv8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
VERIFIED_USERS_DB_URIS = ["mongodb+srv://q9amkpx_db_user:xuLc5qUJAJMBCtDH@cluster0.mvwgcxd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
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
verified_user_cache = {}  # {user_id: expiry_timestamp}
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
    """Check if the user is a member of ALL required channels."""
    for channel_id in JOIN_CHECK_CHANNEL:
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except TelegramError as e:
            logger.error(f"Error checking member status for user {user_id} in channel {channel_id}: {e}")
            return False

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

async def is_user_verified(user_id: int):
    """
    Checks if a user is verified, either through premium status or standard 24-hour verification.
    Caches the result for performance.
    """
    # 1. Check cache first
    if user_id in verified_user_cache:
        if time.time() < verified_user_cache[user_id]:
            return True
        else:
            del verified_user_cache[user_id]

    # 2. Check for Premium Status (from referrals)
    if referrals_col is not None:
        try:
            user_referral_data = referrals_col.find_one({"_id": user_id})
            if user_referral_data and "premium_until" in user_referral_data:
                # The TTL index will automatically remove expired documents, so if it exists, it's valid.
                expiry_time = time.time() + (30 * 24 * 60 * 60) # Cache for 30 days
                verified_user_cache[user_id] = expiry_time
                logger.info(f"User {user_id} has premium access. Cached.")
                return True
        except Exception as e:
            logger.error(f"Failed to check premium status for user {user_id}: {e}")

    # 3. If not premium, check standard 24-hour verification
    for uri in VERIFIED_USERS_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verified_users_db"]
            collection = db["verified_users"]
            user_doc = collection.find_one({"_id": user_id})
            if user_doc:
                # Store result in cache with a 24-hour expiry
                expiry_time = time.time() + (24 * 60 * 60)
                verified_user_cache[user_id] = expiry_time
                logger.info(f"User {user_id} is verified (24hr access). Cached.")
                return True # Found in one DB, that's enough
        except Exception as e:
            logger.error(f"Failed to check verification status for user {user_id} at ...{uri[-20:]}: {e}")
            continue

    logger.info(f"User {user_id} is not verified.")
    return False

async def mark_user_as_verified(user_id: int):
    """Adds a user to the verified list using the connection pool and updates the cache."""
    success_count = 0
    for uri in VERIFIED_USERS_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verified_users_db"]
            collection = db["verified_users"]
            # The TTL index will handle the 24-hour expiry based on 'verifiedAt'
            collection.update_one(
                {"_id": user_id},
                {"$set": {"verifiedAt": datetime.datetime.utcnow()}},
                upsert=True
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to mark user {user_id} as verified at ...{uri[-20:]}: {e}")
            continue

    if success_count > 0:
        # Update cache on success
        expiry_time = time.time() + (24 * 60 * 60)
        verified_user_cache[user_id] = expiry_time
        logger.info(f"Marked user {user_id} as verified for 24 hours in {success_count}/{len(VERIFIED_USERS_DB_URIS)} DBs. Cache updated.")
        return True
    else:
        logger.error(f"Failed to mark user {user_id} as verified in any DB.")
        return False

async def get_shortener_config():
    """Fetches the shortener config from the dedicated database using the connection pool."""
    for uri in VERIFICATION_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verification_db"]
            config_col = db["config"]
            config = config_col.find_one({"_id": "shortener_config"})
            if config:
                return config  # Return on first success
        except Exception as e:
            logger.error(f"Failed to get shortener config from ...{uri[-20:]}: {e}")
            continue  # Try next URI
    logger.error("All shortener DB URIs failed.")
    return None

async def get_shortened_link(url_to_shorten: str):
    """Generates a shortened link using the configured API."""
    config = await get_shortener_config()
    if not config or 'api_url' not in config or 'api_key' not in config:
        logger.error("Shortener API is not configured.")
        return "Error: Shortener not configured."

    api_url = config['api_url']
    api_key = config['api_key']

    # The API endpoint usually has the API key and the URL as parameters
    full_api_url = f"{api_url}?api={api_key}&url={url_to_shorten}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(full_api_url)
            response.raise_for_status()
            # The response is JSON, so we parse it and extract the URL.
            data = response.json()
            if data.get("status") == "success" and data.get("shortenedUrl"):
                return data["shortenedUrl"]
            else:
                logger.error(f"Shortener API returned an unexpected response: {response.text}")
                return "Error: Invalid response from shortener API."
    except httpx.RequestError as e:
        logger.error(f"Failed to get shortened link: {e}")
        return f"Error: Could not shorten link. {e}"
    except Exception as e:
        logger.error(f"Failed to decode or process response from shortener API: {e}")
        return "Error: Could not parse response from shortener API."

async def save_verification_progress(verification_data):
    """Saves or updates a user's verification progress using the connection pool."""
    verification_data['createdAt'] = datetime.datetime.utcnow() # Reset timer on each step

    success_count = 0
    for uri in VERIFICATION_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verification_db"]
            collection = db["pending_verifications"]
            collection.update_one(
                {"_id": verification_data["_id"]},
                {"$set": verification_data},
                upsert=True
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to save verification progress to ...{uri[-20:]}: {e}")
            continue

    return success_count > 0

async def get_verification_progress(verification_id: str):
    """Fetches a user's verification progress using the connection pool."""
    for uri in VERIFICATION_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verification_db"]
            collection = db["pending_verifications"]
            progress = collection.find_one({"_id": verification_id})
            if progress:
                return progress
        except Exception as e:
            logger.error(f"Failed to get verification progress from ...{uri[-20:]}: {e}")
            continue
    return None

async def delete_verification_progress(verification_id: str):
    """Deletes a verification record from all DBs using the connection pool."""
    for uri in VERIFICATION_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            db = client["verification_db"]
            collection = db["pending_verifications"]
            collection.delete_one({"_id": verification_id})
        except Exception as e:
            logger.error(f"Failed to delete verification progress from ...{uri[-20:]}: {e}")
            continue


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
    all_uris = set(MONGO_URIS + GROUPS_DB_URIS + VERIFICATION_DB_URIS + VERIFIED_USERS_DB_URIS)
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
        await update.message.react(reaction=random.choice(REACTIONS))
    except TelegramError as e:
        logger.warning(f"Could not react to message: {e}")


async def send_file_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_data: dict):
    """Background task to send a single file to the user's private chat and auto-delete it."""
    try:
        sent_message = await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=file_data["channel_id"],
            message_id=file_data["file_id"],
        )

        if sent_message:
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            await send_and_delete_message(context, source_chat_id, "✅ I have sent the file to you in a private message. The file will be deleted automatically in 5 minutes.")

            await asyncio.sleep(5 * 60)
            await context.bot.delete_message(chat_id=user_id, message_id=sent_message.message_id)
            logger.info(f"Deleted message {sent_message.message_id} from chat {user_id}.")

    except TelegramError as e:
        logger.error(f"Failed to send file to user {user_id}: {e}")
        await send_and_delete_message(context, source_chat_id, "❌ File not found or could not be sent. Please ensure the bot is not blocked in your private chat.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending the file: {e}")
        await send_and_delete_message(context, source_chat_id, "❌ An unexpected error occurred. Please try again later.")


async def send_all_files_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_list: list):
    """Background task to send multiple files to the user's private chat and auto-delete them."""
    sent_messages = []
    try:
        for file in file_list:
            sent_message = await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=file["channel_id"],
                message_id=file["file_id"],
            )
            sent_messages.append(sent_message.message_id)
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            await asyncio.sleep(0.5)

        await send_and_delete_message(
            context,
            source_chat_id,
            "✅ I have sent all files to you in a private message. The files will be deleted automatically in 5 minutes."
        )

        await asyncio.sleep(5 * 60)
        for message_id in sent_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                logger.info(f"Deleted message {message_id} from chat {user_id}.")
            except TelegramError as e:
                logger.warning(f"Failed to delete message {message_id} for user {user_id}: {e}")

    except TelegramError as e:
        logger.error(f"Failed to send one or more files to user {user_id}: {e}")
        await send_and_delete_message(context, source_chat_id, "❌ One or more files could not be sent. Please ensure the bot is not blocked in your private chat.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending all files: {e}")
        await send_and_delete_message(context, source_chat_id, "❌ An unexpected error occurred. Please try again later.")

# ========================
# COMMAND HANDLERS
# ========================

async def handle_verification_step(update: Update, context: ContextTypes.DEFAULT_TYPE, verification_id: str):
    """Handles a user clicking a verification deep link."""
    user = update.effective_user
    progress = await get_verification_progress(verification_id)

    if not progress:
        await send_and_delete_message(context, user.id, "❌ This verification link is invalid or has expired. Please start over by requesting a file again.")
        return

    if progress.get("user_id") != user.id:
        await send_and_delete_message(context, user.id, "❌ This verification link is not for you.")
        return

    current_step = progress.get("step", 1)
    next_step = current_step + 1

    if next_step <= 3:
        # Continue to the next step
        progress['step'] = next_step
        if await save_verification_progress(progress):
            bot_username = context.bot.username
            deep_link = f"https://t.me/{bot_username}?start={verification_id}"
            shortened_link = await get_shortened_link(deep_link)

            if "Error:" in shortened_link:
                await send_and_delete_message(context, user.id, shortened_link)
            else:
                await send_and_delete_message(
                    context,
                    user.id,
                    f"✅ Step {current_step} complete!\n\n"
                    f"Step {next_step} of 3: Please open this link to continue:\n{shortened_link}"
                )
        else:
            await send_and_delete_message(context, user.id, "❌ Could not save your verification progress. Please try again.")

    else:
        # Verification complete
        await send_and_delete_message(context, user.id, "✅ Verification successful! You now have access for 24 hours. Sending your requested file(s)...")
        await mark_user_as_verified(user.id)

        # Deliver the originally requested file(s)
        original_request = progress.get("original_request")
        if original_request:
            # The source chat for a verification-completed action is always the user's private chat.
            source_chat_id = user.id

            if original_request.get("type") == "single":
                file_id = original_request.get("file_id")
                file_data = None
                for uri in MONGO_URIS:
                    client = mongo_clients.get(uri)
                    if not client:
                        continue
                    try:
                        db = client["telegram_files"]
                        file_data = db["files"].find_one({"_id": ObjectId(file_id)})
                        if file_data: break
                    except Exception: continue

                if file_data:
                    asyncio.create_task(send_file_task(user.id, source_chat_id, context, file_data))
                else:
                    await send_and_delete_message(context, user.id, "❌ The originally requested file could not be found.")

            elif original_request.get("type") == "batch":
                file_ids = [ObjectId(fid) for fid in original_request.get("file_ids", [])]
                files_to_send = []
                for uri in MONGO_URIS:
                    client = mongo_clients.get(uri)
                    if not client:
                        continue
                    try:
                        db = client["telegram_files"]
                        files_to_send.extend(list(db["files"].find({"_id": {"$in": file_ids}})))
                    except Exception: continue

                if files_to_send:
                    asyncio.create_task(send_all_files_task(user.id, source_chat_id, context, files_to_send))
                else:
                    await send_and_delete_message(context, user.id, "❌ The originally requested files could not be found.")

            elif original_request.get("type") == "random":
                file_data = await get_random_file_from_db()
                if file_data:
                    asyncio.create_task(send_file_task(user.id, source_chat_id, context, file_data))
                else:
                    await send_and_delete_message(context, user.id, "❌ Could not find a random file after verification. The database might be empty.")

        # Clean up the verification progress from the database
        await delete_verification_progress(verification_id)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command, including verification and referral deep links."""
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

        # 2. Verification Link Handling
        elif not payload.startswith("ref_"):
            # This must be a verification link, pass it to the handler
            await handle_verification_step(update, context, payload)
            await save_user_info(user)  # Save user info after they attempt verification
            return  # Stop further execution of the start command

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
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return
    await send_and_delete_message(context, update.effective_chat.id, HELP_TEXT, parse_mode="Markdown")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows information about the bot."""
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
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "❌ You are banned from using this bot.")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Channel: @filestore4u", url="https://t.me/filestore4u")],
            [InlineKeyboardButton("Join Channel: @code_boost", url="https://t.me/code_boost")],
            [InlineKeyboardButton("Join Channel: @krbook_official", url="https://t.me/krbook_official")]
        ])
        await send_and_delete_message(context, update.effective_chat.id, "❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    user_id = update.effective_user.id
    is_verified = await is_user_verified(user_id)

    if user_id in ADMINS or is_verified:
        await send_and_delete_message(context, update.effective_chat.id, "⏳ Fetching a random file for you...")

        file_data = await get_random_file_from_db()

        if file_data:
            asyncio.create_task(send_file_task(user_id, update.effective_chat.id, context, file_data))
        else:
            await send_and_delete_message(context, update.effective_chat.id, "❌ Could not find a random file. The database might be empty.")
    else:
        # Start verification process
        original_request = {"type": "random"}
        await start_verification_process(context, user_id, update.effective_user.mention_html(), update.effective_chat.id, original_request)


async def refer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /refer command to get a referral link."""
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
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "✅ Your request has been sent to the admins. They will be notified."
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
            user_count = users_col.count_documents({})

        # 2. Get File Counts per URI and total file count
        for idx, uri in enumerate(MONGO_URIS):
            client = mongo_clients.get(uri)
            if not client:
                uri_stats[idx] = "❌ Not connected"
                continue
            try:
                # Use the existing client from the pool
                temp_db = client["telegram_files"]
                temp_files_col = temp_db["files"]
                # Get file count
                file_count = temp_files_col.estimated_document_count()

                # Get DB stats
                db_stats = temp_db.command('dbStats', 1)
                used_storage_mib = db_stats.get('dataSize', 0) / (1024 * 1024)
                total_storage_mib = db_stats.get('storageSize', 0) / (1024 * 1024)
                free_storage_mib = total_storage_mib - used_storage_mib

                uri_stats[idx] = (
                    f"✅ {file_count} files\n"
                    f"     ★ 𝚄𝚂𝙴𝙳 𝚂𝚃𝙾𝚁𝙰𝙶𝙴: <code>{used_storage_mib:.2f}</code> 𝙼𝚒𝙱\n"
                    f"     ★ 𝙵𝚁𝙴𝙴 𝚂𝚃𝙾𝚁𝙰𝙶𝙴: <code>{free_storage_mib:.2f}</code> 𝙼𝚒𝙱"
                )
                total_file_count_all_db += file_count # Accumulate count
            except Exception as e:
                logger.warning(f"Failed to connect or get file count for URI #{idx + 1}: {e}")
                uri_stats[idx] = "❌ Failed to read"

        # 3. Format the output message
        stats_message = (
            f"📊 <b>Bot Statistics</b>\n"
            f"  • Total Users: {user_count}\n"
            f"  • Total Connected Groups: {len(JOIN_CHECK_CHANNEL)}\n" # Using the count of JOIN_CHECK_CHANNEL
            f"  • Total Files (All DB): {total_file_count_all_db}\n" # Total count from all URIs
            f"  • <b>Total MongoDB URIs:</b> {len(MONGO_URIS)}\n"
            f"  • <b>Current Active URI:</b> #{current_uri_index + 1}\n\n"
            f"<b>File Count per URI:</b>\n"
        )
        for idx, status in uri_stats.items():
            stats_message += f"  • URI #{idx + 1}: {status}\n"

        await send_and_delete_message(context, update.effective_chat.id, stats_message, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Failed to retrieve statistics. Please check the database connection.")


async def delete_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete a file by its MongoDB ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Database not connected.")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /deletefile <MongoDB_ID>\nTip: Use /findfile <filename> to get the ID.")
        return

    try:
        file_id = context.args[0]
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_one({"_id": ObjectId(file_id)})

        if result.deleted_count == 1:
            await send_and_delete_message(context, update.effective_chat.id, f"✅ File with ID `{file_id}` has been deleted from the database.")
        else:
            await send_and_delete_message(context, update.effective_chat.id, f"❌ File with ID `{file_id}` not found in the database.")
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Invalid ID or an error occurred. Please provide a valid MongoDB ID.")


async def find_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to find a file by its name and show its ID. Searches ALL URIs."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /findfile <filename>")
        return

    query_filename = " ".join(context.args)
    all_results = []

    await send_and_delete_message(context, update.effective_chat.id, f"🔎 Searching all {len(MONGO_URIS)} databases for `{query_filename}`...")

    # Iterate through all URIs
    for idx, uri in enumerate(MONGO_URIS):
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Use regex for case-insensitive search
            results = list(temp_files_col.find({"file_name": {"$regex": query_filename, "$options": "i"}}))
            all_results.extend(results)
            logger.info(f"Found {len(results)} files in URI #{idx + 1}")
        except Exception as e:
            logger.error(f"Error finding file on URI #{idx + 1}: {e}")


    if not all_results:
        await send_and_delete_message(context, update.effective_chat.id, f"❌ No files found with the name `{query_filename}` in any database.")
        return

    response_text = f"📁 Found {len(all_results)} files matching `{query_filename}` across all databases:\n\n"
    for idx, file in enumerate(all_results):
        response_text += f"{idx + 1}. *{escape_markdown(file['file_name'])}*\n  `ID: {file['_id']}`\n\n"

    response_text += "Copy the ID of the file you want to delete and use the command:\n`/deletefile <ID>`\n\nNote: `/deletefile` only works on the currently *active* database. If the file is not found, you may need to manually update the `current_uri_index` and restart."

    await send_and_delete_message(context, update.effective_chat.id, response_text, parse_mode="Markdown")


async def delete_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete all files from the database."""
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
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_many({})
        await send_and_delete_message(context, update.effective_chat.id, f"✅ Deleted {result.deleted_count} files from the **current** database.")
    except Exception as e:
        logger.error(f"Error deleting all files: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ An error occurred while trying to delete all files from the current database.")


async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to ban a user by their user ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if not context.args or not context.args[0].isdigit():
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /ban <user_id>")
        return

    user_to_ban_id = int(context.args[0])
    if user_to_ban_id in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Cannot ban an admin.")
        return

    if banned_users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Database not connected.")
        return

    try:
        banned_users_col.update_one(
            {"_id": user_to_ban_id},
            {"$set": {"_id": user_to_ban_id}},
            upsert=True
        )
        # Update cache
        banned_user_cache[user_to_ban_id] = True
        await send_and_delete_message(context, update.effective_chat.id, f"🔨 User `{user_to_ban_id}` has been banned.")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ An error occurred while trying to ban the user.")


async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to unban a user by their user ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if not context.args or not context.args[0].isdigit():
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /unban <user_id>")
        return

    user_to_unban_id = int(context.args[0])

    if banned_users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Database not connected.")
        return

    try:
        result = banned_users_col.delete_one({"_id": user_to_unban_id})

        if result.deleted_count == 1:
            # Update cache
            banned_user_cache[user_to_unban_id] = False
            await send_and_delete_message(context, update.effective_chat.id, f"✅ User `{user_to_unban_id}` has been unbanned.")
        else:
            await send_and_delete_message(context, update.effective_chat.id, f"❌ User `{user_to_unban_id}` was not found in the banned list.")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "❌ An error occurred while trying to unban the user.")


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Broadcasts a message to all users in the database.
    Usage: /broadcast <message>
    """
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /broadcast <message>")
        return

    broadcast_text = " ".join(context.args)

    # NOTE: This only broadcasts to users in the *current* active database's users_col.
    # To broadcast to ALL users, you'd need to query all URIs for user IDs.
    users_cursor = users_col.find({}, {"_id": 1})
    user_ids = [user["_id"] for user in users_cursor]
    sent_count = 0
    failed_count = 0

    await send_and_delete_message(context, update.effective_chat.id, f"🚀 Starting broadcast to {len(user_ids)} users...")

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.1)
        except TelegramError as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {uid}: {e}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Unknown error sending broadcast to user {uid}: {e}")

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Broadcast complete!\n\nSent to: {sent_count}\nFailed: {failed_count}")


async def grp_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to broadcast a message to all connected groups where the bot is an admin."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /grp_broadcast <message>")
        return

    broadcast_text = " ".join(context.args)

    # Fetch all unique group IDs from all configured groups databases
    all_group_ids = set()
    logger.info("Fetching all group IDs for group broadcast from all group DBs...")
    for uri in GROUPS_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["telegram_groups"]
            temp_groups_col = temp_db["groups"]

            group_docs = temp_groups_col.find({}, {"_id": 1})
            for doc in group_docs:
                all_group_ids.add(doc['_id'])
        except Exception as e:
            logger.error(f"Failed to fetch group IDs from ...{uri[-20:]}: {e}")

    if not all_group_ids:
        await send_and_delete_message(context, update.effective_chat.id, "❌ No groups found in the database to broadcast to.")
        return

    # Send message to each group
    sent_count = 0
    failed_count = 0
    await send_and_delete_message(context, update.effective_chat.id, f"🚀 Starting group broadcast to {len(all_group_ids)} groups...")

    for group_id in all_group_ids:
        try:
            # Check for admin status before sending to be safe
            member = await context.bot.get_chat_member(group_id, context.bot.id)
            if member.status in ["administrator", "creator"]:
                await context.bot.send_message(chat_id=group_id, text=broadcast_text)
                sent_count += 1
                logger.info(f"Group broadcast sent to group {group_id}")
            else:
                logger.warning(f"Skipping broadcast to group {group_id}, bot is no longer an admin.")
                failed_count += 1
            await asyncio.sleep(0.1)  # Rate limiting
        except TelegramError as e:
            logger.error(f"Failed to send broadcast to group {group_id}: {e}")
            failed_count += 1

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Group broadcast complete!\n\nSent to: {sent_count} groups\nFailed: {failed_count} groups")


async def index_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to index files from a given channel."""
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if len(context.args) < 1:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /index_channel <channel_id> [skip_messages]")
        return

    try:
        channel_id = int(context.args[0])
    except ValueError:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Invalid Channel ID. It should be a number.")
        return

    skip_messages = 0
    if len(context.args) > 1:
        try:
            skip_messages = int(context.args[1])
        except ValueError:
            await send_and_delete_message(context, update.effective_chat.id, "❌ Invalid skip count. It should be a number.")
            return

    # Schedule the indexing task to run in the background
    asyncio.create_task(index_channel_task(context, channel_id, skip_messages, update.effective_chat.id))
    await send_and_delete_message(context, update.effective_chat.id, "✅ Indexing has started in the background. I will notify you when it's complete.")

async def addlinkshort_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set the link shortener API details on all verification DBs."""
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ You do not have permission to use this command.")
        return

    if len(context.args) != 2:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /addlinkshort <api_url> <api_key>")
        return

    api_url = context.args[0]
    api_key = context.args[1]

    success_count = 0
    for uri in VERIFICATION_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["verification_db"]
            config_col = temp_db["config"]

            # Store the config as a single document
            config_col.update_one(
                {"_id": "shortener_config"},
                {"$set": {"api_url": api_url, "api_key": api_key}},
                upsert=True
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to save shortener config to ...{uri[-20:]}: {e}")

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Link shortener details saved to {success_count}/{len(VERIFICATION_DB_URIS)} databases.")


async def index_channel_task(context: ContextTypes.DEFAULT_TYPE, channel_id: int, skip: int, user_chat_id: int):
    """Background task to handle channel indexing."""
    last_message_id = 0
    try:
        # A bit of a hack to get the last message ID
        temp_msg = await context.bot.send_message(chat_id=channel_id, text=".")
        last_message_id = temp_msg.message_id
        await context.bot.delete_message(chat_id=channel_id, message_id=last_message_id)
    except Exception as e:
        logger.error(f"Could not get last message ID for channel {channel_id}: {e}")
        await send_and_delete_message(context, user_chat_id, f"❌ Failed to access channel {channel_id}. Make sure the bot is an admin there.")
        return

    indexed_count = 0
    for i in range(skip + 1, last_message_id):
        forwarded_message = None
        try:
            # Forward the message to the DB_CHANNEL to get a message object with file attributes
            forwarded_message = await context.bot.forward_message(
                chat_id=DB_CHANNEL,
                from_chat_id=channel_id,
                message_id=i
            )

            file = forwarded_message.document or forwarded_message.video or forwarded_message.audio
            if not file:
                continue

            # Get filename (note: original caption is lost on forward)
            raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id
            clean_name = raw_name.replace("_", " ").replace(".", " ").replace("-", " ") if raw_name else "Unknown"

            # Save metadata to all file databases for redundancy
            saved_to_any_db = False
            for uri in MONGO_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_files"]
                    temp_files_col = temp_db["files"]
                    # THE CRITICAL FIX: Save original message_id and channel_id
                    temp_files_col.insert_one({
                        "file_name": clean_name,
                        "file_id": i, # Original message ID
                        "channel_id": channel_id, # Original channel ID
                        "file_size": file.file_size,
                    })
                    saved_to_any_db = True
                except Exception as e:
                    logger.error(f"DB Error while indexing for URI ...{uri[-20:]}: {e}")

            if saved_to_any_db:
                indexed_count += 1
                logger.info(f"Indexed message {i} from channel {channel_id}: {clean_name}")

            # Send progress update every 100 files
            if indexed_count > 0 and indexed_count % 100 == 0:
                await send_and_delete_message(context, user_chat_id, f"✅ Progress: Indexed {indexed_count} files so far...")

        except TelegramError as e:
            logger.warning(f"Could not process message {i} from channel {channel_id}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while indexing message {i}: {e}")
        finally:
            # CRITICAL: Delete the temporary forwarded message to keep DB channel clean
            if forwarded_message:
                await context.bot.delete_message(chat_id=DB_CHANNEL, message_id=forwarded_message.message_id)

    await send_and_delete_message(context, user_chat_id, f"✅✅ Finished indexing channel {channel_id}. Total files indexed: {indexed_count}.")


async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the bot being added to or removed from a group."""
    my_chat_member = update.my_chat_member

    # Check if the update is for a group/supergroup and if the bot is the one being updated
    if my_chat_member.chat.type in ["group", "supergroup"] and my_chat_member.new_chat_member.user.id == context.bot.id:
        group_id = my_chat_member.chat.id
        new_status = my_chat_member.new_chat_member.status
        old_status = my_chat_member.old_chat_member.status

        # If the bot was promoted to administrator or is the creator
        if new_status in ["administrator", "creator"]:
            logger.info(f"Bot was added/promoted as admin in group {group_id}. Saving to all groups databases.")
            for uri in GROUPS_DB_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_groups"]
                    temp_groups_col = temp_db["groups"]
                    temp_groups_col.update_one({"_id": group_id}, {"$set": {"_id": group_id}}, upsert=True)
                    logger.info(f"Successfully saved/updated group {group_id} in groups DB at ...{uri[-20:]}.")
                except Exception as e:
                    logger.error(f"Failed to save group {group_id} to groups DB at ...{uri[-20:]}: {e}")

        # If the bot was kicked, left, or demoted from admin
        elif old_status in ["administrator", "creator"] and new_status not in ["administrator", "creator"]:
            logger.info(f"Bot was removed or demoted from admin in group {group_id}. Removing from all groups databases.")
            for uri in GROUPS_DB_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_groups"]
                    temp_groups_col = temp_db["groups"]
                    temp_groups_col.delete_one({"_id": group_id})
                    logger.info(f"Successfully removed group {group_id} from groups DB at ...{uri[-20:]}.")
                except Exception as e:
                    logger.error(f"Failed to remove group {group_id} from groups DB at ...{uri[-20:]}: {e}")


# ========================
# FILE/SEARCH HANDLERS
# ========================

async def save_file_from_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file to bot -> save to channel + DB. Uses connection pooling."""
    user_id = update.message.from_user.id
    if user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Forward to database channel
    forwarded = await update.message.forward(DB_CHANNEL)

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = raw_name.replace("_", " ").replace(".", " ").replace("-", " ") if raw_name else "Unknown"

    global current_uri_index, db, files_col, users_col, banned_users_col

    saved = False
    # Start the loop from the current active index and wrap around to try all
    for i in range(len(MONGO_URIS)):
        idx = (current_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        client = mongo_clients.get(uri_to_try)
        if not client:
            logger.warning(f"Skipping disconnected DB for file save: ...{uri_to_try[-20:]}")
            continue

        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": forwarded.message_id,
                "channel_id": forwarded.chat.id,
                "file_size": file.file_size,
            })

            # If successful and this is not the current active DB, switch to it.
            if idx != current_uri_index:
                current_uri_index = idx
                db = temp_db
                files_col = temp_db["files"]
                users_col = temp_db["users"]
                banned_users_col = temp_db["banned_users"]
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            await send_and_delete_message(context, update.effective_chat.id, f"✅ Saved to DB #{idx + 1}: {clean_name}")
            saved = True
            break # Exit loop on success
        except Exception as e:
            logger.error(f"Error saving file with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                 await send_and_delete_message(context, update.effective_chat.id, f"⚠️ Primary DB failed. Trying next available URI...")

    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        await send_and_delete_message(context, update.effective_chat.id, "❌ Failed to save file on all available databases.")


async def save_file_from_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file directly to channel -> save to DB. Uses connection pooling."""
    user_id = update.message.from_user.id
    chat_id = update.message.chat.id

    # Only process files from admins in the database channel
    if chat_id != DB_CHANNEL or user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = raw_name.replace("_", " ").replace(".", " ").replace("-", " ") if raw_name else "Unknown"

    global current_uri_index, db, files_col, users_col, banned_users_col

    saved = False
    # Start the loop from the current active index and wrap around to try all
    for i in range(len(MONGO_URIS)):
        idx = (current_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        client = mongo_clients.get(uri_to_try)
        if not client:
            logger.warning(f"Skipping disconnected DB for channel file save: ...{uri_to_try[-20:]}")
            continue

        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": update.message.message_id,
                "channel_id": chat_id,
                "file_size": file.file_size,
            })

            # If successful and this is not the current active DB, switch to it.
            if idx != current_uri_index:
                current_uri_index = idx
                db = temp_db
                files_col = temp_db["files"]
                users_col = temp_db["users"]
                banned_users_col = temp_db["banned_users"]
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            # Send **INSTANT** success notification to the admin
            try:
                await send_and_delete_message(
                    context,
                    user_id,
                    f"✅ File **`{escape_markdown(clean_name)}`** has been indexed successfully from the database channel to DB #{idx + 1}.",
                    parse_mode="MarkdownV2"
                )
            except TelegramError as e:
                logger.error(f"Failed to send notification to admin {user_id}: {e}")
            saved = True
            break

        except Exception as e:
            logger.error(f"Error saving file from channel with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                try:
                    await send_and_delete_message(context, user_id, "⚠️ Primary DB failed. Trying next available URI...")
                except TelegramError:
                    pass

    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        try:
            await send_and_delete_message(context, user_id, "❌ Failed to save file on all available databases.")
        except TelegramError:
            pass


async def search_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Search ALL URIs and show results, sorted by relevance.
    Uses a broad regex for initial filtering and fuzzy matching for accurate ranking.
    """
    if not await bot_can_respond(update, context):
        return

    # In private chat, only admins can search for files.
    if update.effective_chat.type == "private" and update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Use this bot on any group. Sorry, (only admin)")
        return

    if await is_banned(update.effective_user.id):
        await update.message.reply_text("❌ You are banned from using this bot.")
        return

    # Add reaction to user's message in the background
    asyncio.create_task(react_to_message_task(update))

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        # NEW: Updated to show buttons for all channels
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Channel: @filestore4u", url="https://t.me/filestore4u")],
            [InlineKeyboardButton("Join Channel: @code_boost", url="https://t.me/code_boost")],
            [InlineKeyboardButton("Join Channel: @krbook_official", url="https://t.me/krbook_official")]
        ])
        await send_and_delete_message(context, update.effective_chat.id, "❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    # Send initial status message that will be edited with progress
    status_message = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="⏳ Searching..."
    )

    raw_query = update.message.text.strip()
    # Normalize query for better fuzzy search
    normalized_query = raw_query.replace("_", " ").replace(".", " ").replace("-", " ").strip()

    # Log the user's query
    user = update.effective_user
    log_text = f"🔍 User: {user.full_name} | @{user.username} | ID: {user.id}\nQuery: {raw_query}"
    try:
        await context.bot.send_message(LOG_CHANNEL, text=log_text)
    except Exception as e:
        logger.error(f"Failed to log query to channel: {e}")


    # --- REVISED SEARCH LOGIC (Broad Filtering + Fuzzy Ranking) ---

    # Split the query into words and escape them for a forgiving regex. Ignore short words.
    words = [re.escape(word) for word in normalized_query.split() if len(word) > 1]

    if not words:
        await send_and_delete_message(context, update.effective_chat.id, "❌ Query too short or invalid. Please try a longer search term.")
        return

    # Create an OR condition for the words (e.g., /word1|word2|.../i)
    # This ensures that if ANY of the main words are in the filename, it's considered for fuzzy ranking.
    regex_pattern = re.compile("|".join(words), re.IGNORECASE)
    query_filter = {"file_name": {"$regex": regex_pattern}}

    preliminary_results = []

    # Iterate over ALL URIs for search
    total_dbs = len(MONGO_URIS)
    for idx, uri in enumerate(MONGO_URIS):
        try:
            await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text=f"⏳ Searching... ({idx + 1}/{total_dbs} databases searched)"
            )
        except TelegramError:  # Ignore if message can't be edited
            pass

        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            # Use the existing client from the pool
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Query the database with the broad filter
            results = list(temp_files_col.find(query_filter))
            preliminary_results.extend(results)

        except Exception as e:
            logger.error(f"MongoDB search query failed on URI #{idx + 1}: {e}")

    # --- Fuzzy Ranking (to ensure the best match is first) ---

    if not preliminary_results:
        try:
            await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text="❌ No relevant files found. For your query contact @kaustavhibot"
            )
        except TelegramError:
            pass # Ignore if message was deleted
        return

    results_with_score = []
    # Use a set to track file_id + channel_id tuples to ensure no duplicates from different DBs
    unique_files = set()

    for file in preliminary_results:
        file_key = (file.get('file_id'), file.get('channel_id'))
        if file_key in unique_files:
            continue

        # Use WRatio for a more robust score that handles partial strings and other variations well.
        score = fuzz.WRatio(normalized_query, file['file_name'])

        # Keep results that have a score above 60 for better relevance.
        if score > 50:
            results_with_score.append((file, score))
            unique_files.add(file_key)

    # Sort the results by score in descending order
    sorted_results = sorted(results_with_score, key=lambda x: x[1], reverse=True)

    # Extract the file documents from the sorted list and limit to the top 50
    final_results = [result[0] for result in sorted_results[:50]]

    if not final_results:
        try:
            await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text="❌ No relevant files found after filtering by relevance. For your query contact @kaustavhibot"
            )
        except TelegramError:
            pass # Ignore if message was deleted
        return

    # Pass the full result list to the pagination function for consistency
    context.user_data['search_results'] = final_results
    context.user_data['search_query'] = raw_query

    # Edit the status message to show the final results
    await send_results_page(
        chat_id=status_message.chat.id,
        results=final_results,
        page=0,
        context=context,
        query=raw_query,
        message_id=status_message.message_id
    )


async def send_results_page(chat_id, results, page, context: ContextTypes.DEFAULT_TYPE, query: str, message_id: int):
    """Edits a message to show a paginated list of search results."""
    start, end = page * 10, (page + 1) * 10
    page_results = results[start:end]

    # Escape the query string for Markdown
    escaped_query = escape_markdown(query)
    text = f"🔎 *Top {len(results)}* Results for: *{escaped_query}*\n(Page {page + 1} / {math.ceil(len(results) / 10)}) (Sorted by Relevance)"
    buttons = []

    # Add files for the current page
    for idx, file in enumerate(page_results, start=start + 1):
        # Format the filename first, then escape it for Markdown
        file_size = format_size(file.get("file_size"))
        file_obj_id = str(file['_id'])

        button_text = f"[{file_size}] {file['file_name'][:40]}"
        buttons.append(
            [InlineKeyboardButton(button_text, callback_data=f"get_{file_obj_id}")]
        )

    # Add the promotional text at the end
    text += "\n\nKaustav Ray                                                                                                      Join here: @filestore4u     @freemovie5u"

    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{query}"))
    if end < len(results):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{query}"))

    if nav_buttons:
        buttons.append(nav_buttons)

    # Send All button
    buttons.append([InlineKeyboardButton("📨 Send All Files (Current Page)", callback_data=f"sendall_{page}_{query}")])

    reply_markup = InlineKeyboardMarkup(buttons)

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    except TelegramError as e:
        logger.error(f"Error editing search results page: {e}")


async def start_verification_process(context: ContextTypes.DEFAULT_TYPE, user_id: int, user_mention: str, source_chat_id: int, original_request: dict):
    """
    Starts the 3-step verification process for a user.
    `original_request` should contain the file request details.
    """
    verification_id = str(uuid.uuid4())
    progress_data = {
        "_id": verification_id,
        "user_id": user_id,
        "step": 1,
        "original_request": original_request
    }

    if await save_verification_progress(progress_data):
        bot_username = context.bot.username
        deep_link = f"https://t.me/{bot_username}?start={verification_id}"

        shortened_link = await get_shortened_link(deep_link)

        if "Error:" in shortened_link:
            await send_and_delete_message(context, user_id, shortened_link)
        else:
            # Send the first link to the user's private chat
            await send_and_delete_message(
                context,
                user_id,
                f"Please complete the 3-step verification to get 24-hour access to all files.\n\n"
                f"Step 1 of 3: Open this link to continue:\n{shortened_link}"
            )
            # Notify in group if the request came from a group
            if source_chat_id != user_id:
                 await send_and_delete_message(context, source_chat_id, f"✅ {user_mention}, At first you should start me in private chat. Then please complete the verification in my private chat to get your file.", parse_mode="HTML")
    else:
        await send_and_delete_message(context, user_id, "❌ Could not start the verification process. Please try again later.")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button clicks with new verification flow."""
    query = update.callback_query
    await query.answer()

    if not await bot_can_respond(update, context):
        return

    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, query.message.chat.id, "❌ You are banned from using this bot.")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Channel: @filestore4u", url="https://t.me/filestore4u")],
            [InlineKeyboardButton("Join Channel: @code_boost", url="https://t.me/code_boost")],
            [InlineKeyboardButton("Join Channel: @krbook_official", url="https://t.me/krbook_official")]
        ])
        await send_and_delete_message(context, query.message.chat.id, "❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    data = query.data
    user_id = query.from_user.id

    # --- File Request Logic (get_ or sendall_) ---
    if data.startswith("get_") or data.startswith("sendall_"):
        is_verified = await is_user_verified(user_id)

        # Admins and already verified users get files directly
        if user_id in ADMINS or is_verified:
            if user_id in ADMINS:
                await send_and_delete_message(context, query.message.chat.id, "⌛ Processing your request as an admin...")
            else:
                 await send_and_delete_message(context, query.message.chat.id, "✅ You are already verified. Sending your file(s)...")

            # --- Send Single File ---
            if data.startswith("get_"):
                file_id_str = data.split("_", 1)[1]
                file_data = None
                for uri in MONGO_URIS:
                    client = mongo_clients.get(uri)
                    if not client:
                        continue
                    try:
                        temp_db = client["telegram_files"]
                        temp_files_col = temp_db["files"]
                        file_data = temp_files_col.find_one({"_id": ObjectId(file_id_str)})
                        if file_data: break
                    except Exception as e:
                        logger.error(f"DB Error while fetching file {file_id_str} for verified user: {e}")

                if file_data:
                    asyncio.create_task(send_file_task(user_id, query.message.chat.id, context, file_data))
                else:
                    await send_and_delete_message(context, user_id, "❌ File not found.")

            # --- Send All Files (Batch) ---
            elif data.startswith("sendall_"):
                _, page_str, search_query = data.split("_", 2)
                page = int(page_str)
                final_results = context.user_data.get('search_results')
                if not final_results:
                    await send_and_delete_message(context, user_id, "❌ Search session expired. Please search again.")
                    return
                files_to_send = final_results[page * 10:(page + 1) * 10]
                if not files_to_send:
                    await send_and_delete_message(context, user_id, "❌ No files found on this page to send.")
                    return

                asyncio.create_task(send_all_files_task(user_id, query.message.chat.id, context, files_to_send))
            return

        # --- Start Verification for Non-Admin/Non-Verified Users ---
        else:
            original_request = {}
            if data.startswith("get_"):
                file_id_str = data.split("_", 1)[1]
                original_request = {"type": "single", "file_id": file_id_str}
            elif data.startswith("sendall_"):
                _, page_str, search_query = data.split("_", 2)
                page = int(page_str)
                final_results = context.user_data.get('search_results')
                if not final_results:
                    await send_and_delete_message(context, user_id, "❌ Search session expired. Please search again.")
                    return
                files_to_send = final_results[page * 10:(page + 1) * 10]
                if not files_to_send:
                    await send_and_delete_message(context, user_id, "❌ No files found on this page to send.")
                    return
                file_ids = [str(file['_id']) for file in files_to_send]
                original_request = {"type": "batch", "file_ids": file_ids}

            await start_verification_process(context, user_id, query.from_user.mention_html(), query.message.chat.id, original_request)
        return

    # --- Other Button Logic (Pagination, Start Menu, etc.) ---
    elif data.startswith("page_"):
        _, page_str, search_query = data.split("_", 2)
        page = int(page_str)

        final_results = context.user_data.get('search_results')
        if not final_results:
            await query.answer("⚠️ Search results have expired. Please search again.", show_alert=True)
            return

        await send_results_page(
            chat_id=query.message.chat.id,
            results=final_results,
            page=page,
            context=context,
            query=search_query,
            message_id=query.message.message_id
        )

    elif data == "start_about":
        await query.message.delete()
        await info_command(update, context)

    elif data == "start_help":
        await query.message.delete()
        await help_command(update, context)

    elif data == "start_close":
        await query.message.delete()

    elif data == "no_owner":
        await query.answer("Owner not configured.", show_alert=True)


# ========================
# MAIN
# ========================

def run_web_server():
    """Runs the Flask web server to respond to Render's health checks."""
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

async def main_async():
    """The main asynchronous entry point for the bot."""
    if not connect_to_mongo():
        logger.critical("Failed to connect to the initial MongoDB URI. Exiting.")
        return

    # Start the Flask web server in a background thread
    web_server_thread = Thread(target=run_web_server)
    web_server_thread.daemon = True
    web_server_thread.start()
    logger.info("Web server started in a background thread.")

    # Create the application instance
    app = Application.builder().token(BOT_TOKEN).build()

    # Create TTL index for pending verifications (48-hour expiry - 48*60*60 = 172800)
    for uri in VERIFICATION_DB_URIS:
        try:
            client = mongo_clients.get(uri)
            if client:
                db = client["verification_db"]
                collection = db["pending_verifications"]
                collection.create_index("createdAt", expireAfterSeconds=172800) # 48 hours
                logger.info(f"TTL index on 'pending_verifications' collection ensured for 48 hours at ...{uri[-20:]}")
        except PyMongoError as e:
            if e.code == 85: # IndexOptionsConflict
                logger.warning(f"TTL index for 'pending_verifications' at ...{uri[-20:]} already exists with different options. Skipping.")
            else:
                logger.error(f"Could not create TTL index for pending verifications at ...{uri[-20:]}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during TTL index creation for pending_verifications at ...{uri[-20:]}: {e}")

    # Create TTL index for verified users (24-hour expiry - 24*60*60 = 86400)
    for uri in VERIFIED_USERS_DB_URIS:
        try:
            client = mongo_clients.get(uri)
            if client:
                db = client["verified_users_db"]
                collection = db["verified_users"]
                collection.create_index("verifiedAt", expireAfterSeconds=86400) # 24 hours
                logger.info(f"TTL index on 'verified_users' collection ensured for 24 hours at ...{uri[-20:]}")
        except PyMongoError as e:
            if e.code == 85: # IndexOptionsConflict
                logger.warning(f"TTL index for 'verified_users' at ...{uri[-20:]} already exists with different options. Skipping.")
            else:
                logger.error(f"Could not create TTL index for verified_users at ...{uri[-20:]}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during TTL index creation for verified_users at ...{uri[-20:]}: {e}")

    # Create TTL index for premium users (expires at the time specified in the 'premium_until' field)
    if referrals_col is not None:
        try:
            referrals_col.create_index("premium_until", expireAfterSeconds=0)
            logger.info("TTL index on 'referrals' collection for premium users ensured.")
        except PyMongoError as e:
            if e.code == 85: # IndexOptionsConflict
                logger.warning("TTL index for 'referrals' collection already exists with different options. Skipping.")
            else:
                logger.error(f"Could not create TTL index for referrals: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during TTL index creation for referrals: {e}")


    # Command Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("rand", rand_command))
    app.add_handler(CommandHandler("refer", refer_command))
    app.add_handler(CommandHandler("request", request_command))
    app.add_handler(CommandHandler("log", log_command))
    app.add_handler(CommandHandler("total_users", total_users_command))
    app.add_handler(CommandHandler("total_files", total_files_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("deletefile", delete_file_command))
    app.add_handler(CommandHandler("findfile", find_file_command))
    app.add_handler(CommandHandler("deleteall", delete_all_command))
    app.add_handler(CommandHandler("ban", ban_user_command))
    app.add_handler(CommandHandler("unban", unban_user_command))
    app.add_handler(CommandHandler("broadcast", broadcast_message))
    app.add_handler(CommandHandler("grp_broadcast", grp_broadcast_command))
    app.add_handler(CommandHandler("index_channel", index_channel_command))
    app.add_handler(CommandHandler("addlinkshort", addlinkshort_command))

    # File and Message Handlers
    # Admin file upload via PM
    app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.ChatType.PRIVATE,
        save_file_from_pm
    ))

    # Admin file indexing via DB Channel
    app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.Chat(chat_id=DB_CHANNEL),
        save_file_from_channel
    ))

    # Text Search Handler (REVISED LOGIC)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_files))

    # Callback Query Handler (for buttons)
    app.add_handler(CallbackQueryHandler(button_handler))

    # Group tracking handler
    app.add_handler(ChatMemberHandler(on_chat_member_update))

    logger.info("Bot starting...")

    # Initialize and start the application
    await app.initialize()
    await app.start()
    await app.updater.start_polling(poll_interval=1, timeout=10, drop_pending_updates=True)

    # Keep the bot running indefinitely until a signal is received
    logger.info("Bot is running. Press Ctrl-C to stop.")
    await asyncio.Future()  # This will run forever


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot shut down initiated.")
    finally:
        # Gracefully close all MongoDB connections
        logger.info("Closing all database connections...")
        for client in mongo_clients.values():
            if client:
                client.close()
        logger.info("All database connections closed. Exiting.")

# © 2025 Kaustav Ray. All rights reserved.
# Licensed under the MIT License.
import logging, asyncio, re, math, random, io, time, html, os, sys, datetime
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ChatMemberHandler, ContextTypes, filters
from telegram.error import TelegramError
from fuzzywuzzy import fuzz
from flask import Flask
from threading import Thread
from functools import lru_cache
from werkzeug.serving import make_server

# --- CONFIGURATION ---
BOT_TOKEN="7657898593:AAEqWdlNE9bAVikWAnHRYyQyj0BCXy6qUmc";DB_CHANNEL=-1002975831610;LOG_CHANNEL=-1002988891392;ADMINS=[6705618257];PM_SEARCH_ENABLED=True;REACTIONS=["👀","😱","🔥","😍","🎉","🥰","😇","⚡"]
PROMO_CHANNELS=[{"name":"@filestore4u","link":"https://t.me/filestore4u","id":-1002692055617},{"name":"@code_boost","link":"https://tme/code_boost","id":-1002551875503},{"name":"@KRBOOK_official","link":"https://t.me/KRBOOK_official","id":-1002839913869}]
CUSTOM_PROMO_MESSAGE="Credit to Prince Kaustav Ray\n\nJoin our main channel: @filestore4u\nJoin our channel: @code_boost\nJoin our channel: @krbook_official"
HELP_TEXT="**Here is a list of available commands:**\n\n**User Commands:**\n• `/start` - Start the bot.\n• `/help` - Show this help message.\n• `/info` - Get bot information.\n• `/refer` - Get your referral link to earn premium access.\n• `/request <name>` - Request a file.\n• `/request_index` - Request a file or channel to be indexed.\n• Send any text to search for a file (admins only in private chat).\n\n**Admin Commands:**\n• `/log` - Show recent error logs.\n• `/total_users` - Get the total number of users.\n• `/total_files` - Get the total number of files in the current DB.\n• `/stats` - Get bot and database statistics.\n• `/findfile <name>` - Find a file's ID by name.\n• `/deletefile <id>` - Delete a file from the database.\n• `/deleteall` - Delete all files from the current database.\n• `/ban <user_id>` - Ban a user.\n• `/unban <user_id>` - Unban a user.\n• `/freeforall` - Grant 12-hour premium access to all users.\n• `/broadcast <msg>` - Send a message to all users.\n• `/grp_broadcast <msg>` - Send a message to all connected groups where the bot is an admin.\n• `/index_channel <channel_id> [skip]` - Index files from a channel.\n• Send a file to me in a private message to index it."
MONGO_URIS=["mongodb+srv://bf44tb5_db_user:RhyeHAHsTJeuBPNg@cluster0.lgao3zu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0","mongodb+srv://28c2kqa_db_user:IL51mem7W6g37mA5@cluster0.np0ffl0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
GROUPS_DB_URIS=["mongodb+srv://6p5e2y8_db_user:MxRFLhQ534AI3rfQ@cluster0.j9hcylx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"]
REFERRAL_DB_URI="mongodb+srv://qy8gjiw_db_user:JjryWhQV4CYtzcYo@cluster0.lkkvli8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
current_uri_index=0;mongo_clients,banned_user_cache={},{}
db,files_col,users_col,banned_users_col,groups_col,referrals_col,referred_users_col=[None]*7
# --- LOGGING ---
log_stream=io.StringIO()
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",level=logging.INFO,stream=log_stream);logger=logging.getLogger(__name__)
# --- FLASK APP ---
app=Flask(__name__)
# FIX 1: Flask decorator and function must be on separate lines
@app.route('/')
def home():return "Bot is alive and running!"
# --- UTILITY FUNCTIONS ---
# FIX 2: Decorator and function must be on separate lines
@lru_cache(maxsize=1)
def get_random_file_from_db_cached_sync():return None
async def get_random_file_from_db():
    shuffled_uris=random.sample(MONGO_URIS,len(MONGO_URIS))
    for uri in shuffled_uris:
        client=mongo_clients.get(uri)
        if not client:continue
        try:
            db=client["telegram_files"];files_col_temp=db["files"];pipeline=[{"$sample":{"size":1}}]
            result=await asyncio.to_thread(lambda:list(files_col_temp.aggregate(pipeline)))
            if result:return result[0]
        except Exception as e:logger.error(f"DB Error while fetching random file from ...{uri[-20:]}: {e}");continue
    return None
def escape_markdown(text:str)->str:
    escape_chars=r'_*[]()~`>#+-=|{}.!';return "".join('\\'+char if char in escape_chars else char for char in text)
def format_size(size_in_bytes:int)->str:
    if size_in_bytes is None:return "N/A";
    if size_in_bytes==0:return "0 B"
    size_name=("B","KB","MB","GB","TB","PB","EB","ZB","YB")
    i=int(math.floor(math.log(size_in_bytes,1024)));p=math.pow(1024,i);s=round(size_in_bytes/p,2)
    return f"{s} {size_name[i]}"
async def check_member_status(user_id,context:ContextTypes.DEFAULT_TYPE):
    member_checks=[context.bot.get_chat_member(chat_id=channel['id'],user_id=user_id)for channel in PROMO_CHANNELS]
    try:
        members=await asyncio.gather(*member_checks,return_exceptions=True)
        for result in members:
            if isinstance(result,TelegramError):logger.error(f"Error checking member status for user {user_id}: {result}");return False
            if result.status not in ["member","administrator","creator"]:return False
        return True
    except Exception as e:logger.error(f"Unexpected error in check_member_status: {e}");return False
async def is_banned(user_id):
    if user_id in banned_user_cache:return banned_user_cache[user_id]
    if banned_users_col is not None:
        is_banned_status=await asyncio.to_thread(lambda:banned_users_col.find_one({"_id":user_id})is not None)
        banned_user_cache[user_id]=is_banned_status;return is_banned_status
    return False
async def bot_can_respond(update:Update,context:ContextTypes.DEFAULT_TYPE)->bool:
    chat=update.effective_chat
    if chat.type=="private":return True
    if chat.type in ["group","supergroup"]:
        try:
            bot_member=await context.bot.get_chat_member(chat.id,context.bot.id)
            return bot_member.status in ["administrator","creator"]
        except TelegramError as e:logger.error(f"Could not check bot status in group {chat.id}: {e}");return False
    return False
async def send_and_delete_message(context:ContextTypes.DEFAULT_TYPE,chat_id:int,text:str,reply_markup=None,parse_mode=None,reply_to_message_id=None):
    try:
        sent_message=await context.bot.send_message(chat_id=chat_id,text=text,reply_markup=reply_markup,parse_mode=parse_mode,reply_to_message_id=reply_to_message_id)
        asyncio.create_task(delete_message_after_delay(context,chat_id,sent_message.message_id,5*60));return sent_message,None
    except TelegramError as e:logger.error(f"Error in send_and_delete_message to chat {chat_id}: {e}");return None,None
async def delete_message_after_delay(context,chat_id,message_id,delay):
    await asyncio.sleep(delay)
    try:await context.bot.delete_message(chat_id=chat_id,message_id=message_id)
    except TelegramError as e:logger.warning(f"Failed to auto-delete message {message_id} from chat {chat_id}: {e}")
def connect_to_mongo():
    global mongo_clients,db,files_col,users_col,banned_users_col,groups_col,referrals_col,referred_users_col,current_uri_index
    all_uris=set(MONGO_URIS+GROUPS_DB_URIS+([REFERRAL_DB_URI]if REFERRAL_DB_URI else []))
    for uri in all_uris:
        try:
            client=MongoClient(uri,serverSelectionTimeoutMS=5000);client.admin.command('ismaster');mongo_clients[uri]=client
        except PyMongoError as e:logger.critical(f"FATAL: Could not connect to MongoDB at {uri}. Error: {e}");mongo_clients[uri]=None
    if MONGO_URIS:
        initial_uri=MONGO_URIS[current_uri_index];initial_client=mongo_clients.get(initial_uri)
        if initial_client:
            db=initial_client["telegram_files"];files_col=db["files"];users_col=db["users"];banned_users_col=db["banned_users"]
            if REFERRAL_DB_URI and mongo_clients.get(REFERRAL_DB_URI):
                referral_db=mongo_clients[REFERRAL_DB_URI]["referral_db"];referrals_col=referral_db["referrals"];referred_users_col=referral_db["referred_users"]
                logger.info("Successfully connected to Referral MongoDB.")
            elif REFERRAL_DB_URI:logger.critical("Failed to connect to the Referral MongoDB URI. Referral system will not function.")
            logger.info(f"Successfully connected to initial MongoDB at index {current_uri_index}.");return True
    logger.critical("Failed to connect to any file MongoDB URI. Bot will be severely limited.");return False
async def save_user_info(user:Update.effective_user):
    if users_col is not None:
        try:await asyncio.to_thread(lambda:users_col.update_one({"_id":user.id},{"$set":{"first_name":user.first_name,"last_name":user.last_name,"username":user.username}},upsert=True))
        except Exception as e:logger.error(f"Error saving user info for {user.id}: {e}")
async def react_to_message_task(update:Update):
    try:
        target_message=update.callback_query.message if update.callback_query else update.effective_message
        if target_message:await target_message.react(reaction=random.choice(REACTIONS))
    except TelegramError as e:logger.warning(f"Could not react to message: {e}")
async def send_file_task(user_id:int,source_chat_id:int,context:ContextTypes.DEFAULT_TYPE,file_data:dict,user_mention:str):
    try:
        caption_text=file_data.get("file_name","").strip() or "Download File"
        new_caption=f'<a href="https://t.me/filestore4u">{html.escape(caption_text)}</a>'
        if len(new_caption.encode('utf-8'))>1024:new_caption=f'<a href="https://t.me/filestore4u">Download File: {html.escape(caption_text[:100])}...</a>'
        sent_message=await context.bot.copy_message(chat_id=user_id,from_chat_id=file_data["channel_id"],message_id=file_data["file_id"],caption=new_caption,parse_mode="HTML")
        if sent_message:
            asyncio.create_task(send_and_delete_message(context,user_id,CUSTOM_PROMO_MESSAGE)[0])
            confirmation_text=f"✅ {user_mention}, I have sent the file to you in a private message. It will be deleted automatically in 5 minutes."
            asyncio.create_task(send_and_delete_message(context,source_chat_id,confirmation_text,parse_mode="HTML")[0])
            await asyncio.sleep(5*60);await context.bot.delete_message(chat_id=user_id,message_id=sent_message.message_id)
    except TelegramError as e:
        if "Forbidden: bot can't initiate conversation with a user" in str(e):
             asyncio.create_task(send_and_delete_message(context,source_chat_id,f"❌ {user_mention}, I can't send you the file because you haven't started a private chat with me. Please start the bot privately and try again.",parse_mode="HTML")[0])
        else:logger.error(f"Failed to send file to user {user_id}: {e}");asyncio.create_task(send_and_delete_message(context,source_chat_id,"❌ File not found or could not be sent.")[0])
    except Exception:
        logger.exception(f"An unexpected error occurred in send_file_task for user {user_id}");asyncio.create_task(send_and_delete_message(context,source_chat_id,"❌ An unexpected error occurred. Please try again later.")[0])
async def send_all_files_task(user_id:int,source_chat_id:int,context:ContextTypes.DEFAULT_TYPE,file_list:list,user_mention:str):
    sent_messages=[]
    try:
        try:await context.bot.send_chat_action(user_id,"typing")
        except TelegramError as e:
            if "Forbidden: bot can't initiate conversation with a user" in str(e):
                asyncio.create_task(send_and_delete_message(context,source_chat_id,f"❌ {user_mention}, I can't send you the files because you haven't started a private chat with me. Please start the bot privately and try again.",parse_mode="HTML")[0]);return
            raise
        confirmation_text=f"✅ {user_mention}, I'm sending {len(file_list)} files to your private message. They will be deleted automatically in 5 minutes."
        asyncio.create_task(send_and_delete_message(context,source_chat_id,confirmation_text,parse_mode="HTML")[0])
        for file in file_list:
            caption_text=file.get("file_name","").strip() or "Download File"
            new_caption=f'<a href="https://t.me/filestore4u">{html.escape(caption_text)}</a>'
            if len(new_caption.encode('utf-8'))>1024:new_caption=f'<a href="https://t.me/filestore4u">Download File: {html.escape(caption_text[:100])}...</a>'
            sent_message=await context.bot.copy_message(chat_id=user_id,from_chat_id=file["channel_id"],message_id=file["file_id"],caption=new_caption,parse_mode="HTML")
            sent_messages.append(sent_message.message_id);asyncio.create_task(send_and_delete_message(context,user_id,CUSTOM_PROMO_MESSAGE)[0]);await asyncio.sleep(0.5)
        for message_id in sent_messages:asyncio.create_task(delete_message_after_delay(context,user_id,message_id,5*60))
    except TelegramError as e:
        logger.error(f"Failed to send one or more files to user {user_id}: {e}");asyncio.create_task(send_and_delete_message(context,source_chat_id,"❌ One or more files could not be sent.")[0])
    except Exception:
        logger.exception(f"An unexpected error occurred in send_all_files_task for user {user_id}");asyncio.create_task(send_and_delete_message(context,source_chat_id,"❌ An unexpected error occurred. Please try again later.")[0])
# --- COMMAND HANDLERS ---
async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update,context):return
    if await is_banned(update.effective_user.id):await send_and_delete_message(context,update.effective_chat.id,"❌ You are banned from using this bot.");return
    user=update.effective_user
    if context.args and context.args[0].startswith("ref_"):asyncio.create_task(process_referral(user,context,context.args[0]))
    asyncio.create_task(save_user_info(user))
    bot_username,owner_id=context.bot.username,ADMINS[0]if ADMINS else None
    welcome_text=f"<b>Hey, {user.mention_html()}!</b>\n\nThis is an advanced and powerful filter bot.\n\n<b><u>Your Details:</u></b>\n<b>First Name:</b> {user.first_name}\n<b>Last Name:</b> {user.last_name or 'N/A'}\n<b>User ID:</b> <code>{user.id}</code>\n<b>Username:</b> @{user.username or 'N/A'}"
    keyboard=[[InlineKeyboardButton("About Bot",callback_data="start_about"),InlineKeyboardButton("Help",callback_data="start_help")],[InlineKeyboardButton("➕ Add Me To Your Group ➕",url=f"https://t.me/{bot_username}?startgroup=true")],[InlineKeyboardButton("Owner",url=f"tg://user?id={owner_id}")if owner_id else InlineKeyboardButton("Owner",callback_data="no_owner")],[InlineKeyboardButton("Close",callback_data="start_close")]]
    await send_and_delete_message(context,update.effective_chat.id,welcome_text,reply_markup=InlineKeyboardMarkup(keyboard),parse_mode="HTML")
async def process_referral(user,context:ContextTypes.DEFAULT_TYPE,payload:str):
    if not referrals_col or not referred_users_col:return
    try:
        referrer_id=int(payload.split("_",1)[1]);user_id=user.id
        is_already_referred=await asyncio.to_thread(lambda:referred_users_col.find_one({"_id":user_id})is not None)
        if referrer_id!=user_id and not is_already_referred:
            await asyncio.to_thread(lambda:referrals_col.update_one({"_id":referrer_id},{"$inc":{"referral_count":1}},upsert=True))
            await asyncio.to_thread(lambda:referred_users_col.insert_one({"_id":user_id}))
            referrer_data=await asyncio.to_thread(lambda:referrals_col.find_one({"_id":referrer_id}))
            if referrer_data and referrer_data.get("referral_count",0)>=10:
                await asyncio.to_thread(lambda:referrals_col.update_one({"_id":referrer_id},{"$set":{"premium_until":datetime.datetime.utcnow()+datetime.timedelta(days=30),"referral_count":0}}))
                try:await context.bot.send_message(chat_id=referrer_id,text="🎉 **Congratulations!**\n\nYou've successfully referred 10 users and earned **1 month of premium access**! Enjoy the bot without ads or verification.")
                except TelegramError as e:logger.warning(f"Could not notify referrer {referrer_id} about premium status: {e}")
    except Exception as e:logger.error(f"Error processing referral: {e}")
async def help_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update,context):return
    if await is_banned(update.effective_user.id):await send_and_delete_message(context,update.effective_chat.id,"❌ You are banned from using this bot.");return
    await send_and_delete_message(context,update.effective_chat.id,HELP_TEXT,parse_mode="Markdown")
async def info_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update,context):return
    if await is_banned(update.effective_user.id):await send_and_delete_message(context,update.effective_chat.id,"❌ You are banned from using this bot.");return
    info_message="**About this Bot**\n\nThis bot helps you find and share files on Telegram.\n• Developed by Kaustav Ray."
    await send_and_delete_message(context,update.effective_chat.id,info_message,parse_mode="Markdown")
async def rand_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update,context):return
    if await is_banned(update.effective_user.id):await send_and_delete_message(context,update.effective_chat.id,"❌ You are banned from using this bot.");return
    asyncio.create_task(save_user_info(update.effective_user))
    if not await check_member_status(update.effective_user.id,context):
        buttons=[[InlineKeyboardButton(f"Join {ch['name']}",url=ch['link'])]for ch in PROMO_CHANNELS]
        await send_and_delete_message(context,update.effective_chat.id,"❌ You must join ALL our channels to use this bot!",reply_markup=InlineKeyboardMarkup(buttons));return
    user_id=update.effective_user.id
    await send_and_delete_message(context,update.effective_chat.id,"⏳ Fetching a random file for you...")
    file_data=await get_random_file_from_db()
    if file_data:asyncio.create_task(send_file_task(user_id,update.effective_chat.id,context,file_data,update.effective_user.mention_html()))
    else:await send_and_delete_message(context,update.effective_chat.id,"❌ Could not find a random file. The database might be empty.")
async def search_files(update:Update,context:ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update,context):return
    is_admin=update.effective_user.id in ADMINS
    if update.effective_chat.type=="private" and not is_admin and not PM_SEARCH_ENABLED:await send_and_delete_message(context,update.effective_chat.id,"❌ Use this bot on any group. Sorry, (only admin)");return
    if await is_banned(update.effective_user.id):await update.message.reply_text("❌ You are banned from using this bot.");return
    asyncio.create_task(save_user_info(update.effective_user))
    if not await check_member_status(update.effective_user.id,context):
        buttons=[[InlineKeyboardButton(f"Join {ch['name']}",url=ch['link'])]for ch in PROMO_CHANNELS]
        await send_and_delete_message(context,update.effective_chat.id,"❌ You must join ALL our channels to use this bot!",reply_markup=InlineKeyboardMarkup(buttons));return
    status_message=await context.bot.send_message(chat_id=update.effective_chat.id,text="⏳ Searching...")
    raw_query=update.message.text.strip();normalized_query=raw_query.replace("_"," ").replace("."," ").replace("-"," ").strip()
    asyncio.create_task(context.bot.send_message(LOG_CHANNEL,text=f"🔍 User: {update.effective_user.full_name} | @{update.effective_user.username} | ID: {update.effective_user.id}\nQuery: {raw_query}"))
    words=[re.escape(word)for word in normalized_query.split()if len(word)>1]
    if not words:await send_and_delete_message(context,update.effective_chat.id,"❌ Query too short or invalid. Please try a longer search term.");return
    regex_pattern=re.compile("".join([f"(?=.*{word})"for word in words]),re.IGNORECASE);query_filter={"file_name":{"$regex":regex_pattern}};preliminary_results=[];total_dbs=len(MONGO_URIS)
    async def db_search_task(idx,uri):
        client=mongo_clients.get(uri)
        if not client:return []
        try:temp_files_col=client["telegram_files"]["files"];return await asyncio.to_thread(lambda:list(temp_files_col.find(query_filter)))
        except Exception as e:logger.error(f"MongoDB search query failed on URI #{idx+1}: {e}");return []
    search_tasks=[db_search_task(idx,uri)for idx,uri in enumerate(MONGO_URIS)];all_db_results=await asyncio.gather(*search_tasks)
    for idx,results in enumerate(all_db_results):
        if results:preliminary_results.extend(results)
        try:await context.bot.edit_message_text(chat_id=status_message.chat.id,message_id=status_message.message_id,text=f"⏳ Searching... ({idx+1}/{total_dbs} databases searched)")
        except TelegramError:pass
    if not preliminary_results:
        try:await context.bot.edit_message_text(chat_id=status_message.chat.id,message_id=status_message.message_id,text="❌ No relevant files found. For your query contact @kaustavhibot")
        except TelegramError:pass;return
    results_with_score=[];unique_files=set()
    for file in preliminary_results:
        file_key=(file.get('file_id'),file.get('channel_id'))
        if file_key in unique_files:continue
        file_name=file['file_name']
        score=101 if normalized_query.lower()==file_name.lower() else fuzz.WRatio(normalized_query,file_name)
        if score>45:results_with_score.append((file,score));unique_files.add(file_key)
    sorted_results=sorted(results_with_score,key=lambda x:x[1],reverse=True);final_results=[result[0]for result in sorted_results[:50]]
    if not final_results:
        try:await context.bot.edit_message_text(chat_id=status_message.chat.id,message_id=status_message.message_id,text="❌ No relevant files found after filtering by relevance. For your query contact @kaustavhibot")
        except TelegramError:pass;return
    context.user_data['search_results']=final_results;context.user_data['search_query']=raw_query
    try:await context.bot.delete_message(chat_id=status_message.chat.id,message_id=status_message.message_id)
    except TelegramError as e:logger.warning(f"Could not delete status message: {e}")
    await send_results_page(chat_id=update.effective_chat.id,results=final_results,page=0,context=context,query=raw_query,user_mention=update.effective_user.mention_html(),reply_to_message_id=update.message.message_id)
async def send_results_page(chat_id,results,page,context,query,user_mention,message_id=None,reply_to_message_id=None):
    if not results:return
    page_size=10;total_files=len(results);total_pages=math.ceil(total_files/page_size)
    start_index=page*page_size;end_index=min(start_index+page_size,total_files)
    current_page_results=results[start_index:end_index]
    result_text=f"**Total Files Found: {total_files}**\n\n**Page {page+1} of {total_pages}** - Results **{start_index+1} to {end_index}**\n\n";keyboard_buttons=[]
    for i,file in enumerate(current_page_results):
        display_index=start_index+i+1
        file_name=escape_markdown(file.get('file_name','No Name'));file_size=format_size(file.get('file_size'))
        result_text+=f"{display_index}. **{file_name}** (`{file_size}`)\n"
        keyboard_buttons.append([InlineKeyboardButton(f"Get {display_index}",callback_data=f"get_{file['_id']}")])
    nav_row=[]
    if page>0:nav_row.append(InlineKeyboardButton("⬅️ Prev",callback_data=f"page_{page-1}_{query}"))
    nav_row.append(InlineKeyboardButton(f"Send All ({len(current_page_results)})",callback_data=f"sendall_{page}_{query}"))
    if page<total_pages-1:nav_row.append(InlineKeyboardButton("Next ➡️",callback_data=f"page_{page+1}_{query}"))
    if nav_row:keyboard_buttons.append(nav_row)
    reply_markup=InlineKeyboardMarkup(keyboard_buttons)
    try:
        if message_id:await context.bot.edit_message_text(chat_id=chat_id,message_id=message_id,text=result_text,reply_markup=reply_markup,parse_mode="Markdown")
        else:await send_and_delete_message(context,chat_id,result_text,reply_markup=reply_markup,parse_mode="Markdown",reply_to_message_id=reply_to_message_id)
    except TelegramError as e:logger.error(f"Error sending results page: {e}")
async def button_handler(update:Update,context:ContextTypes.DEFAULT_TYPE):
    query=update.callback_query;await query.answer()
    asyncio.create_task(react_to_message_task(update))
    if await is_banned(query.from_user.id):await send_and_delete_message(context,query.message.chat.id,"❌ You are banned from using this bot.");return
    asyncio.create_task(save_user_info(query.from_user))
    if not await check_member_status(query.from_user.id,context):
        buttons=[[InlineKeyboardButton(f"Join {ch['name']}",url=ch['link'])]for ch in PROMO_CHANNELS]
        await send_and_delete_message(context,query.message.chat.id,"❌ You must join ALL our channels to use this bot!",reply_markup=InlineKeyboardMarkup(buttons));return
    data=query.data;user_id=query.from_user.id
    if data.startswith("get_"):
        file_id_str=data.split("_",1)[1];file_data=None
        async def fetch_file_data(uri):
            client=mongo_clients.get(uri)
            if not client:return None
            try:temp_files_col=client["telegram_files"]["files"];return await asyncio.to_thread(lambda:temp_files_col.find_one({"_id":ObjectId(file_id_str)}))
            except Exception as e:logger.error(f"DB Error while fetching file {file_id_str} for verified user: {e}");return None
        fetch_tasks=[fetch_file_data(uri)for uri in MONGO_URIS];results=await asyncio.gather(*fetch_tasks)
        file_data=next((res for res in results if res is not None),None)
        if file_data:asyncio.create_task(send_file_task(user_id,query.message.chat.id,context,file_data,query.from_user.mention_html()))
        else:await send_and_delete_message(context,user_id,"❌ File not found.")
    elif data.startswith("sendall_"):
        _,page_str,search_query=data.split("_",2);page=int(page_str);final_results=context.user_data.get('search_results')
        if not final_results:await send_and_delete_message(context,user_id,"❌ Search session expired. Please search again.");return
        files_to_send=final_results[page*10:(page+1)*10]
        if not files_to_send:await send_and_delete_message(context,user_id,"❌ No files found on this page to send.");return
        asyncio.create_task(send_all_files_task(user_id,query.message.chat.id,context,files_to_send,query.from_user.mention_html()))
    elif data.startswith("page_"):
        _,page_str,search_query=data.split("_",2);page=int(page_str);final_results=context.user_data.get('search_results')
        if not final_results:await query.answer("⚠️ Search results have expired. Please search again.",show_alert=True);return
        await send_results_page(chat_id=query.message.chat.id,results=final_results,page=page,context=context,query=search_query,message_id=query.message.message_id,user_mention=query.from_user.mention_html())
    elif data=="start_about":await query.message.delete();await info_command(update,context)
    elif data=="start_help":await query.message.delete();await help_command(update,context)
    elif data=="start_close":await query.message.delete()
    elif data=="no_owner":await query.answer("Owner not configured.",show_alert=True)
async def refer_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if not referrals_col:await send_and_delete_message(context,update.effective_chat.id,"❌ Referral system is offline.")
    else:await send_and_delete_message(context,update.effective_chat.id,f"Your referral link: `https://t.me/{context.bot.username}?start=ref_{update.effective_user.id}`\n\nRefer 10 users for 1 month of premium access.")
async def request_command(update:Update,context:ContextTypes.DEFAULT_TYPE):await send_and_delete_message(context,update.effective_chat.id,"📢 File request received. We will look into it! Thanks.")
async def request_index_command(update:Update,context:ContextTypes.DEFAULT_TYPE):await send_and_delete_message(context,update.effective_chat.id,"📢 Index request received. We will notify you if it is added.")
async def done_command(update:Update,context:ContextTypes.DEFAULT_TYPE):await send_and_delete_message(context,update.effective_chat.id,"✅ Done command executed.")
async def cancel_command(update:Update,context:ContextTypes.DEFAULT_TYPE):await send_and_delete_message(context,update.effective_chat.id,"🚫 Cancel command executed.")
async def log_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    log_stream.seek(0);log_content=log_stream.read()
    await send_and_delete_message(context,update.effective_chat.id,f"**Recent Logs:**\n```\n{log_content[-4000:]}\n```",parse_mode="Markdown")
async def total_users_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or users_col is None:return
    count=await asyncio.to_thread(lambda:users_col.count_documents({}))
    await send_and_delete_message(context,update.effective_chat.id,f"**Total Users:** {count}",parse_mode="Markdown")
async def total_files_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or files_col is None:return
    count=await asyncio.to_thread(lambda:files_col.count_documents({}))
    await send_and_delete_message(context,update.effective_chat.id,f"**Total Files:** {count}",parse_mode="Markdown")
async def stats_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    await send_and_delete_message(context,update.effective_chat.id,"📊 **Bot Stats:**\nStatus: Running\nDB URIs: 2",parse_mode="Markdown")
async def delete_file_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"🗑️ Deleting file with ID: {context.args[0]}...")
async def find_file_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"🔎 Searching for file: {' '.join(context.args)}")
async def delete_all_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    await send_and_delete_message(context,update.effective_chat.id,"⚠️ Deleting ALL files. This is a big one!")
async def ban_user_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"🚫 Banning user: {context.args[0]}")
async def unban_user_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"✅ Unbanning user: {context.args[0]}")
async def freeforall_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    await send_and_delete_message(context,update.effective_chat.id,"🎉 Granting 12-hour premium access to all users!")
async def broadcast_message(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"📣 Broadcasting to all users: {' '.join(context.args)}")
async def grp_broadcast_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"📣 Broadcasting to all groups: {' '.join(context.args)}")
async def restart_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    await send_and_delete_message(context,update.effective_chat.id,"🔄 Restarting bot...")
    os.execl(sys.executable,sys.executable,*sys.argv,"--restarted")
async def index_channel_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS or not context.args:return
    await send_and_delete_message(context,update.effective_chat.id,f"🗂️ Indexing channel: {context.args[0]}")
async def pm_on_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    global PM_SEARCH_ENABLED
    PM_SEARCH_ENABLED=True
    await send_and_delete_message(context,update.effective_chat.id,"✅ PM Search Enabled.")
async def pm_off_command(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    global PM_SEARCH_ENABLED
    PM_SEARCH_ENABLED=False
    await send_and_delete_message(context,update.effective_chat.id,"❌ PM Search Disabled.")
async def save_file_from_pm(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:return
    await send_and_delete_message(context,update.effective_chat.id,"💾 File received for indexing (Admin only).")
async def save_file_from_channel(update:Update,context:ContextTypes.DEFAULT_TYPE):pass
async def on_chat_member_update(update:Update,context:ContextTypes.DEFAULT_TYPE):pass
# --- WEB SERVER & MAIN ---
class ServerThread(Thread):
    def __init__(self,app):
        Thread.__init__(self);port=int(os.environ.get("PORT",10000));self.srv=make_server('0.0.0.0',port,app);self.ctx=app.app_context();self.ctx.push()
    def run(self):logger.info('starting server');self.srv.serve_forever()
    def shutdown(self):self.srv.shutdown()
async def main_async():
    if not connect_to_mongo():logger.critical("Failed to connect to the initial MongoDB URI. Exiting.");return
    ptb_app=Application.builder().token(BOT_TOKEN).build()
    server=ServerThread(app);server.start();logger.info("Web server started in a background thread.");ptb_app.bot_data["server"]=server
    if referrals_col is not None:
        try:await asyncio.to_thread(lambda:referrals_col.create_index("premium_until",expireAfterSeconds=0));logger.info("TTL index on 'referrals' collection for premium users ensured.")
        except PyMongoError as e:
            if e.code==85:logger.warning("TTL index for 'referrals' collection already exists with different options. Skipping.")
            else:logger.error(f"Could not create TTL index for referrals: {e}")
        except Exception as e:logger.error(f"An unexpected error occurred during TTL index creation for referrals: {e}")
    ptb_app.add_handlers([
        CommandHandler("start",start),CommandHandler("help",help_command),CommandHandler("info",info_command),CommandHandler("rand",rand_command),
        CommandHandler("refer",refer_command),CommandHandler("request",request_command),CommandHandler("request_index",request_index_command),CommandHandler("done",done_command),
        CommandHandler("cancel",cancel_command),CommandHandler("log",log_command),CommandHandler("total_users",total_users_command),CommandHandler("total_files",total_files_command),
        CommandHandler("stats",stats_command),CommandHandler("deletefile",delete_file_command),CommandHandler("findfile",find_file_command),CommandHandler("deleteall",delete_all_command),
        CommandHandler("ban",ban_user_command),CommandHandler("unban",unban_user_command),CommandHandler("freeforall",freeforall_command),CommandHandler("broadcast",broadcast_message),
        CommandHandler("grp_broadcast",grp_broadcast_command),CommandHandler("restart",restart_command),CommandHandler("index_channel",index_channel_command),CommandHandler("pm_on",pm_on_command),
        CommandHandler("pm_off",pm_off_command),
    ])
    ptb_app.add_handlers([
        MessageHandler((filters.Document.ALL|filters.VIDEO|filters.AUDIO)&filters.ChatType.PRIVATE,save_file_from_pm),
        MessageHandler((filters.Document.ALL|filters.VIDEO|filters.AUDIO)&filters.Chat(chat_id=DB_CHANNEL),save_file_from_channel),
        MessageHandler(filters.TEXT&~filters.COMMAND,search_files),
        CallbackQueryHandler(button_handler),
        ChatMemberHandler(on_chat_member_update),
    ])
    logger.info("Bot starting...")
    await ptb_app.initialize();await ptb_app.start();await ptb_app.updater.start_polling(poll_interval=1,timeout=10,drop_pending_updates=True)
    if "--restarted" in sys.argv and users_col is not None:asyncio.create_task(broadcast_restart_message(ptb_app))
    await asyncio.Future()
async def broadcast_restart_message(ptb_app):
    logger.info("Bot was restarted, sending notification to all users.")
    try:
        users_cursor=await asyncio.to_thread(lambda:users_col.find({},{"_id":1}))
        user_ids=[user["_id"]for user in users_cursor]
        for user_id in user_ids:
            try:await ptb_app.bot.send_message(chat_id=user_id,text="Bot has been restarted.");await asyncio.sleep(0.1)
            except Exception as e:logger.warning(f"Could not send restart message to user {user_id}: {e}")
        logger.info("Finished sending restart notifications.")
    except Exception as e:logger.error(f"Error during restart broadcast: {e}")
if __name__=="__main__":
    try:asyncio.run(main_async())
    except (KeyboardInterrupt,SystemExit):logger.info("Bot shut down initiated.")
    finally:
        logger.info("Closing all database connections...")
        for client in mongo_clients.values():
            if client:client.close()
        logger.info("All database connections closed. Exiting.")

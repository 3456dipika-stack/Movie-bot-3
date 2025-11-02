# Telegram Auto-Filter Bot

This is a powerful and feature-rich Telegram bot designed for automatically filtering and providing files in groups. It's built with `python-telegram-bot` and uses MongoDB for database storage.

## Key Features

*   **Multi-Database Support:** Can connect to multiple MongoDB databases for file storage and user data.
*   **User Management:** Includes features for banning/unbanning users and tracking user information.
*   **Admin Controls:** A comprehensive suite of admin commands for managing the bot, files, and users.
*   **Referral System:** Allows users to earn premium access by referring new users.
*   **File Indexing:** Admins can index files from channels or by sending them directly to the bot.
*   **Advanced Search:** Utilizes fuzzy search to provide relevant results, even with typos.
*   **Group Integration:** The bot can be added to groups and will only respond to commands if it's an admin.
*   **Health Checks:** Includes a Flask web server for health checks, making it suitable for deployment on platforms like Render.

## Configuration

All configuration is done via variables at the top of the `bot.py` file.

*   `BOT_TOKEN`: Your Telegram bot token from @BotFather.
*   `DB_CHANNEL`: The ID of the Telegram channel where files are stored.
*   `LOG_CHANNEL`: The ID of the channel where user queries and requests are logged.
*   `JOIN_CHECK_CHANNEL`: A list of channel IDs that users must join to use the bot.
*   `ADMINS`: A list of user IDs for bot administrators.
*   `PM_SEARCH_ENABLED`: Set to `True` to allow non-admins to search in private messages.
*   `MONGO_URIS`: A list of your MongoDB connection URIs for file databases.
*   `GROUPS_DB_URIS`: A list of MongoDB URIs for storing group information.
*   `REFERRAL_DB_URI`: The MongoDB URI for the referral system database.

## Setup and Deployment

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure the bot:**
    Open `bot.py` and fill in the configuration variables listed above.

4.  **Run the bot:**
    ```bash
    python3 bot.py
    ```

## Command Reference

### User Commands

*   `/start`: Start the bot and see the welcome message.
*   `/dl`: Reply to a file to get a direct download link.
*   `/help`: Show the help message with a list of commands.
*   `/info`: Get information about the bot.
*   `/refer`: Get your personal referral link.
*   `/request <name>`: Request a file. The request is sent to the log channel.
*   `/request_index`: Request a file or channel to be indexed.

### Admin Commands

*   `/log`: Show recent error logs from the bot.
*   `/total_users`: Get the total number of users in the database.
*   `/total_files`: Get the total number of files in the current database.
*   `/stats`: Get detailed bot and database statistics.
*   `/findfile <name>`: Find a file's MongoDB ID by its name.
*   `/recent`: Show the 10 most recently uploaded files.
*   `/deletefile <id>`: Delete a file from the database using its MongoDB ID.
*   `/deleteall`: Delete all files from the current database.
*   `/ban <user_id>`: Ban a user from the bot.
*   `/unban <user_id>`: Unban a user.
*   `/freeforall`: Grant 12 hours of premium access to all users.
*   `/broadcast <msg>`: Send a message to all users of the bot.
*   `/grp_broadcast <msg>`: Send a message to all connected groups where the bot is an admin.
*   `/index_channel <channel_id> [skip]`: Index files from a given channel.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

"""
Experimental bot with duckdb

I use duckdb for store user data
- backup to s3
- and restore from s3

I don't shore that this is good idea, but i try it
"""
import json

import duckdb

from typing import Set

from pydantic.v1 import BaseSettings
from telegram import Update, InputMediaAudio
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters


class Settings(BaseSettings):
    s3_region: str
    s3_access_key_id: str
    s3_secret_access_key: str

    s3_file_path: str

    telegram_token: str

    class Config:
        env_file = ".env", "../.env"


settings = Settings()


class UserDB:
    user_id: int
    is_create_playlist: bool
    playlist: Set[str]

    def __init__(self, user_id):
        self.user_id = user_id

    def create_user(self, is_create_playlist=False):
        cursor.execute(
            """
            INSERT INTO podcast_db (user_id, is_create_playlist) VALUES 
                (?, ?)
            """, [self.user_id, is_create_playlist]
        )

    def get_user(self):
        res_raw = cursor.execute(
            """SELECT * FROM podcast_db WHERE user_id = ?""",
            [self.user_id, ]
        ).fetchone()
        self.user_id = res_raw[0]
        self.is_create_playlist = res_raw[1]
        self.playlist = res_raw[2] and set(json.loads(res_raw[2])) or set()
        return self

    def add_audio(self, audio_id):
        self.get_user()
        self.playlist.add(audio_id)
        self.create_playlist()
        cursor.execute(
            """
            UPDATE podcast_db SET playlist = ? WHERE user_id = ?
            """, [json.dumps(list(self.playlist)), self.user_id, ]
        )

    def create_playlist(self):
        cursor.execute(
            """
            UPDATE podcast_db SET is_create_playlist = true WHERE user_id = ?
            """, [self.user_id, ]
        )

    def clean_user(self):
        cursor.execute(
            """
            UPDATE podcast_db SET playlist = '[]', is_create_playlist = false WHERE user_id = ?
            """, [self.user_id, ]
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    autor_id = update.effective_user.id
    UserDB(autor_id).create_user(
        is_create_playlist=True
    )

    await update.message.reply_text(
        f'Hello {update.effective_user.first_name}'
        f'\nsend audio for me and them send /create'
    )


async def c_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    autor_id = update.effective_user.id
    UserDB(autor_id).create_user(
        is_create_playlist=True
    )

    await update.message.reply_text(
        f'Hello {update.effective_user.first_name}'
        f'\nThis bot create playlist from your audio'
        f'\nsend audio for me and them send /create'
        f'\nauthor: @denis_malin and source code:'
        f'\nhttps://github.com/skar404/playlist-bot'
    )


async def new_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    autor_id = update.effective_user.id

    await update.message.reply_text(
        f'Hello {update.effective_user.first_name}'
        f'\nsend audio for me'
    )

    user = UserDB(autor_id).get_user()
    if not user:
        UserDB(autor_id).create_user(
            is_create_playlist=True
        )
    else:
        user.create_playlist()


async def add_audio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    autor_id = update.effective_user.id
    user = UserDB(autor_id).get_user()
    if not user:
        return

    user.add_audio(update.message.audio.file_id)


async def create_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    autor_id = update.effective_user.id
    user = UserDB(autor_id).get_user()
    if not user:
        return

    if not user.is_create_playlist or not user.playlist:
        await update.message.reply_text(
            f'You don\'t start create playlist, first:'
            f'\nsend /new or audio for me',
        )
        return

    await context.bot.send_media_group(
        chat_id=update.message.chat_id,
        media=[InputMediaAudio(
            media=audio
        ) for audio in user.playlist]
    )

    user.clean_user()


cursor = duckdb.connect(
    ":memory:",
)


print('stage load db')
cursor.execute(f"""
    INSTALL httpfs;
    LOAD httpfs;

    SET s3_region='{settings.s3_region}';
    SET s3_access_key_id='{settings.s3_access_key_id}';
    SET s3_secret_access_key='{settings.s3_secret_access_key}';

    CREATE TABLE podcast_db AS FROM read_parquet('s3://{settings.s3_file_path}');
    INSERT INTO podcast_db SELECT * FROM read_parquet('s3://{settings.s3_file_path}');
""")
print('done migrate')

# cursor.execute(
#     f"""
#     CREATE TABLE podcast_db (
#         user_id INTEGER UNIQUE,
#         is_create_playlist BOOLEAN DEFAULT FALSE NULL,
#         playlist JSON DEFAULT '[]'
#     );
#     CREATE SEQUENCE seq_podcast_db_id START 1;
#
#     COPY podcast_db TO 's3://{settings.s3_file_path}';
#     """
# )

app = ApplicationBuilder().token(settings.telegram_token).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", c_help))
app.add_handler(CommandHandler("new", new_playlist))
app.add_handler(CommandHandler("create", create_playlist))
app.add_handler(MessageHandler(filters.AUDIO, add_audio))

try:
    print('app start')
    app.run_polling()
finally:
    print('app stop')
    cursor.execute(f"""COPY podcast_db TO 's3://{settings.s3_file_path}';""")
    print('done backup db')

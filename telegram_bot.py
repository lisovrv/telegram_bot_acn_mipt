import asyncio
import aiohttp
import json
import os
import sqlite3
import typing as tp
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor


bot = Bot(token=os.environ['BOT_TOKEN'])
dp = Dispatcher(bot)

API_KEY = os.environ['API_KEY']

SEARCH_COUNTIES = ["US", "UK", "DE", "KR", "RU"]
GOOD_RESPONSE = '2'

MOVIE_TV_DICT = {
    "movie": {"name": "title", "date": "release_date"},
    "tv": {"name": "name", "date": "first_air_date"},
}

CONFIGURATION_PATH = "https://api.themoviedb.org/3/configuration"
SEARCH_PATH = "https://api.themoviedb.org/3/search/"
WATCH_PATH = "https://api.themoviedb.org/3/"

DB_NAME = "film_database.db"


@dp.message_handler(commands=['start'])
async def welcome(message: types.Message) -> None:
    await message.reply("\U0001F3A6 Hello! I'm KinoFox Bot  \U0001F3A6 \n")


@dp.message_handler(commands=['help'])
async def help_task(message: types.Message) -> None:
    await message.reply(
                    "*Commands:*\n"
                    "_/start_ - start interaction\n"
                    "_/help_ - information about commands\n"
                    "_/history_ - return history of film requests of users\n"
                    "_/stats_ - return history of film requests of users with the number of requests\n"
                    "To find a film, just give me a *film name* \U0001f600",
                    parse_mode="Markdown")


async def get_sorted_shows_from_db(connection: tp.Any) -> tp.Dict[str, str]:
    cursor = connection.cursor()
    cursor.execute("select * from lang")
    raw_history = dict(cursor.fetchall())
    history = dict(sorted(raw_history.items(), key=lambda item: item[1], reverse=True))
    return history


@dp.message_handler(commands=['history'])
async def history_task(message: types.Message) -> None:
    connection = sqlite3.connect(DB_NAME)
    history = await get_sorted_shows_from_db(connection)
    text = ''
    if history:
        for name in history.keys():
            text += f"\U0001F4CD {name} \n"
        await message.reply(text, parse_mode="Markdown")
    else:
        await message.reply("No history yet \U0001F972", parse_mode="Markdown")
    connection.close()


@dp.message_handler(commands=['stats'])
async def stats_task(message: types.Message) -> None:
    connection = sqlite3.connect(DB_NAME)
    history = await get_sorted_shows_from_db(connection)
    text = ''
    if history:
        for name, number in history.items():
            text += f"\U0001F3AF {name}  - *{number} times*\n"
        await message.reply(text, parse_mode="Markdown")
    else:
        await message.reply("No statistics yet \U0001F972", parse_mode="Markdown")
    connection.close()


async def request(session: aiohttp.ClientSession, api: str, params: tp.Dict[str, tp.Any]) -> tp.Any:
    async with session.get(api, params=params) as r:
        response_code = str(r.status)[0]
        if response_code == GOOD_RESPONSE:
            text_load = await r.text()
            return json.loads(text_load)


async def get_search_result_and_show_type(sess: aiohttp.ClientSession,
                                          message: types.Message) -> tp.Tuple[tp.Any, str]:
    # choose between movie and TV-show looking at their popularity
    result = {}
    show = "movie"
    params = {"api_key": API_KEY, "query": message.text, "page": 1}
    response_json_movie, response_json_tv = await asyncio.gather(
        request(sess, SEARCH_PATH + "movie", params),
        request(sess, SEARCH_PATH + "tv", params)
    )

    if response_json_movie["results"] and response_json_tv["results"]:
        if response_json_movie["results"][0]["popularity"] > response_json_tv["results"][0]["popularity"]:
            result = response_json_movie["results"][0]
            show = "movie"
        else:
            result = response_json_tv["results"][0]
            show = "tv"
    elif response_json_movie["results"]:
        result = response_json_movie["results"][0]
        show = "movie"
    elif response_json_tv["results"]:
        result = response_json_tv["results"][0]
        show = "tv"
    return result, show


async def send_response(sess: aiohttp.ClientSession, message: types.Message, api_key: tp.Dict[str, str],
                        show: str, result: tp.Any, link: str) -> None:
    # poster
    response_cfg = await request(sess, CONFIGURATION_PATH, api_key)
    await message.answer_photo(response_cfg["images"]["base_url"] +
                               response_cfg["images"]["poster_sizes"][-1] +
                               result["poster_path"])
    # message
    name = MOVIE_TV_DICT[show]["name"]
    date = MOVIE_TV_DICT[show]["date"]
    show = "Movie" if show == "movie" else "TV show"
    message_text = "*" + result[name] + "* (" + result[date][:4] + ")  _" + show + "_\n"
    message_text += "*Rating:*  *" + str(result["vote_average"])
    message_text += "* (" + str(result["vote_count"] / 1000)[:-2] + "k)\n"
    message_text += "_" + result["overview"] + "_\n"
    message_text += link
    await message.reply(message_text, parse_mode="Markdown", disable_web_page_preview=True)


async def update_database(show: str, result: tp.Any) -> None:
    name = MOVIE_TV_DICT[show]["name"]
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    cursor.execute("update lang set num = num + 1 where name=:name",
                   {"name": result[name]})
    cursor.execute("insert or ignore into lang values (?, ?)",
                   (result[name], 1))
    connection.commit()
    connection.close()


async def get_show_link(sess: aiohttp.ClientSession, api_key: tp.Dict[str, str],
                        show: str, result: tp.Any) -> str:
    link = ""
    response_json_watch = await request(sess,
                                        WATCH_PATH + show + "/" +
                                        str(result["id"]) +
                                        "/watch/providers",
                                        api_key)
    if response_json_watch is not None:
        response = response_json_watch["results"]
        film_link = None
        for region in SEARCH_COUNTIES:
            if region in response:
                film_link = response[region]["link"]
                break
        if film_link is not None:
            link = film_link
    return link


@dp.message_handler()
async def get_film_info(message: types.Message) -> None:
    api_key = {"api_key": API_KEY}

    async with aiohttp.ClientSession() as sess:
        link: str = ""
        result, show = await get_search_result_and_show_type(sess, message)
        if result:
            link = await get_show_link(sess, api_key, show, result)
        if not result or not link:
            await message.reply("Ooops, I can't find such  a film ... \U0001F972")
            return None

        await send_response(sess, message, api_key, show, result, link)
        await update_database(show, result)


def create_db() -> None:
    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()
    cur.execute("create table lang (name varchar unique , num)")
    con.commit()
    con.close()


if __name__ == '__main__':
    create_db()
    executor.start_polling(dp)

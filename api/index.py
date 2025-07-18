from fastapi import FastAPI, Request
#from tgbot.main import tgbot
from api.functions import hook_handler
from api.check import update_handler
from api.update_batch import redis_update_handler, get_saved_chat, handle_unsorted, update_chat_users, set_origins
from urllib.parse import unquote, urlparse

app = FastAPI()

@app.post('/api/bot')
async def tgbot_webhook_route(request: Request):
    body = await request.body()
    print(request, body)
    update_dict = await request.json()
    print(update_dict)
    await tgbot.update_bot(update_dict)
    return ''

@app.post('/api/message')
async def send_message(request: Request):
    body = await request.body()
    print(request, unquote(body.decode()))
    print(urlparse(body.decode()))
    try:
        await hook_handler(body.decode())
    except Exception as e:
        print(e)

    return "post accepted"

@app.get('/api/message')
async def send_message(request: Request):
    #await tgbot.send_message('A message sent')
    return "hello"

@app.get('/api/update')
async def update(request: Request):
    try:
        await update_handler()
    except Exception as e:
        print(e)

@app.get('/api/update-redis')
async def update(request: Request):
    try:
        await redis_update_handler()
    except Exception as e:
        print("Exception: ", e)

@app.get('/api/chat')
async def update(request: Request, chat: str):
    try:
        #data = await get_chat_history(chat, session)
        data = await get_saved_chat(chat)
    except Exception as e:
        data = e
    return data

@app.get('/api/handle-unsorted')
async def update(request: Request):
    try:
        await handle_unsorted()
    except Exception as e:
        print(e)

@app.get('/api/update-users')
async def update(request: Request):
    try:
        await update_chat_users()
    except Exception as e:
        print(e)

@app.get('/api/set-origins')
async def update(request: Request):
    try:
        await set_origins()
    except Exception as e:
        print(e)

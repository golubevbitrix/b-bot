from urllib.parse import unquote
import httpx
import re
import asyncio
import asyncpg
import time
import os
import redis
import emoji
import random 
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
slist = os.getenv("list").split(',')
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")

async def hook_handler(request):
  request = unquote(request).strip().replace("\\","")
  event = re.search('event=(.+?)&', request).group(1)
  print(type(event), event)
  print(slist)
  if event == 'ONSESSIONFINISH':
    try:
      await finish_handler(request)
    except Exception as e:
      print(e)
  elif event == 'ONOPENLINEMESSAGEADD':
    try:
      await add_handler(request)
      print("message skipped")
    except Exception as e:
      print(e)  
  elif event == 'ONSESSIONSTART': 
    try:
       await start_handler(request)
    except Exception as e:
      print(e)  
      
def chat_code(request):
  data = {}
  request = unquote(request)
  data['id'] = re.search('\[connector_id\]=(.+?)&', request).group(1)
  data['line'] = re.search('\[connector\]\[line_id\]=(.+?)&', request).group(1)
  data['chat'] = re.search('\[connector\]\[chat_id\]=(.+?)&', request).group(1)
  data['user'] = re.search('data\[DATA\]\[connector\]\[user_id\]=(.+?)&', request).group(1)
  code = '|'.join(data.values())
  print('code: ', code)
  return code

async def chat_id(code):
  async with httpx.AsyncClient() as client:
    data = {"USER_CODE": code}
    response = await client.post(api +'imopenlines.dialog.get', data=data)
    response = response.json()
    print('chatId: ', response)
    connector = response["result"]["entity_id"].split("|")[0]
    return {"chat": str(response["result"]["id"]), "user": str(response["result"]["owner"]), "connector": connector}
    
async def update_chat(chat, line, user):
    pool = await asyncpg.create_pool(connection_string)
    r = redis.Redis.from_url(redis_url)
    timestamp = int(time.time())
    timestamp = str(int(time.time()))
    statement = f"""
      INSERT INTO chats (id, time, line, user_id, active)
      VALUES ('{chat}', '{timestamp}', '{line}', '{user}', 'Y')
      ON CONFLICT (id)
      DO UPDATE SET id = '{chat}', time = '{timestamp}', line = '{line}', user_id = '{user}', active = 'Y';
    """
    r.hset(chat, mapping={"time": timestamp, "line": line, "user": user})
    #r.mset(mapping={f"{chat}-time": timestamp, f"{chat}-line": line, f"{chat}-user": user})
    async with pool.acquire() as conn:
    # Execute a statement to create a new table.
        await conn.execute(statement)
    await pool.close()  

async def add_handler(request):
  code = chat_code(request)
  data = await chat_id(code)
  #return 
  chat = data["chat"]
  r = redis.Redis.from_url(redis_url)
  data = r.hgetall(chat)
  print("chat data: ", data, not data)
  if not data:
    timestamp = str(int(time.time()))
    r.hset('unsorted', timestamp + str(random.randint(0,100)), chat)
  print("message text: ")
  text = re.search('\[message\]\[text\]=(.+?)&', request, re.DOTALL).group(1)
  print("text: ", text)
  await handle_set_origin_message(text, chat)
  await handle_exclude_message(text, chat)
  await handle_include_message(text, chat)
  
  print(text)
  return
  '''
  #if text
  emojis = emoji.emoji_list(text)
  for i in emojis:
    print(i["emoji"])
  emojis = len(emojis) > 0
  for i in slist:
    if text.find(i) > -1:
      emojis = True
      break
    
  user = re.search('\[message\]\[user_id\]=(.+?)&', request)
  print('message user: ', user, data["user"] == str(user))
  if user:
    user = user.group(1)
  #if data["user"] == str(user) or emojis:
    #await delete_chat(chat)
    
  else:
    print('add_handler: ')
    print('chat: ', chat)
    line = re.search('\[connector\]\[line_id\]=(.+?)&', request).group(1)
    print('line: ', line)
    user = data["user"]
    #user = re.search('\[user_id\]=(.+?)&', request).group(1)
    print('usee: ', user)
    if user != '0':
        await update_chat(chat, line, user)
    '''
async def finish_handler(request):
  chat = re.search('\[chat_id\]=(.+?)&', request)
  if chat:
    await delete_chat(chat.group(1))

async def start_handler(request):
  chat = re.search('\[connector\]\[chat_id\]=(.+?)&', request).group(1)
  connector = re.search('\[connector\]\[chat_id\]=(.+?)&', request)
  if connector:
    connector = connector.group(1)
  if "group" in connector:
    print("group chat")
    return
  r = redis.Redis.from_url(redis_url)
  timestamp = str(int(time.time()))
  r.hset('unsorted', timestamp + str(random.randint(0,100)), chat)
  
async def delete_chat(chat):
  pool = await asyncpg.create_pool(connection_string)
  r = redis.Redis.from_url(redis_url)
  print ('deleting chat ', chat)
  dres = r.delete(str(chat))
  print(dres)
  statement = f"UPDATE chats SET active = 'N' WHERE id = '{chat}'"
  print(statement)
  async with pool.acquire() as conn:
    response = await conn.execute(statement)
    #response = await conn.fetchall("SELECT * FROM chats")
    print(response)
  await pool.close()
  print('chat ', chat, ' deleted')

async def handle_set_origin_message(text , chat):
  r = redis.Redis.from_url(redis_url)
  message = re.search('#ORIGIN##(\d+)', text)
  if message:
    if message.group(0) == text:    
      user = message.group(1)
      data = r.hgetall(chat)
      data["origin"] = user
      r.hset(chat, mapping=data)

async def handle_exclude_message(text, chat):
  r = redis.Redis.from_url(redis_url)
  message = re.search('#EXCLUDE##', text)  
  if message:
    if message.group(0) == text:
      data = r.hgetall(chat)
      data["excluded"] = "true"
      print(data)
      r.hset(chat, mapping=data)

async def handle_include_message(text, chat):
  r = redis.Redis.from_url(redis_url)
  message = re.search('#INCLUDE##', text) 
  print("include check: ", chat, text)
  print(message)
  timestamp = str(int(time.time()))
  if message:
    print(message.group(0))
    if message.group(0) == text:
      data = r.hgetall(chat)
      print(data)
      if "excluded" in data:
        data["excluded"] = "false"
        r.hset(chat, mapping=data)
      elif "user" not in data:
        r.hset('unsorted', timestamp + str(random.randint(0,100)), chat)
  
  
  

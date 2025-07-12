from urllib.parse import unquote
from api.functions import delete_chat, update_chat, chat_id
import httpx
import re
import os
from dotenv import load_dotenv
import asyncio
import asyncpg
import time
import inspect
#import aioredis
import redis

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
delay = int(os.getenv("delay"))*60
connection_string = os.getenv("postgresql")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")

async def redis_update_handler():
    printn(api, connection_string, redis_url)
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    timestamp = int(time.time())
    lines = await get_lines(timestamp)
    statement = "SELECT * FROM chats"
    keys = r.keys()
    list = []
    unsorted = None
    pipeline = r.pipeline()
    for key in keys:
        if key == 'unsorted':
            continue 
            #await handle_unsorted()
        elif key.find('-') == -1:
            list.append(key)
            pipeline.hgetall(key)
    string = "MGET " + ', '.join(list)
    printn(string)
    mget_time = int(round(time.time()*10000))
    output = pipeline.execute()
    printn('pipeline execution time: ', int(round(time.time()*10000)) - mget_time)
    for row, key in zip(output, list):
            #print("row: ", key, row)
            if "line" not in row:
                    printn("skipped for no line in the row")
                    continue
            #print(lines)
            #hgetall_time = int(round(time.time()*10000))        
            queue = lines[row["line"]]
            #print(queue)
            #if timestamp - int(row["time"]) > delay and timestamp - int(row["time"]) < delay * 100 and len(queue) > 1:
            if "origin" in row and len(queue) > 1:
                statuses = {}
                #print(statuses, "!")
                printn(queue, row)
                for user in queue:
                    status = await get_status(user)
                    statuses[user] = status 
                printn(statuses)
                if False in statuses.values():
                    for user, status in statuses.items():
                        printn(user, status, row["user"])
                        if status and user != row["user"]:
                            await update_chat(key, row["line"], user)
                            await change_user(key, user)
                        
                elif "origin" in row:
                    if row["user"] != row["origin"]:
                        await update_chat(key, row["line"], row["origin"])
                        await change_user(key, row["origin"]) 
                    else:
                        printn("user is origin")

    printn("update finished")
  
async def change_user(chat, user):
    printn("change user: started..")
    async with httpx.AsyncClient() as client:
      printn("change user: connection..")
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      try:
          printn("change user: posting..")
          response = await client.post(api + 'imopenlines.operator.transfer', data=data)
          response = response.json()
          printn('transfer response: ', response)
      except Exception as e:
          printn('transfer exception: ', e)
        
async def get_lines(timestamp):
    async with httpx.AsyncClient() as client:
      lines = {}
      response = await client.post(api + 'imopenlines.config.list.get')
      json = response.json()
      printn('execution time: ', timestamp - time.time())
      cmd = {}
      for line in json["result"]:
          cmd[f"line-{line["ID"]}"] = f"imopenlines.config.get?CONFIG_ID={line["ID"]}"
      data = {"cmd": cmd}
      printn(data)
      response = await client.post(api + 'batch', json=data)
      print(response.json())
      result = response.json()["result"]["result"]
      printn(result)
      for line in result:
          lines[line["ID"]] = line["QUEUE"]
      printn(lines)
      printn('execution time: ', timestamp - int(time.time()))
      return lines

async def get_status(user):
    async with httpx.AsyncClient() as client:
        data = {"USER_ID": user}
        response = await client.post(api + 'timeman.status', data=data)
        json = response.json()
        status = json["result"]["STATUS"]
        if status == "OPENED":
            return True
        else:
            return False 

async def handle_unsorted():
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    unsorted = r.hgetall('unsorted')
    printn(unsorted)
    for key in unsorted.keys():
        try:
            chat = unsorted[key]
            data = await get_data(chat)
            line = data["entity_id"].split('|')[1]
            owner = data["owner"]
            printn(owner, line)
            if int(owner) != 0:
               #hash = r.hget(chat)
               #print(hash)
               r.hset(chat, mapping={"line": line, "user": owner, "origin": owner})
               r.hdel('unsorted', key)
               printn("origin set: ", chat, owner)
        except Exception as e:
            printn(f"{unsorted[key]} has not been deleted for {e}")
            
async def get_data(chat):
    async with httpx.AsyncClient() as client:
        data = {"CHAT_ID": chat}
        printn(data)
        try:
            response = await client.post(api +'imopenlines.dialog.get', data=data)
            response = response.json()
        
            return(response["result"])
        except Exception as e:
            printn("dialog get exception: ", e)
            
async def get_saved_chat(chat):
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    data = r.hget(chat)
    return data

def printn(*args):
    print(f"#line {inspect.currentframe().f_back.f_lineno}: ", args)

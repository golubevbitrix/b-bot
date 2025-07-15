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
import json

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
    users = await get_users(lines)
    statuses = await get_statuses(users)
    printn(users, statuses)
    keys = r.keys()
    list = []
    unsorted = None
    pipeline = r.pipeline()
    for key in keys:
        if key == 'unsorted':
            #continue 
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
        printn(key,row)
        if "line" not in row:
            printn("skipped for no line in the row")
            continue     
        row["queue"] = lines[row["line"]]
        row["chat"] = key
        queue = dict.fromkeys(row["queue"], None)
        #printn(key)
        if "origin" in row and len(queue) > 1:
            for user in queue.keys():
                queue[user] = statuses[user]
            if False in queue.values():
                for user, status in queue.items():
                    #printn(user, status, row["user"])
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
    lines = {}
    async with httpx.AsyncClient() as client:      
      response = await client.post(api + 'imopenlines.config.list.get')
      result = response.json()
      data = {}
      for line in result["result"]:
          data[line["ID"]] = line["ID"]
      printn(data)
      result = await batch_request("imopenlines.config.get", "CONFIG_ID", data.keys())
      for line in result.values():
          print(type(line))
          #line = json.loads(line)
          lines[line["ID"]] = line["QUEUE"]
      printn(lines)
      printn('execution time: ', timestamp - int(time.time()))
      return lines

async def get_statuses(users):
    result = await batch_request("timeman.status", "USER_ID", users.keys())
    for user in users.keys():
        users[user] = result[user]["STATUS"] == "OPENED"
    return users

async def handle_unsorted():
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    unsorted = r.hgetall('unsorted')
    printn(unsorted)
    data = await get_data(unsorted.values())
    print(type(data), data[list(data.keys())[0]])
    
    for key in data.keys():
        try:
            printn(key)
            chat = data[key]
            #print(chat)
            id = chat["id"]
            #data = await get_data(chat)
            line = chat["entity_id"].split('|')[1]
            owner = chat["owner"]
            printn(owner, line)
            if int(owner) != 0:
               r.hset(id, mapping={"line": line, "user": owner, "origin": owner})
               r.hdel('unsorted', key)
               printn("origin set: ", id, owner)
            printn(key, "completed")
        except Exception as e:
            printn(f"{unsorted[key]} has not been deleted for {e}")
            
async def get_data(chats):
    result = await batch_request('imopenlines.dialog.get', 'CHAT_ID', chats)
    return result
    '''
    async with httpx.AsyncClient() as client:
        data = {"CHAT_ID": chat}
        printn(data)
        try:
            response = await client.post(api +'imopenlines.dialog.get', data=data)
            response = response.json()
        
            return(response["result"])
        except Exception as e:
            printn("dialog get exception: ", e)
    '''      
async def get_saved_chat(chat):
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    data = r.hget(chat)
    return data

async def get_users(lines):
    users = []
    output = {}
    for line in lines.values():
        print(line)
        users.extend(line)
    printn(users)
    for user in users:
        output[str(user)] = ""
    return output

async def batch_request(path, param, array):
    cmd = {}
    for key in array:
        cmd[key] = f"{path}?{param}={key}"
    json = {"cmd": cmd}
    #printn(json)
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{api}batch", json=json)
        result = response.json()["result"]["result"]
        printn(len(result), type(result))
        return result
        
def printn(*args):
    print(f"#line {inspect.currentframe().f_back.f_lineno}: ", args)

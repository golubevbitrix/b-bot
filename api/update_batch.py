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
    #r = redis.Redis.from_url(redis_url, decode_responses=True)
    timestamp = int(time.time())
    lines = await get_lines(timestamp)
    users = await get_users(lines)
    statuses = await get_statuses(users)
    printn(users, statuses)

    output, list = await get_redis_data()
    #printn('pipeline execution time: ', int(round(time.time()*10000)) - mget_time)
    users_to_change = {}
    chats_to_change = {}
    for row, key in zip(output, list):
        excluded = "false"
    
        if "line" not in row:
            printn("skipped for no line in the row")
            continue     
        row["queue"] = lines[row["line"]]
        row["chat"] = key
        queue = dict.fromkeys(row["queue"], None)
        if "excluded" in row:
            excluded = row["excluded"]
                #excluded = "
        #printn(key)
        if "origin" in row and len(queue) > 1 and excluded == "false":
            for user in queue.keys():
                queue[user] = statuses[user]
            printn(queue)
            if False in queue.values():
                printn(queue.values())
                for user, status in queue.items():
                    printn(user, status, row["user"])
                    if status and user != row["user"]:
                        await update_chat(key, row["line"], user)
                        await change_user(key, user)
                        chats_to_change[key] = {"user":user, "line": row["line"]}
                        
            elif "origin" in row:
                if row["user"] != row["origin"]:
                    await update_chat(key, row["line"], row["origin"])
                    await change_user(key, row["origin"]) 
                    chats_to_change[key] = {"user":user, "line": row["line"]}
                else:
                    printn("user is origin")

    printn("update finished")
  
async def change_user(chat, user):
   # printn("change user: started..")
    async with httpx.AsyncClient() as client:
      #printn("change user: connection..")
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      try:
          printn("change user: posting..")
          response = await client.post(api + 'imopenlines.operator.transfer', data=data)
          response = response.json()
          printn('transfer response: ', response["result"])
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
    printn(list(unsorted.keys()).sort())
    data = await get_data(unsorted.values())
    #print(type(data), data[list(data.keys())[0]])
    printn(len(list(data.keys())))
    for key in unsorted.keys():
        try:
            #printn(key)         
            chat = data[unsorted[key]]
            #print(chat)
            id = chat["id"]
            #data = await get_data(chat)
            line = chat["entity_id"].split('|')[1]
            owner = chat["owner"]
            session = chat["entity_data_1"].split('|')[5]
            #printn(owner, line)
            if int(owner) != 0:
               r.hset(id, mapping={"line": line, "user": owner, "session": session})
               r.hdel('unsorted', key)
               printn("origin set: ", id, owner)
            #printn(key, owner, line, "completed")
        except Exception as e:
            printn(f"{unsorted[key]} has not been deleted for {e}")
    printn("sorting finished")
    
async def get_data(chats):
    result = await batch_request('imopenlines.dialog.get', 'CHAT_ID', chats)
    return result

async def get_saved_chat(chat):
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    printn(chat)
    data = r.hgetall(str(chat))
    printn(data)
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

async def get_chats(chats):
    response = await batch_request("dialog.get","CHAT_ID", chats)
    response = response.json()
    return response["result"]["result"]

async def batch_request(path, param, keys):
    output = {}
    if not isinstance(keys, list):
        keys = list(keys)
    remaining = []
    #printn(len(keys))
    if len(keys) > 50:
        remaining = keys[50:]
        keys = keys[:50]
    cmd = {}
    for key in keys:
        cmd[key] = f"{path}?{param}={key}"
    json = {"cmd": cmd}
    #printn(json)
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{api}batch", json=json)
        result = response.json()
        #printn(len(result), type(result))
        output = result["result"]["result"]
        #printn(output.keys())
        first = output[list(output.keys())[0]]

        if len(remaining) > 0:
            remaining = await batch_request(path, param, remaining)
            output = output|remaining 
        #printn(len(list(output.keys())))
    #printn(len(output.keys()), output.keys())
    first = output[list(output.keys())[0]]
    
    return output

async def update_chat_users():
    output, keys = await get_redis_data()
    result = await batch_request("imopenlines.dialog.get","CHAT_ID", keys)
    printn(type(keys))
    printn(len(list(result.keys())), result.keys())
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    for row, key in zip(output, keys):
        #row["id"] = ke
        #printn(key, row)
        if key not in result:
            printn(key, " skipped")
            continue
        owner = result[key]["owner"]
        
        if "user" not in row:
            row["user"] = owner
            line = result[key]["entity_id"].split('|')[1]
            
            row["line"] = line
        session = result[key]["entity_data_1"].split('|')[5]
        row["session"] = session 
        r.hset(key, mapping=row)    

    printn("update finished")
    #for row in output:
        #printn(row["id"], chat["owner"], row["user"])

async def get_redis_data():
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    keys = sorted(r.keys())
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
    printn(type(output), len(output), len(list))
    return output, list

async def get_origin(client, chat, session):
    url = f"{api}imopenlines.session.history.get?CHAT_ID={chat}&SESSION_ID={session}"
    try:
        response = await client.get(url, timeout=60.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        printn("Error: ", exc)
        return None
    json = response.json()
    messages = json["result"]["message"]
    #printn(messages)
    messages = dict(sorted(messages.items()))
       
    for key in messages.keys():
        #printn(key)
        text = messages[key]["text"]
        user = ""
        match = re.search("\[USER=(\d+) REPLACE\].*\[/USER\] начал работу с диалогом", text)
        if match:
            user = match.group(1)
            print(chat, "user: ", user)
            return user
                
        '''
        async with client.stream('GET', url) as response:
            count = 0
            async for chunk in response.aiter_lines():
                print(count, chunk)
                count += 1
        '''
async def set_origins():
    #r = redis.Redis.from_url(redis_url, decode_responses=True)
    output, list = await get_redis_data()
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    async with httpx.AsyncClient() as client:
        for row, key in zip(output, list):
            origin = "0"
            if "origin" in row:
                origin = row["origin"]
                if origin[0] == "[":
                    printn(key, origin)
                    origin = "0"
            #printn(row["origin"] is None)
            if str(origin) == "0":
                if "session" not in row:
                    printn(key, "skipped for no session")
                    continue
                origin = await get_origin(client, key, row["session"])
                if origin is not None:
                    row["origin"] = origin 
                    r.hset(key, mapping=row)
    printn("setting finished")
                    
def printn(*args):
    print(f"#line {inspect.currentframe().f_back.f_lineno}: ", args)

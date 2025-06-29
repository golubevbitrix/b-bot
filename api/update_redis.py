from urllib.parse import unquote
from api.functions import delete_chat, update_chat
import httpx
import re
import os
from dotenv import load_dotenv
import asyncio
import asyncpg
import time
#import aioredis
import redis

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
delay = int(os.getenv("delay"))*60
connection_string = os.getenv("postgresql")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")

async def redis_update_handler():
    print(api, connection_string, redis_url)
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    timestamp = int(time.time())
    lines = await get_lines(timestamp)
    statement = "SELECT * FROM chats"
    keys = r.keys()
    list = []
    pipeline = r.pipeline()
    for key in keys:
        #list.append(f"{key}-time, {key}-user, {key}-line")
        if key.find('-') == -1:
            list.append(key)
            pipeline.hgetall(key)
    string = "MGET " + ', '.join(list)
    print(string)
    mget_time = int(round(time.time()*10000))
    output = pipeline.execute()
    print('pipeline execution time: ', int(round(time.time()*10000)) - mget_time)
    for row, key in zip(output, list):
            print("row: ", key, row)
            hgetall_time = int(round(time.time()*10000))
            user = row["user"]
            queue = lines[row["line"]]
            if timestamp - int(row["time"]) > delay and timestamp - int(row["time"]) < delay * 100 and len(queue) > 1:
                for line in lines[row["line"]]:
                    print(line, user, user == line)
                if (user in queue) and (len(queue) > 1):
                    print('queue len: ', len(queue) > 1)
                    #queue.remove(str(user))
                #user = queue[0]
                print('line: ', lines[row["line"]])
                print('user: ', user)
                r.hset(key, mapping={"time": str(timestamp),"user": str(user), "line": str(row["line"])})
                try: 
                    status = True
                    try:
                        status = await get_status(user)
                        print('status: ', status)
                    except Exception as e:
                        status = True
                    if not status:
                        if (user in queue) and (len(queue) > 1):
                            #print('queue len: ', len(queue) > 1)
                            queue.remove(str(user))
                        user = queue[0]
                        status = await get_status(queue[0])
                        if status:
                            await update_chat(key, row["line"], user)
                            await change_user(key, user)
                except Exception as e:
                    print('call exception: ', e)
                
                #await conn.execute(f"UPDATE chats SET time = '{str(timestamp)}', user_id = '{str(user)}' WHERE id = '{row["id"]}'")
            #elif timestamp - int(row["time"]) < 400:
               # r.delete(
    
async def change_user(chat, user):
    print("change user: started..")
    async with httpx.AsyncClient() as client:
      print("change user: connection..")
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      try:
          print("change user: posting..")
          response = await client.post(api + 'imopenlines.operator.transfer', data=data)
          response = response.json()
          print('transfer response: ', response)
      except Exception as e:
          print('transfer exception: ', e)
        
async def get_lines(timestamp):
    async with httpx.AsyncClient() as client:
      lines = {}
      response = await client.post(api + 'imopenlines.config.list.get')
      json = response.json()
      print('execution time: ', timestamp - time.time())
      for line in json["result"]:
          #print(line)
          data = {"CONFIG_ID": line["ID"]}
          response = await client.post(api + 'imopenlines.config.get', data=data)
          result = response.json()["result"]
          lines[result["ID"]] = result["QUEUE"]
          print(lines)
          print('execution time: ', timestamp - int(time.time()))
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


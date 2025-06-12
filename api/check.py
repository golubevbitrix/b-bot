from urllib.parse import unquote
from api.functions import delete_chat
import httpx
import re
import asyncio
import asyncpg
import time

#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'

async def update_handler():
    pool = await asyncpg.create_pool(connection_string)
    timestamp = int(time.time())
    lines = await get_lines()
    statement= "SELECT * FROM chats"
    async with pool.acquire() as conn:
    # Execute a statement to create a new table.
        data = await conn.fetch(statement)
        print('fetch result: ', data)
        data = [dict(row) for row in data]
        print('table: ', data)
        for row in data:
            print(row)
            print(timestamp - int(row["time"]))
            if timestamp - int(row["time"]) > 240:
                print('queue: ', lines[row["line"]])
                user = row["user_id"]
                queue = lines[row["line"]]
                print('user :', user)
                for line in lines[row["line"]]:
                    print(line, user, user == line)
                if user in queue:
                    queue.remove(str(user))
                user = queue[0]
                print('line: ', lines[row["line"]])
                print('user: ', user)
                await change_user(row["chat"], user)
                await delete_chat(row["chat"])
    await pool.close()  

async def change_user(chat, user):
    async with httpx.AsyncClient() as client:
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.operator.transfer', data=data)
      response = response.json()
      print('transfer response: ', response)
        
async def get_lines():
    async with httpx.AsyncClient() as client:
      lines = {}
      response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.config.list.get')
      json = response.json()
    
      for line in json["result"]:
          print(line)
          data = {"CONFIG_ID": line["ID"]}
          response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.config.get', data=data)
          result = response.json()["result"]
          lines[result["ID"]] = result["QUEUE"]
          print(lines)
      return lines

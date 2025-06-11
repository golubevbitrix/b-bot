from urllib.parse import unquote
import httpx
import re
import asyncio
import asyncpg
import time

#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
def chat_code(request):
  data = {}
  request = unquote(request)
  data['connector']['id'] = re.search('\[connector_id\]=(.+?)&', request).group(1)
  data['connector']['line'] = re.search('\[connector\]\[line_id\]=(.+?)&', request).group(1)
  data['connector']['chat'] = re.search('\[connector\]\[chat_id\]=(.+?)&', request).group(1)
  data['connector']['user'] = re.search('data\[DATA\]\[connector\]\[user_id\]=(.+?)&', request).group(1)
  data['code'] = '|'.join(data['connector'].values())
  data['chat'] = re.search('\[message\]\[chat_id\]=(.+?)&', request).group(1)
  data['user'] = re.search('\[message\]\[user_id\]=(.+?)&', request).group(1)
  
  return data

async def chat_id(code):
  async with httpx.AsyncClient() as client:
    data = {"USER_CODE": code}
    response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.session.open', data=data)
    response = response.json()
    print(response)
    return str(response["result"]["chatId"])
    
async def update_chat(chat):
    pool = await asyncpg.create_pool(connection_string)
    timestamp = str(time.time())
    statement = f"""
      INSERT INTO chats (id, time)
      VALUES ('{chat}', '{timestamp}')
      ON CONFLICT (id)
      DO UPDATE SET id = '{chat}', time = '{timestamp}';
    """
    async with pool.acquire() as conn:
    # Execute a statement to create a new table.
        await conn.execute(statement)
    await pool.close()  

async def handle_new_message(request):
  code = chat_code(request)
  chat = await chat_id(code)
  response = await update_chat(chat)
#def find(array, term):
  #for i in array:
    #if 

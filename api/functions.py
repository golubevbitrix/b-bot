from urllib.parse import unquote
import httpx
import re
import asyncio
import asyncpg
import time

#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'

async def hook_handler(request):
  request = unquote(request)
  event = re.search('event=(.+?)&', request).group(1)
  print(type(event), event)
  #event = true if event
  #chat = re.search('\[message\]\[chat_id\]=(.+?)&', request).group(1)
  #user = re.search('\[message\]\[user_id\]=(.+?)&', request).group(1)
  if event == 'ONSESSIONFINISH':
    try:
      await finish_handler(request)
    except Exception as e:
      print(e)
  elif event == 'ONOPENLINEMESSAGEADD':
    try:
      await add_handler(request)
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
    response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.session.open', data=data)
    response = response.json()
    print('chatId: ', response)
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

async def add_handler(request):
  chat = re.search('\[message\]\[chat_id\]=(.+?)&', request)
  
  if chat:
    await delete_chat(chat.group(1))
  else:
    code = chat_code(request)
    chat = await chat_id(code)
    response = await update_chat(chat)
    
async def finish_handler(request):
  chat = re.search('\[chat_id\]=(.+?)&', request)
  if chat:
    await delete_chat(chat.group(1))
    
async def delete_chat(chat):
  pool = await asyncpg.create_pool(connection_string)
  statement = f"DELETE * FROM chats WHERE id = '{chat}'"
  async with pool.acquire() as conn:
    await conn.execute(statement)
  await pool.close()
#def find(array, term):
  #for i in array:
    #if 

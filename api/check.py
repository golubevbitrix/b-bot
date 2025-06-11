from urllib.parse import unquote
import httpx
import re
import asyncio
import asyncpg
import time

#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
async def update_chat(chat):
    pool = await asyncpg.create_pool(connection_string)
    timestamp = time.time()
    async with pool.acquire() as conn:
    # Execute a statement to create a new table.
        data = await conn.execute(statement)
        data = [dict(row) for row in data]
        
    await pool.close()  

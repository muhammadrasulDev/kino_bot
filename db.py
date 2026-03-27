import asyncpg
import os
from typing import Optional, List, Dict, Any

class Database:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        database_url = os.getenv("DATABASE_URL")
        self.pool = await asyncpg.create_pool(database_url)
        await self._create_tables()
    
    async def _create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    number INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    language TEXT NOT NULL,
                    link TEXT NOT NULL
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    link TEXT NOT NULL UNIQUE
                )
            ''')
    
    async def add_movie(self, number: int, title: str, category: str, language: str, link: str) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO movies (number, title, category, language, link) VALUES ($1, $2, $3, $4, $5)',
                    number, title, category, language, link
                )
            return True
        except:
            return False
    
    async def delete_movie(self, number: int) -> bool:
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute('DELETE FROM movies WHERE number = $1', number)
                return result != "DELETE 0"
        except:
            return False
    
    async def get_movie(self, number: int) -> Optional[Dict[str, Any]]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow('SELECT * FROM movies WHERE number = $1', number)
                if row:
                    return dict(row)
                return None
        except:
            return None
    
    async def get_movies_by_filter(self, category=None, language=None, limit=100, offset=0):
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT number, title, category, language FROM movies WHERE 1=1"
                params = []
                idx = 1
                if category:
                    query += f" AND category = ${idx}"
                    params.append(category)
                    idx += 1
                if language:
                    query += f" AND language = ${idx}"
                    params.append(language)
                    idx += 1
                query += f" ORDER BY number LIMIT ${idx} OFFSET ${idx+1}"
                params.extend([limit, offset])
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except:
            return []
    
    async def get_all_categories(self):
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('SELECT DISTINCT category FROM movies ORDER BY category')
                return [row['category'] for row in rows]
        except:
            return []
    
    async def get_all_languages(self):
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('SELECT DISTINCT language FROM movies ORDER BY language')
                return [row['language'] for row in rows]
        except:
            return []
    
    async def add_channel(self, name: str, link: str) -> bool:
        try:
            if link.startswith("@"):
                link = f"https://t.me/{link[1:]}"
            elif not link.startswith("http"):
                link = f"https://t.me/{link}"
            async with self.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO channels (name, link) VALUES ($1, $2) ON CONFLICT (link) DO NOTHING',
                    name, link
                )
            return True
        except:
            return False
    
    async def remove_channel(self, link: str) -> bool:
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute('DELETE FROM channels WHERE link = $1', link)
                return result != "DELETE 0"
        except:
            return False
    
    async def get_channels(self):
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('SELECT name, link FROM channels')
                return [{'name': row['name'], 'link': row['link']} for row in rows]
        except:
            return []
    
    async def add_user(self, user_id: int, username=None, first_name=None):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO users (user_id, username, first_name) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO UPDATE SET username=$2, first_name=$3',
                    user_id, username, first_name
                )
            return True
        except:
            return False
    
    async def count_users(self):
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow('SELECT COUNT(*) FROM users')
                return row['count']
        except:
            return 0
    
    async def stats_by_category(self):
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('SELECT category, COUNT(*) FROM movies GROUP BY category')
                return {row['category']: row['count'] for row in rows}
        except:
            return {}
    
    async def stats_by_language(self):
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('SELECT language, COUNT(*) FROM movies GROUP BY language')
                return {row['language']: row['count'] for row in rows}
        except:
            return {}
    
    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

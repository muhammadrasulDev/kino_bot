import aiosqlite
import asyncio
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

class Database:
    """Database handler for movie bot"""
    
    def __init__(self, db_path: str = "movies.db"):
        self.db_path = db_path
        self.conn = None
    
    async def connect(self):
        """Connect to database"""
        self.conn = await aiosqlite.connect(self.db_path)
        await self._create_tables()
    
    async def _create_tables(self):
        """Create all necessary tables"""
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                number INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                language TEXT NOT NULL,
                link TEXT NOT NULL
            )
        ''')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT
            )
        ''')
        
        # Kanal jadvali - name va link bilan
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                link TEXT NOT NULL UNIQUE
            )
        ''')
        
        await self.conn.commit()
    
    # Movie operations
    async def add_movie(self, number: int, title: str, category: str, language: str, link: str) -> bool:
        """Add a new movie"""
        try:
            await self.conn.execute(
                'INSERT INTO movies (number, title, category, language, link) VALUES (?, ?, ?, ?, ?)',
                (number, title, category, language, link)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding movie: {e}")
            return False
    
    async def delete_movie(self, number: int) -> bool:
        """Delete a movie by number"""
        try:
            cursor = await self.conn.execute(
                'DELETE FROM movies WHERE number = ?',
                (number,)
            )
            await self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting movie: {e}")
            return False
    
    async def get_movie(self, number: int) -> Optional[Dict[str, Any]]:
        """Get movie by number"""
        try:
            cursor = await self.conn.execute(
                'SELECT * FROM movies WHERE number = ?',
                (number,)
            )
            row = await cursor.fetchone()
            if row:
                return {
                    'number': row[0],
                    'title': row[1],
                    'category': row[2],
                    'language': row[3],
                    'link': row[4]
                }
            return None
        except Exception as e:
            print(f"Error getting movie: {e}")
            return None
    
    async def get_movies_by_filter(self, category: Optional[str] = None, language: Optional[str] = None, 
                                   limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get movies filtered by category and/or language"""
        try:
            query = "SELECT number, title, category, language FROM movies WHERE 1=1"
            params = []
            
            if category:
                query += " AND category = ?"
                params.append(category)
            
            if language:
                query += " AND language = ?"
                params.append(language)
            
            query += " ORDER BY number LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            
            return [
                {
                    'number': row[0],
                    'title': row[1],
                    'category': row[2],
                    'language': row[3]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Error getting movies by filter: {e}")
            return []
    
    async def get_all_categories(self) -> List[str]:
        """Get all distinct categories"""
        try:
            cursor = await self.conn.execute('SELECT DISTINCT category FROM movies ORDER BY category')
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            print(f"Error getting categories: {e}")
            return []
    
    async def get_all_languages(self) -> List[str]:
        """Get all distinct languages"""
        try:
            cursor = await self.conn.execute('SELECT DISTINCT language FROM movies ORDER BY language')
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            print(f"Error getting languages: {e}")
            return []
    
    # Channel operations
    async def add_channel(self, name: str, link: str) -> bool:
        """Add a required channel with name and link"""
        try:
            # Linkni to'g'rilash
            if link.startswith("@"):
                link = f"https://t.me/{link[1:]}"
            elif not link.startswith("http"):
                link = f"https://t.me/{link}"
            
            await self.conn.execute(
                'INSERT OR IGNORE INTO channels (name, link) VALUES (?, ?)',
                (name, link)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding channel: {e}")
            return False

    async def remove_channel(self, link: str) -> bool:
        """Remove a required channel by link"""
        try:
            cursor = await self.conn.execute(
                'DELETE FROM channels WHERE link = ?',
                (link,)
            )
            await self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error removing channel: {e}")
            return False

    async def get_channels(self):
        """Get all required channels with name and link"""
        try:
            cursor = await self.conn.execute('SELECT name, link FROM channels')
            rows = await cursor.fetchall()
            return [{'name': row[0], 'link': row[1]} for row in rows]
        except Exception as e:
            print(f"Error getting channels: {e}")
            return []
    
    # User operations
    async def add_user(self, user_id: int, username: Optional[str] = None, first_name: Optional[str] = None) -> bool:
        """Add or update user"""
        try:
            await self.conn.execute(
                'INSERT OR REPLACE INTO users (user_id, username, first_name) VALUES (?, ?, ?)',
                (user_id, username, first_name)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding user: {e}")
            return False
    
    async def count_users(self) -> int:
        """Get total user count"""
        try:
            cursor = await self.conn.execute('SELECT COUNT(*) FROM users')
            row = await cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            print(f"Error counting users: {e}")
            return 0
    
    async def stats_by_category(self) -> Dict[str, int]:
        """Get movie statistics by category"""
        try:
            cursor = await self.conn.execute(
                'SELECT category, COUNT(*) as count FROM movies GROUP BY category'
            )
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as e:
            print(f"Error getting category stats: {e}")
            return {}
    
    async def stats_by_language(self) -> Dict[str, int]:
        """Get movie statistics by language"""
        try:
            cursor = await self.conn.execute(
                'SELECT language, COUNT(*) as count FROM movies GROUP BY language'
            )
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as e:
            print(f"Error getting language stats: {e}")
            return {}
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()

# Global database instance
db = Database()
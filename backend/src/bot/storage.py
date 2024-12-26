import sqlite3
import logging
import json
from threading import Timer
from typing import Dict, Optional, List
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class UserPreferences:
    source: Optional[str]
    max_price: Optional[float]
    min_price: Optional[float]
    locations: List[str]
    bedrooms: Optional[int]
    property_type: Optional[str]
    ber_rating: Optional[str]

class PreferenceDebouncer:
    def __init__(self, db_manager, timeout=60):
        self.db_manager = db_manager
        self.timeout = timeout
        self.pending_changes = {}
        
    def queue_change(self, user_id: int, new_prefs: dict):
        if user_id in self.pending_changes:
            self.pending_changes[user_id]['timer'].cancel()
            
        timer = Timer(self.timeout, self._save_changes, args=[user_id])
        self.pending_changes[user_id] = {
            'prefs': new_prefs,
            'timer': timer
        }
        timer.start()

    def _save_changes(self, user_id: int):
        if user_id in self.pending_changes:
            try:
                new_prefs = self.pending_changes[user_id]['prefs']
                self.db_manager.save_preferences(user_id, new_prefs)
                del self.pending_changes[user_id]
            except Exception as e:
                logger.error(f"Error saving preferences for user {user_id}: {str(e)}")

class DatabaseManager:
    def __init__(self, db_path: str = "settings/user_data.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()
        self.debouncer = PreferenceDebouncer(self)

    def init_db(self):
        """Initialize database with updated schema"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id INTEGER PRIMARY KEY,
                    source TEXT,
                    max_price REAL,
                    min_price REAL,
                    locations TEXT,
                    bedrooms INTEGER,
                    property_type TEXT,
                    ber_rating TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS preference_history (
                    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    old_settings TEXT,
                    new_settings TEXT,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user_preferences(user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_user_preferences_user_id 
                ON user_preferences(user_id);
            """)

    def queue_preference_update(self, user_id: int, new_prefs: dict):
        """Queue preference updates with debouncing"""
        self.debouncer.queue_change(user_id, new_prefs)

    def save_preferences(self, user_id: int, preferences: Dict) -> bool:
        """Save user preferences with validation"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                
                # Store old preferences for history
                old_prefs = self.get_preferences(user_id)
                
                # Normalize source name
                if 'source' in preferences:
                    preferences['source'] = preferences['source'].lower()

                # Convert locations list to JSON string
                if 'locations' in preferences:
                    if isinstance(preferences['locations'], list):
                        preferences['locations'] = json.dumps(preferences['locations'])
                
                # Create a complete preferences dict with all fields
                complete_prefs = {
                    'source': preferences.get('source'),
                    'max_price': preferences.get('max_price'),
                    'min_price': preferences.get('min_price'),
                    'locations': preferences.get('locations'),
                    'bedrooms': preferences.get('bedrooms'),
                    'property_type': preferences.get('property_type'),
                    'ber_rating': preferences.get('ber_rating')
                }

                # Prepare values for SQL
                fields = list(complete_prefs.keys())
                placeholders = ['?' for _ in fields]
                values = [complete_prefs[field] for field in fields]
                
                # Construct SQL statement
                sql = f"""
                    INSERT INTO user_preferences (user_id, {', '.join(fields)})
                    VALUES (?, {', '.join(placeholders)})
                    ON CONFLICT (user_id) DO UPDATE SET
                    {', '.join(f'{field} = excluded.{field}' for field in fields)},
                    last_updated = CURRENT_TIMESTAMP
                """
                
                # Execute with all values including user_id
                cur.execute(sql, [user_id] + values)
                
                # Record history
                if old_prefs != preferences:
                    cur.execute("""
                        INSERT INTO preference_history 
                        (user_id, old_settings, new_settings)
                        VALUES (?, ?, ?)
                    """, (user_id, json.dumps(old_prefs), json.dumps(preferences)))

                return True

        except Exception as e:
            logger.error(f"Error saving preferences for user {user_id}: {str(e)}")
            return False


    def get_preferences(self, user_id: int) -> Optional[Dict]:
        """Get user preferences with proper type conversion"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                
                cur.execute("""
                    SELECT * FROM user_preferences 
                    WHERE user_id = ?
                """, (user_id,))
                
                row = cur.fetchone()
                
                if not row:
                    return None
                
                prefs = dict(row)
                
                # Convert locations from JSON string to list
                if prefs.get('locations'):
                    try:
                        prefs['locations'] = json.loads(prefs['locations'])
                    except json.JSONDecodeError:
                        prefs['locations'] = []
                
                # Remove None values and SQL metadata
                return {k: v for k, v in prefs.items() 
                       if v is not None and k not in ['last_updated']}
                
        except Exception as e:
            logger.error(f"Error getting preferences for user {user_id}: {str(e)}")
            return None

    def delete_preferences(self, user_id: int) -> bool:
        """Delete user preferences"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM user_preferences WHERE user_id = ?", (user_id,))
                return cur.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting preferences for user {user_id}: {str(e)}")
            return False

    def get_all_active_users(self) -> List[int]:
        """Get all users with active preferences"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT user_id FROM user_preferences")
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting active users: {str(e)}")
            return []

    def get_preference_history(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Get preference change history for a user"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT change_id, old_settings, new_settings, changed_at
                    FROM preference_history
                    WHERE user_id = ?
                    ORDER BY changed_at DESC
                    LIMIT ?
                """, (user_id, limit))
                
                return [{
                    'change_id': row[0],
                    'old_settings': json.loads(row[1]) if row[1] else None,
                    'new_settings': json.loads(row[2]),
                    'changed_at': row[3]
                } for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting preference history: {str(e)}")
            return []

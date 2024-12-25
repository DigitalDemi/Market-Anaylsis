import sqlite3
import logging
from pathlib import Path
# Holds the ip for telegram usernames that can use the bot
auth_id = {
    298564435
}

# Values for user prefrences -> this holds the current state

PATH = './settings'

def soft_assert(condition: bool, message: str) -> bool:
    """Log instead of raising assertion."""
    if condition:
        logging.info(f"Assertion passed: {message}")
        return True
    logging.error(f"Assertion failed: {message}")
    return False

class PathHandler:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._check_path()  
        
    def _create_path(self) -> bool:
        """Create a directory if it doesn't exist."""
        if not soft_assert(self.path is not None, "Path cannot be None"):
            return False
            
        try:
            if self.path.exists():
                return soft_assert(True, f"Path already exists: {self.path}")
                
            self.path.mkdir(exist_ok=False)
            return soft_assert(self.path.exists(), f"Created path: {self.path}")
        except Exception as e:
            logging.error(f"Failed to create path: {e}")
            return False
            
    def _check_path(self, create_if_missing: bool = True) -> bool:
        """Check if path exists and optionally create it."""
        if not soft_assert(self.path is not None, "Path cannot be None"):
            return False
        
        if not self.path.exists():
            logging.warning(f"Path missing: {self.path}")
            return self._create_path() if create_if_missing else False
        
        return soft_assert(self.path.is_dir(), f"Path {self.path} must be a directory")

def check_if_database_exists(path, user_id):
    if not soft_assert(path is not None, "Path cannot be None"):
        return 

    soft_assert(path.exists(), f"Path exists creating database")

    if soft_assert(Path(f'{path}/{user_id}.db').exists(), f"File exists at {path}/{user_id}.db"):
       return   

    con = sqlite3.connect(f"{path}/{user_id}.db")
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE user_preferences(
        user_id INTEGER NOT NULL PRIMARY KEY,
        current_settings TEXT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE preference_history(
        change_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        old_settings TEXT,
        new_settings TEXT,
        changed_at TIMESTAMP,
        change_type TEXT NOT NULL CHECK (change_type in ('CREATE', 'UPDATE', 'DELETE')),
        FOREIGN KEY (user_id) REFERENCES user_preferences(user_id)
    )
    """)

    soft_assert(Path(f'{path}/{user_id}.db').exists(), f"The file been created {path}/{user_id}.db")

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] %(message)s'
    )
    PathHandler(Path(PATH))
    check_if_database_exists(Path(PATH), 1)

# def check_if_file_exsits():
#     for i in auth_id:


# How do I want to store the prefrences
# get the prefrences -> store them -> dictionary -> json
# check if it in the cache, check if its in a file , tell the user to create their prefrences 


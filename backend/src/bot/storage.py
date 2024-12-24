import sqlite3
import logging
from pathlib import Path
# Holds the ip for telegram usernames that can use the bot
auth_id = {
    298564435
}

PATH = './settings'

def soft_assert(condition: bool, message: str) -> bool:
    """Log instead of raising assertion."""
    if condition:
        logging.info(f"Assertion passed: {message}")
        return True
    logging.error(f"Assertion failed: {message}")
    return False

def create_path(path: Path) -> bool:
    if not soft_assert(path is not None, "Path cannot be None"):
        return False
        
    try:
        path.mkdir(exist_ok=False)
        return soft_assert(path.exists(), f"Created path: {path}")
    except FileExistsError:
        logging.info(f"Path exists: {path}")
        return False
    except Exception as e:
        logging.error(f"Failed to create path: {e}")
        return False

def check_path(path: Path, create_if_missing: bool = True) -> bool:
    if not soft_assert(path is not None, "Path cannot be None"):
        return False
    
    if not path.exists():
        logging.warning(f"Path missing: {path}")
        return create_path(path) if create_if_missing else False
    
    return soft_assert(path.is_dir(), f"Path {path} must be a directory")





# def check_if_file_exsits():
#     for i in auth_id:
if __name__ == '__main__':
    check_path(Path(PATH))



# How do I want to store the prefrences
# get the prefrences -> store them -> dictionary -> json
# check if it in the cache, check if its in a file , tell the user to create their prefrences 


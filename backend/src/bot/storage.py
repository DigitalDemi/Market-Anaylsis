import sqlite3
import logging
from pathlib import Path
# Holds the ip for telegram usernames that can use the bot
auth_id = {
    298564435
}

PATH = './settings'

def create_path(path):
    try:
        path.mkdir(exist_ok=False)
    except FileExistsError as e:
        logging.info(f"Path already exists: {path} : {e}")
        raise


def check_if_path_exists(path):
    logging.info(f"Checking if path exsists")
    valid = Path.exists(path)
    try:
        assert valid, f"Path, {path} does not exists"
    except AssertionError as e:
        logging.error(str(e))
    if not valid: 
        logging.warning(f"Path does not exist. Creating: {path}")
        try: 
            create_path(path)
        except Exception as e:
            logging.warning(f"Failed to create path: {e}")
            raise







# def check_if_file_exsits():
#     for i in auth_id:
if __name__ == '__main__':
    check_if_path_exists(Path(PATH))



# How do I want to store the prefrences
# get the prefrences -> store them -> dictionary -> json
# check if it in the cache, check if its in a file , tell the user to create their prefrences 


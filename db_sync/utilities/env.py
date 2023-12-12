import os
import dotenv
import hashlib
from pathlib import Path
from datetime import datetime


def check(dir_path=None):
    """
    :param str dir_path: custom location where .env file is located
    :return:
    """
    # check if .env file is loaded
    env_load = os.getenv('env_load')
    if env_load is None:
        # if path is not provided then use env within project (not available in source)
        if dir_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent
            env_file = os.path.join(base_dir, 'db_sync', '.env')
            dotenv.load_dotenv(env_file, override=True)
        else:
            dotenv.load_dotenv(dir_path, override=True)


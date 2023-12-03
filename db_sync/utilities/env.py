import os
import dotenv
import hashlib
from pathlib import Path
from datetime import datetime


def check():
    dc_load = os.getenv('dcload')
    if dc_load is None:
        base_dir = Path(__file__).resolve().parent.parent.parent
        env_dir = os.path.join(base_dir, 'db_sync', 'env')
        print(env_dir)
        env_file = os.path.join(env_dir, '.env')
        with open(env_file, 'wb') as wf:
            env_lines = list()
            for filename in os.listdir(env_dir):
                file_path = os.path.join(env_dir, filename)
                with open(file_path, 'rb') as rf:
                    env_lines.append(rf.read())
            env_data = b'\n'.join(env_lines).strip()
            wf.write(env_data)
        dotenv.load_dotenv(env_file, override=True)


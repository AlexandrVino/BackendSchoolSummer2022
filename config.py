import os

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
print(dotenv_path, os.path.exists(dotenv_path))
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

from dotenv import dotenv_values
from loader import load

env = dotenv_values(".env")

try:
    load(env)
except Exception as e:
    print(f"Error: {e}")

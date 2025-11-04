import sys
from datetime import datetime

def log(msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stdout.write(f"[{now}] {msg}\n")
    sys.stdout.flush()

import sys
from datetime import datetime

def log(msg: str):
    # obtém timestamp atual formatado
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # escreve a mensagem formatada
    sys.stdout.write(f"[{now}] {msg}\n")

    # evita buffer
    sys.stdout.flush()

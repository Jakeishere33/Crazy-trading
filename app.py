from flask import Flask
import threading

from engine import main as start_engine

app = Flask(__name__)

@app.route("/")
def home():
    return "Options Engine Running", 200

engine_thread_started = False

def launch_engine():
    global engine_thread_started
    if not engine_thread_started:
        engine_thread_started = True
        t = threading.Thread(target=start_engine, daemon=True)
        t.start()

launch_engine()

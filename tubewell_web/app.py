import random
import struct
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
from flask_cors import CORS
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import threading
import time
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import os
import queue
import shutil
import sqlite3

app = Flask(__name__)
app.secret_key = 'tubewell-manager-secret-key-2024'  # Change this to a secure secret key
CORS(app)

# -----------------------------
# Flask-Login Configuration
# -----------------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'

# -----------------------------
# User Management (Simple in-memory for demo)
# -----------------------------
class User(UserMixin):
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password = password

# Simple user database (in production, use a real database)
users = {
    1: User(1, 'admin', '123'),  # Change password in production
    2: User(2, 'operator', 'operator123')  # Change password in production
}

@login_manager.user_loader
def load_user(user_id):
    return users.get(int(user_id))

# -----------------------------
# Authentication Routes
# -----------------------------
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        remember_me = bool(request.form.get('remember_me'))
        
        # this line is use to show th user who is logged in
        user = next((u for u in users.values() if u.username == username), None)
        
        if user and user.password == password:
            login_user(user, remember=remember_me)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Invalid username or password', 'error')
    
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('login'))

# -----------------------------
# History storage
# -----------------------------
history_data = {}
HISTORY_FILE = "history.json"
history_Lock = threading.Lock()
save_queue = queue.Queue()

# -----------------------------
# Tubewell Data Structure
# -----------------------------
tubewells = {}
for i in range(30):
    tubewells[i] = {
        "name": f"Tubewell {i+1}",
        "status": False,
        "voltage": {"A": 0, "B": 0, "C": 0},
        "current": {"A": 0, "B": 0, "C": 0},
        "active_power": {"A": 0, "B": 0, "C": 0},
        "reactive_power": {"A": 0, "B": 0, "C": 0},
        "power_factor": {"A": 0, "B": 0, "C": 0},
        "frequency": 0,
        "total_runtime": 0,
        "session_start": None,
        "history": []
    }

# Map PLC device IDs to tubewell IDs
device_to_tubewell = {}
for i in range(30):
    device_to_tubewell[f"device-{i+1}"] = i

toggle_commands = {
    0: {
        "on":  {"msgType": "setRs485Value", "data": "03060000000149E8"},
        "off": {"msgType": "setRs485Value", "data": "03060000000209E9"},
    },
    1: {
        "on":  {"msgType": "setRs485Value", "data": " "},
        "off": {"msgType": "setRs485Value", "data": " "},
    },
    2: {
        "on":  {"msgType": "setRs485Value", "data": " "},
        "off": {"msgType": "setRs485Value", "data": " "},
    },
    3: {
        "on":  {"msgType": "setRs485Value", "data": " "},
        "off": {"msgType": "setRs485Value", "data": " "},
    },
    4: {
        "on":  {"msgType": "setRs485Value", "data": " "},
        "off": {"msgType": "setRs485Value", "data": " "},
    },
    5: {
        "on":  {"msgType": "setRs485Value", "data": " "},
        "off": {"msgType": "setRs485Value", "data": " "},
    },
}

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_SUB = "/techno/pub"
MQTT_TOPIC_PUB = "/techno/sub"

client = mqtt.Client()

# -----------------------------
# Database Configuration
# -----------------------------
DB_FILE = "tubewell_data.db"

# Initialize database
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Create table for raw data (for small charts)
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tubewell_id INTEGER,
            timestamp DATETIME,
            voltage_a REAL,
            voltage_b REAL,
            voltage_c REAL,
            current_a REAL,
            current_b REAL,
            current_c REAL,
            active_power_a REAL,
            active_power_b REAL,
            active_power_c REAL,
            reactive_power_a REAL,
            reactive_power_b REAL,
            reactive_power_c REAL,
            frequency REAL
        )
    ''')
    
    # Create table for aggregated data (for big charts)
    c.execute('''
        CREATE TABLE IF NOT EXISTS aggregated_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tubewell_id INTEGER,
            bucket_start DATETIME,
            voltage_a_avg REAL,
            voltage_b_avg REAL,
            voltage_c_avg REAL,
            current_a_avg REAL,
            current_b_avg REAL,
            current_c_avg REAL,
            active_power_a_avg REAL,
            active_power_b_avg REAL,
            active_power_c_avg REAL,
            reactive_power_a_avg REAL,
            reactive_power_b_avg REAL,
            reactive_power_c_avg REAL,
            frequency_avg REAL,
            data_points INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()

# Call this function when starting the app
init_db()

# Store incoming MQTT data
def store_raw_data(tubewell_id, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
        INSERT INTO raw_data 
        (tubewell_id, timestamp, voltage_a, voltage_b, voltage_c, 
         current_a, current_b, current_c, active_power_a, active_power_b, active_power_c,
         reactive_power_a, reactive_power_b, reactive_power_c, frequency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        tubewell_id, datetime.utcnow(),
        data["voltage"]["A"], data["voltage"]["B"], data["voltage"]["C"],
        data["current"]["A"], data["current"]["B"], data["current"]["C"],
        data["active_power"]["A"], data["active_power"]["B"], data["active_power"]["C"],
        data["reactive_power"]["A"], data["reactive_power"]["B"], data["reactive_power"]["C"],
        data["frequency"]
    ))
    
    conn.commit()
    conn.close()

# Aggregate data into 15-minute buckets
def aggregate_data():
    """Run this periodically to aggregate raw data into 15-minute buckets"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get the latest aggregation time
    c.execute('SELECT MAX(bucket_start) FROM aggregated_data')
    result = c.fetchone()
    last_aggregation = result[0] if result[0] else '2000-01-01 00:00:00'
    
    print(f"[AGGREGATION] Last aggregation: {last_aggregation}")
    
    # Aggregate new data in 15-minute intervals
    c.execute('''
        INSERT INTO aggregated_data 
        (tubewell_id, bucket_start, 
         voltage_a_avg, voltage_b_avg, voltage_c_avg,
         current_a_avg, current_b_avg, current_c_avg,
         active_power_a_avg, active_power_b_avg, active_power_c_avg,
         reactive_power_a_avg, reactive_power_b_avg, reactive_power_c_avg,
         frequency_avg, data_points)
        
        SELECT 
            tubewell_id,
            datetime(strftime('%s', timestamp) - (strftime('%s', timestamp) % 900), 'unixepoch') as bucket_start,
            AVG(voltage_a), AVG(voltage_b), AVG(voltage_c),
            AVG(current_a), AVG(current_b), AVG(current_c),
            AVG(active_power_a), AVG(active_power_b), AVG(active_power_c),
            AVG(reactive_power_a), AVG(reactive_power_b), AVG(reactive_power_c),
            AVG(frequency), COUNT(*)
        FROM raw_data 
        WHERE timestamp > ?
        GROUP BY tubewell_id, bucket_start
        HAVING COUNT(*) > 0
    ''', (last_aggregation,))
    
    rows_affected = c.rowcount
    print(f"[AGGREGATION] Added {rows_affected} new aggregated rows")
    
    conn.commit()
    conn.close()

# -----------------------------
# Safe JSON Save Helper
# -----------------------------
def safe_save_json(data, filename):
    tmpfile = filename + ".tmp"
    try:
        with open(tmpfile, "w") as f:
            f.write(data)

        for _ in range(5):
            try:
                shutil.move(tmpfile, filename)
                return True
            except PermissionError:
                time.sleep(0.1)
        print(f"[save_worker] Failed to save {filename} after multiple attempts")
        return False
    finally:
        if os.path.exists(tmpfile):
            try:
                os.remove(tmpfile)
            except:
                pass

# -----------------------------
# File save worker
# -----------------------------
def save_history():
    """Write history_data to disk with locking and retry."""
    max_retries = 10
    retry_delay = 0.2
    for attempt in range(max_retries):
        try:
            with history_Lock:
                safe_save_json(json.dumps(history_data, indent=2), HISTORY_FILE)
                print(f"[save_worker] History saved successfully (attempt {attempt + 1})")
                return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                print(f"[save_worker] File locked, retrying in {retry_delay}s... (attempt {attempt + 1})")
                time.sleep(retry_delay)
            else:
                print(f"[save_worker] Failed to save history after {max_retries} attempts: {e}")
                return False
        except Exception as e:
            print(f"[save_worker] Unexpected error saving history: {e}")
            return False

def save_worker():
    """Background worker thread that saves history when queued."""
    while True:
        save_queue.get()
        save_history()
        save_queue.task_done()

# -----------------------------
# Helpers
# -----------------------------
def get_ieee_float(b1, b2, b3, b4, byteorder="big"):
    if byteorder == "big":
        raw = bytes([b1, b2, b3, b4])
    else:
        raw = bytes([b4, b3, b2, b1])
    return struct.unpack('!f', raw)[0]

# -----------------------------
# MQTT parsing
# -----------------------------
def parse_mqtt_data(payload, tubewell_id):
    try:
        dev_id = payload.get("devId")
        data_hex = payload.get("data", "").replace("\n", "").replace(" ", "")
        if len(data_hex) < 250:
            print(f"[WARN] Skipping short MQTT payload ({len(data_hex)} chars)")
            return

        bytes_list = [int(data_hex[i:i+2], 16) for i in range(0, len(data_hex), 2)]

        print("\n=== RAW PAYLOAD DEBUG ===")
        print("Device ID:", dev_id)
        print("Hex string (first 80 chars):", data_hex[:80], "...")
        print("Bytes (first 40):", bytes_list[:40])
        print("Total bytes received:", len(bytes_list))
        print("==========================\n")

        tw = tubewells[tubewell_id]
        tw["status"] = True 

        # Parse values
        tw["voltage"]["A"] = round(get_ieee_float(bytes_list[13], bytes_list[14], bytes_list[15], bytes_list[16]), 1)
        tw["voltage"]["B"] = round(get_ieee_float(bytes_list[17], bytes_list[18], bytes_list[19], bytes_list[20]), 1)
        tw["voltage"]["C"] = round(get_ieee_float(bytes_list[21], bytes_list[22], bytes_list[23], bytes_list[24]), 1)

        tw["current"]["A"] = round(get_ieee_float(bytes_list[29], bytes_list[30], bytes_list[31], bytes_list[32]), 2)
        tw["current"]["B"] = round(get_ieee_float(bytes_list[33], bytes_list[34], bytes_list[35], bytes_list[36]), 2)
        tw["current"]["C"] = round(get_ieee_float(bytes_list[37], bytes_list[38], bytes_list[39], bytes_list[40]), 2)

        tw["active_power"]["A"] = round(get_ieee_float(bytes_list[49], bytes_list[50], bytes_list[51], bytes_list[52]), 2)
        tw["active_power"]["B"] = round(get_ieee_float(bytes_list[53], bytes_list[54], bytes_list[55], bytes_list[56]), 2)
        tw["active_power"]["C"] = round(get_ieee_float(bytes_list[57], bytes_list[58], bytes_list[59], bytes_list[60]), 2)

        tw["reactive_power"]["A"] = round(get_ieee_float(bytes_list[65], bytes_list[66], bytes_list[67], bytes_list[68]), 2)
        tw["reactive_power"]["B"] = round(get_ieee_float(bytes_list[69], bytes_list[70], bytes_list[71], bytes_list[72]), 2)
        tw["reactive_power"]["C"] = round(get_ieee_float(bytes_list[73], bytes_list[74], bytes_list[75], bytes_list[76]), 2)

        tw["power_factor"]["A"] = round(get_ieee_float(bytes_list[97], bytes_list[98], bytes_list[99], bytes_list[100]), 3)
        tw["power_factor"]["B"] = round(get_ieee_float(bytes_list[101], bytes_list[102], bytes_list[103], bytes_list[104]), 3)
        tw["power_factor"]["C"] = round(get_ieee_float(bytes_list[105], bytes_list[106], bytes_list[107], bytes_list[108]), 3)

        tw["frequency"] = round(get_ieee_float(bytes_list[121], bytes_list[122], bytes_list[123], bytes_list[124]), 2)

        # Store data in database
        store_raw_data(tubewell_id, tw)

        log_history(tubewell_id, tw)
    except Exception as e:
        print(f"Error parsing MQTT data for tubewell {tubewell_id}:", e)

def on_connect(client, userdata, flags, rc):
    print("\n" + "="*50)
    print("🔌 MQTT CONNECTION ATTEMPT")
    print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Topic subscribed: {MQTT_TOPIC_SUB}")
    if rc == 0:
        print("SUCCESS: Connected to MQTT broker")
        print("Subscribed to topic:", MQTT_TOPIC_SUB)
        client.subscribe(MQTT_TOPIC_SUB)
    else:
        print(f"FAILED: Connection error code {rc}")
    print("="*50 + "\n")

def on_message(client, userdata, msg):
    
    print("MQTT MESSAGE RECEIVED!")
    print(f"Topic: {msg.topic}")
    print(f"Payload length: {len(msg.payload)} bytes")
    
    try:
        payload_str = msg.payload.decode()
        print(f"Raw payload (first 200 chars): {payload_str[:200]}...")
        
        payload = json.loads(payload_str)
        dev_id = payload.get("devId", "MISSING_DEV_ID")
        data_present = bool(payload.get("data"))
        
        print(f"Device ID: '{dev_id}'")
        print(f"Data present: {data_present}")
        print(f"Available devices: {list(device_to_tubewell.keys())[:5]}...")  # First 5
        
        if dev_id in device_to_tubewell:
            tubewell_id = device_to_tubewell[dev_id]
            print(f" MATCH: {dev_id} → Tubewell {tubewell_id}")
            
            if data_present:
                print("Starting data parsing...")
                parse_mqtt_data(payload, tubewell_id)
                print("Data parsing completed")
                
                # Verify data was stored
                tw = tubewells[tubewell_id]
                print(f"Updated Tubewell {tubewell_id} - Voltage A: {tw['voltage']['A']}V")
            else:
                print("No data field in payload")
        else:
            print(f"NO MATCH: '{dev_id}' not in device_to_tubewell")
            print(f"Looking for: {dev_id}")
            print(f"Available: {list(device_to_tubewell.keys())}")
            
    except json.JSONDecodeError as e:
        print(f"JSON DECODE ERROR: {e}")
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
    

client.on_connect = on_connect
client.on_message = on_message

print("Connecting to MQTT broker...")
print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
print(f"Subscribe topic: {MQTT_TOPIC_SUB}")
print(f"Publish topic: {MQTT_TOPIC_PUB}")

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    print("MQTT connect() called successfully")
except Exception as e:
    print(f"MQTT connect() failed: {e}")

# Start MQTT loop
print("Starting MQTT loop...")
mqtt_thread = threading.Thread(target=client.loop_start, daemon=True)
mqtt_thread.start()
print("MQTT loop started in background thread")

# -----------------------------
# Dummy data simulation
# -----------------------------
# def simulate_dummy_data():
#     while True:
#         for i in range(30):
#             tw = tubewells[i]
#             tw["voltage"]["A"] = round(random.uniform(210, 250), 1)
#             tw["voltage"]["B"] = round(random.uniform(210, 250), 1)
#             tw["voltage"]["C"] = round(random.uniform(210, 250), 1)
#             tw["current"]["A"] = round(random.uniform(5, 20), 2)
#             tw["current"]["B"] = round(random.uniform(5, 20), 2)
#             tw["current"]["C"] = round(random.uniform(5, 20), 2)
#             tw["active_power"]["A"] = round(random.uniform(1000, 5000), 2)
#             tw["active_power"]["B"] = round(random.uniform(1000, 5000), 2)
#             tw["active_power"]["C"] = round(random.uniform(1000, 5000), 2)
#             tw["reactive_power"]["A"] = round(random.uniform(100, 500), 2)
#             tw["reactive_power"]["B"] = round(random.uniform(100, 500), 2)
#             tw["reactive_power"]["C"] = round(random.uniform(100, 500), 2)
#             tw["power_factor"]["A"] = round(random.uniform(0.7, 1.0), 3)
#             tw["power_factor"]["B"] = round(random.uniform(0.7, 1.0), 3)
#             tw["power_factor"]["C"] = round(random.uniform(0.7, 1.0), 3)
#             tw["frequency"] = round(random.uniform(49.5, 50.5), 2)
#             log_history(i, tw)
#         time.sleep(5)

# -----------------------------
# History load & log
# -----------------------------
def load_history():
    global history_data
    try:
        with open(HISTORY_FILE, "r") as f:
            raw = json.load(f)
        converted = {}
        for k, v in raw.items():
            try:
                ik = int(k)
            except Exception:
                ik = k
            converted[ik] = v
        history_data = converted
        print("History loaded from disk.")
    except (FileNotFoundError, json.JSONDecodeError):
        print("No previous history found or file invalid; starting fresh.")
        history_data = {}

    for i in range(30):
        if i not in history_data:
            history_data[i] = {
                "voltage": [], "current": [], "active_power": [],
                "reactive_power": [], "power_factor": [],
                "frequency": [], "runtime": []
            }
        else:
            for key in ("voltage", "current", "active_power", "reactive_power", "power_factor", "frequency", "runtime"):
                if key not in history_data[i]:
                    history_data[i][key] = []

def log_history(tubewell_id, mqtt_data):
    global history_data
    now = datetime.now().isoformat()
    for phase in ("A", "B", "C"):
        history_data[tubewell_id]["voltage"].append({"time": now, "phase": phase, "value": mqtt_data["voltage"][phase]})
        history_data[tubewell_id]["current"].append({"time": now, "phase": phase, "value": mqtt_data["current"][phase]})
        history_data[tubewell_id]["active_power"].append({"time": now, "phase": phase, "value": mqtt_data["active_power"][phase]})
        history_data[tubewell_id]["reactive_power"].append({"time": now, "phase": phase, "value": mqtt_data["reactive_power"][phase]})
        history_data[tubewell_id]["power_factor"].append({"time": now, "phase": phase, "value": mqtt_data["power_factor"][phase]})
    history_data[tubewell_id]["frequency"].append({"time": now, "value": mqtt_data["frequency"]})
    history_data[tubewell_id]["runtime"].append({"time": now, "value": mqtt_data["total_runtime"]})
    for key in history_data[tubewell_id]:
        history_data[tubewell_id][key] = history_data[tubewell_id][key][-500:]
    try:
        save_queue.put_nowait(True)
    except queue.Full:
        pass

def start_periodic_saver(interval_seconds=300):
    def _loop():
        while True:
            time.sleep(interval_seconds)
            try:
                save_queue.put_nowait(True)
            except queue.Full:
                pass
    t = threading.Thread(target=_loop, daemon=True)
    t.start()

# Add periodic aggregation (run every 15 minutes)
def start_periodic_aggregation():
    def _aggregate_loop():
        while True:
            time.sleep(900)  # 15 minutes
            try:
                aggregate_data()
                print("Data aggregated into 15-minute buckets")
            except Exception as e:
                print(f"Error during aggregation: {e}")
    
    t = threading.Thread(target=_aggregate_loop, daemon=True)
    t.start()

# Start aggregation when app starts
start_periodic_aggregation()

# -----------------------------
# Protected Routes
# -----------------------------
@app.route("/")
@login_required
def index():
    return render_template("index.html")

@app.route("/history")
@login_required
def history():
    return render_template("history.html")

@app.route("/tubewell/<int:id>")
@login_required
def tubewell_detail(id):
    if id not in tubewells:
        return "Tubewell not found", 404
    tubewell = tubewells[id]
    return render_template("tubewell_detail.html", tubewell_id=id, tubewell=tubewell)

# -----------------------------
# API Routes (Keep them accessible without login for frontend functionality)
# -----------------------------
@app.route("/api/tubewells")
def api_tubewells():
    return jsonify([
        {"id": i, "name": tw["name"], "status": "ON" if tw["status"] else "OFF"}
        for i, tw in tubewells.items()
    ])

@app.route("/api/tubewell/<int:id>/data")
def api_tubewell_data(id):
    if id not in tubewells:
        return jsonify({"error": "Invalid tubewell"}), 404
    tw = tubewells[id]
    runtime = tw["total_runtime"]
    if tw["status"] and tw["session_start"]:
        runtime += int(time.time() - tw["session_start"])

 
    
    # Return zero values if tubewell is OFF
    if not tw["status"]:
        return jsonify({
            "name": tw["name"],
            "status": "OFF",
            "voltage": {"A": 0, "B": 0, "C": 0},
            "current": {"A": 0, "B": 0, "C": 0},
            "active_power": {"A": 0, "B": 0, "C": 0},
            "reactive_power": {"A": 0, "B": 0, "C": 0},
            "power_factor": {"A": 0, "B": 0, "C": 0},
            "frequency": 0,
            "total_runtime": runtime,
            "history": tw["history"]
        })
    
    else:
        return jsonify({
            "name": tw["name"],
            "status": "ON",
            "voltage": tw["voltage"],
            "current": tw["current"],
            "active_power": tw["active_power"],
            "reactive_power": tw["reactive_power"],
            "power_factor": tw["power_factor"],
            "frequency": tw["frequency"],
            "total_runtime": runtime,
            "history": tw["history"]
        })

@app.route("/api/tubewell/<int:id>/toggle", methods=["POST"])
@login_required  # Protect toggle functionality
def api_toggle_tubewell(id):
    if id not in tubewells:
        return jsonify({"error": "Invalid tubewell"}), 404
    tw = tubewells[id]
    cmds = toggle_commands.get(id, {})
    if tw["status"]:
        runtime = int(time.time() - tw["session_start"]) if tw["session_start"] else 0
        tw["total_runtime"] += runtime
        tw["status"] = False
        tw["session_start"] = None
        tw["history"].append({"timestamp": int(time.time()), "action": "OFF", "runtime": runtime})
        if cmds.get("off"):
            client.publish(MQTT_TOPIC_PUB, json.dumps(cmds["off"]))
    else:
        tw["status"] = True
        tw["session_start"] = time.time()
        tw["history"].append({"timestamp": int(time.time()), "action": "ON"})
        if cmds.get("on"):
            client.publish(MQTT_TOPIC_PUB, json.dumps(cmds["on"]))
    return jsonify({"status": "ON" if tw["status"] else "OFF"})

@app.route("/api/tubewell/<int:id>/status")
def api_tubewell_status(id):
    if id not in tubewells:
        return jsonify({"error": "Invalid tubewell"}), 404
    tw = tubewells[id]
    return jsonify({"status": "ON" if tw["status"] else "OFF"})

@app.route("/api/tubewell/<int:id>/history")
@login_required  # Protect history data
def api_tubewell_history(id):
    if id not in history_data:
        return jsonify({"error": "Invalid tubewell"}), 404
    
    # Ensure all required keys exist
    tw_history = history_data[id]
    for key in ("voltage", "current", "active_power", "reactive_power", "power_factor", "frequency", "runtime"):
        if key not in tw_history:
            tw_history[key] = []
    
    return jsonify(tw_history)

@app.route("/api/tubewell/history")
@login_required
def api_all_tubewell_history():
    """API endpoint to get history for all tubewells"""
    return jsonify(history_data)

@app.route("/api/tubewell/<int:id>/chart_data")
def api_tubewell_chart_data(id):
    if id not in tubewells:
        return jsonify({"error": "Invalid tubewell"}), 404
    tw = tubewells[id]
    return jsonify({
        "voltage": tw["voltage"],
        "current": tw["current"],
        "active_power": tw["active_power"],
        "reactive_power": tw["reactive_power"],
        "frequency": tw["frequency"],
        "status": "ON" if tw["status"] else "OFF"
    })

@app.route("/api/debug/tubewell/<int:id>")
def api_debug_tubewell(id):
    """Debug endpoint to check current tubewell data and MQTT status"""
    if id not in tubewells:
        return jsonify({"error": "Invalid tubewell"}), 404
    
    tw = tubewells[id]
    return jsonify({
        "id": id,
        "name": tw["name"],
        "status": "ON" if tw["status"] else "OFF",
        "voltage": tw["voltage"],
        "current": tw["current"], 
        "active_power": tw["active_power"],
        "reactive_power": tw["reactive_power"],
        "frequency": tw["frequency"],
        "has_data": any([
            tw["voltage"]["A"] > 0,
            tw["current"]["A"] > 0, 
            tw["active_power"]["A"] > 0
        ]),
        "last_mqtt_update": "Never" if not tw["history"] else tw["history"][-1]["timestamp"] if tw["history"] else "Never"
    })

@app.route("/api/tubewell/<int:id>/recent")
def api_tubewell_recent(id):
    """Get recent data for small charts (last 20 seconds)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
        SELECT * FROM raw_data 
        WHERE tubewell_id = ? AND timestamp > datetime('now', '-20 seconds')
        ORDER BY timestamp DESC
        LIMIT 20
    ''', (id,))
    
    rows = c.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append({
            "timestamp": row[2],
            "voltage": {"A": row[3], "B": row[4], "C": row[5]},
            "current": {"A": row[6], "B": row[7], "C": row[8]},
            "active_power": {"A": row[9], "B": row[10], "C": row[11]},
            "reactive_power": {"A": row[12], "B": row[13], "C": row[14]},
            "frequency": row[15]
        })
    
    return jsonify(data)

@app.route("/api/tubewell/<int:id>/aggregated")
def api_tubewell_aggregated(id):
    """Get 15-minute aggregated data for big charts"""
    date_str = request.args.get('date')
    if date_str:
        # Specific date requested
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
            start_time = target_date
            end_time = target_date + timedelta(days=1)
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    else:
        # Default to last 24 hours
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=1)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
        SELECT * FROM aggregated_data 
        WHERE tubewell_id = ? AND bucket_start BETWEEN ? AND ?
        ORDER BY bucket_start
    ''', (id, start_time, end_time))
    
    rows = c.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append({
            "timestamp": row[2],
            "voltage": {"A": row[3], "B": row[4], "C": row[5]},
            "current": {"A": row[6], "B": row[7], "C": row[8]},
            "active_power": {"A": row[9], "B": row[10], "C": row[11]},
            "reactive_power": {"A": row[12], "B": row[13], "C": row[14]},
            "frequency": row[15],
            "data_points": row[16]
        })
    
    return jsonify(data)

@app.route("/api/debug/data-flow")
def api_debug_data_flow():
    """Debug endpoint to check data flow for device-1"""
    tubewell_id = 0  # device-1 maps to tubewell 0
    tw = tubewells[tubewell_id]
    
    return jsonify({
        "tubewell_id": tubewell_id,
        "device_mapping": "device-1 -> tubewell 0",
        "status": "ON" if tw["status"] else "OFF",
        "current_data": {
            "voltage": tw["voltage"],
            "current": tw["current"], 
            "active_power": tw["active_power"],
            "reactive_power": tw["reactive_power"],
            "frequency": tw["frequency"]
        },
        "has_mqtt_data": any([
            tw["voltage"]["A"] > 0,
            tw["current"]["A"] > 0,
            tw["active_power"]["A"] > 0
        ]),
        "last_update": tw["history"][-1]["timestamp"] if tw["history"] else "Never"
    })

@app.route('/api/comparison')
def api_comparison():
    ids = request.args.get('ids', '')
    id_list = [i.strip() for i in ids.split(',') if i.strip()]
    from_date = request.args.get('from')
    to_date = request.args.get('to')

    # Optional: parse date range
    try:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime.now() - timedelta(days=1)
        to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else datetime.now()
    except Exception:
        from_dt = datetime.now() - timedelta(days=1)
        to_dt = datetime.now()

    # --- Mock data ---
    tubewells_data = {}
    for tw_id in id_list:
        name = str(tw_id)
        metrics = {
            "voltage": [{"time": (from_dt + timedelta(hours=i)).isoformat(), "value": {"A": random.uniform(210, 230), "B": random.uniform(210, 230), "C": random.uniform(210, 230)}} for i in range(24)],
            "current": [{"time": (from_dt + timedelta(hours=i)).isoformat(), "value": {"A": random.uniform(10, 20), "B": random.uniform(10, 20), "C": random.uniform(10, 20)}} for i in range(24)],
            "active_power": [{"time": (from_dt + timedelta(hours=i)).isoformat(), "value": random.uniform(2000, 4000)} for i in range(24)],
            "reactive_power": [{"time": (from_dt + timedelta(hours=i)).isoformat(), "value": random.uniform(500, 1000)} for i in range(24)],
            "power_factor": [{"time": (from_dt + timedelta(hours=i)).isoformat(), "value": random.uniform(0.85, 0.95)} for i in range(24)]
        }

        tubewells_data[tw_id] = {
            "id": tw_id,
            "name": name,
            "metrics": metrics,
            "available_start": from_dt.isoformat(),
            "available_end": to_dt.isoformat(),
            "total_energy": round(random.uniform(100, 300), 2)
        }

    response = {
        "tubewells": tubewells_data,
        "available_start": from_dt.isoformat(),
        "available_end": to_dt.isoformat(),
        "labels": [(from_dt + timedelta(hours=i)).isoformat() for i in range(24)]
    }

    return jsonify(response)


# Main

if __name__ == "__main__":
    load_history()
    init_db()  # Ensure database is initialized
    threading.Thread(target=save_worker, daemon=True).start()
    start_periodic_saver(60)
    start_periodic_aggregation()  # Start the aggregation thread
   
    print("\n" + "="*60)
    print(" STARTING TUBEWELL MONITORING SYSTEM")
    print("="*60)
    
    # Connect to MQTT broker
    print("Connecting to MQTT broker...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f" Connected to {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        print(f" MQTT connection failed: {e}")
    
    # Start MQTT loop in background thread
    print(" Starting MQTT message loop...")
    mqtt_thread = threading.Thread(target=client.loop_start, daemon=True)
    mqtt_thread.start()
    print(" MQTT client started and listening for messages")
    
    print(f" Subscribed to: {MQTT_TOPIC_SUB}")
    print(f" Publishing to: {MQTT_TOPIC_PUB}")
    print(f" Device mapping: {len(device_to_tubewell)} devices configured")
    print("="*60 + "\n")
    
    # Start Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
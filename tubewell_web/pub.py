import paho.mqtt.client as mqtt
import json
import random
import struct
import time

# MQTT Config
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "/techno/pub"

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Size of the frame (enough to cover last index, e.g. frequency at 121)
FRAME_SIZE = 200  

def put_float(buf, index, value):
    """Insert a float (IEEE-754 big endian) into the buffer at the given index."""
    buf[index:index+4] = struct.pack('!f', round(value, 2))  # rounded to 2 decimals

while True:
    payload = {"devId": "device-1"}  # must match device_to_tubewell in Flask

    # Generate sample values
    voltage_A = random.uniform(210, 230)
    voltage_B = random.uniform(210, 230)
    voltage_C = random.uniform(210, 230)

    current_A = random.uniform(1, 5)
    current_B = random.uniform(1, 5)
    current_C = random.uniform(1, 5)

    active_A = random.uniform(0.5, 2.0)
    active_B = random.uniform(0.5, 2.0)
    active_C = random.uniform(0.5, 2.0)

    reactive_A = random.uniform(0.1, 1.0)
    reactive_B = random.uniform(0.1, 1.0)
    reactive_C = random.uniform(0.1, 1.0)

    pf_A = random.uniform(0.8, 1.0)
    pf_B = random.uniform(0.8, 1.0)
    pf_C = random.uniform(0.8, 1.0)

    frequency = random.uniform(49.5, 50.5)

    # Build frame
    data_bytes = bytearray(FRAME_SIZE)

    # Insert values at the exact offsets Flask expects
    put_float(data_bytes, 13, voltage_A)
    put_float(data_bytes, 17, voltage_B)
    put_float(data_bytes, 21, voltage_C)

    put_float(data_bytes, 29, current_A)
    put_float(data_bytes, 33, current_B)
    put_float(data_bytes, 37, current_C)

    put_float(data_bytes, 49, active_A)
    put_float(data_bytes, 53, active_B)
    put_float(data_bytes, 57, active_C)

    put_float(data_bytes, 65, reactive_A)
    put_float(data_bytes, 69, reactive_B)
    put_float(data_bytes, 73, reactive_C)

    put_float(data_bytes, 97, pf_A)
    put_float(data_bytes, 101, pf_B)
    put_float(data_bytes, 105, pf_C)

    put_float(data_bytes, 121, frequency)

    # Convert to hex string
    data_hex = data_bytes.hex().upper()
    payload["data"] = data_hex

    # Publish
    client.publish(MQTT_TOPIC, json.dumps(payload))

    # Log nicely
    print(f"Published data for {payload['devId']}:")
    print(f"  Voltage: A={voltage_A:.2f}V, B={voltage_B:.2f}V, C={voltage_C:.2f}V")
    print(f"  Current: A={current_A:.2f}A, B={current_B:.2f}A, C={current_C:.2f}A")
    print(f"  Active Power: A={active_A:.2f}kW, B={active_B:.2f}kW, C={active_C:.2f}kW")
    print(f"  Reactive Power: A={reactive_A:.2f}kVAr, B={reactive_B:.2f}kVAr, C={reactive_C:.2f}kVAr")
    print(f"  PF: A={pf_A:.2f}, B={pf_B:.2f}, C={pf_C:.2f}")
    print(f"  Frequency: {frequency:.2f} Hz\n")

    time.sleep(2)  # publish every 2 seconds

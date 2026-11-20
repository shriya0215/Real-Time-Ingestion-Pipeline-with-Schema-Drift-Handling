# DRIFTGUARD
Real-Time IoT Data Intelligence Platform with Schema Drift Handling.

## Run on Windows
1. Install Python 3.10+.
2. Extract this folder.
3. Open Command Prompt in the folder.
4. Run:
   `py -m pip install -r requirements.txt`
5. Run:
   `py app.py`
6. Open http://127.0.0.1:5000

This college-demo build uses a local streaming simulator and SQLite, so it works without Kafka or cloud credentials. The architecture is modular and can be extended with Kafka/Redpanda.

## Features
- Live IoT event simulator with 10 devices
- Pipeline start/pause and event-rate control
- Schema V1/V2/V3 and drift simulation
- Field normalization and type casting
- Validation and quarantine
- Replay
- Alert resolution
- Event explorer
- Device fleet
- Live dashboard
- SQLite persistence

## Demo
1. Open Command Center.
2. Click Schema Drift Lab.
3. Simulate Firmware Update.
4. Inspect the schema change and activity.
5. Open Event Explorer to inspect processed records.
6. Send an invalid event to see quarantine.
7. Replay a quarantined event.
8. Reset Demo to return to V1.

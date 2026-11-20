import os
from flask import Flask, jsonify, request, render_template
import sqlite3, json, random, threading, time, uuid
from datetime import datetime, timezone
from copy import deepcopy

app=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),"driftguard.db")
lock=threading.Lock()
pipeline={"running":True,"mode":"SIMULATION","rate":2}
devices=[
 {"device_id":"IOT-001","name":"Thermal Sensor Alpha","type":"temperature_sensor","location":"Factory Zone A","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-002","name":"Humidity Sensor Beta","type":"humidity_sensor","location":"Factory Zone B","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-003","name":"Pressure Sensor Gamma","type":"pressure_sensor","location":"Assembly Line 1","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-004","name":"Energy Meter Delta","type":"energy_meter","location":"Power Room","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-005","name":"Vibration Sensor Epsilon","type":"vibration_sensor","location":"Assembly Line 2","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-006","name":"Thermal Sensor Zeta","type":"temperature_sensor","location":"Factory Zone C","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-007","name":"Humidity Sensor Eta","type":"humidity_sensor","location":"Storage Area","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-008","name":"Pressure Sensor Theta","type":"pressure_sensor","location":"Boiler Room","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-009","name":"Energy Meter Iota","type":"energy_meter","location":"Power Room 2","firmware":"1.0","status":"ONLINE"},
 {"device_id":"IOT-010","name":"Vibration Sensor Kappa","type":"vibration_sensor","location":"Assembly Line 3","firmware":"1.0","status":"ONLINE"},
]
schemas={
 1:{"device_id":"string","timestamp":"string","temperature":"float","humidity":"float","battery":"integer"},
 2:{"device_id":"string","timestamp":"string","temperature":"float","humidity":"float","battery":"integer","signal_strength":"integer","location":"string"},
 3:{"device_id":"string","timestamp":"string","temp_celsius":"string","humidity_percent":"float","battery_level":"integer","sensor_metadata":"object"}
}
schema_version=1
schema_history=[]
activity=[]
def db():
 c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c
def init_db():
 c=db()
 c.executescript("""
 CREATE TABLE IF NOT EXISTS raw_events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE,device_id TEXT,payload TEXT,schema_version INTEGER,received_at TEXT);
 CREATE TABLE IF NOT EXISTS processed_events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE,device_id TEXT,device_type TEXT,temperature REAL,humidity REAL,battery INTEGER,location TEXT,firmware TEXT,schema_version INTEGER,quality_status TEXT,payload TEXT,processed_at TEXT);
 CREATE TABLE IF NOT EXISTS quarantine(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT,payload TEXT,error TEXT,device_id TEXT,created_at TEXT,replayed INTEGER DEFAULT 0);
 CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,severity TEXT,title TEXT,message TEXT,created_at TEXT,resolved INTEGER DEFAULT 0);
 """)
 c.commit()
 # seed demo rows only if empty
 if c.execute("select count(*) from processed_events").fetchone()[0]==0:
  for i in range(50):
   d=random.choice(devices); temp=round(random.uniform(19,36),1); hum=round(random.uniform(30,80),1); bat=random.randint(45,100)
   payload={"device_id":d["device_id"],"timestamp":now(),"temperature":temp,"humidity":hum,"battery":bat}
   process_event(payload,seed=True)
 c.close()
def now(): return datetime.now(timezone.utc).isoformat()
def log(title,msg,kind="INFO"):
 activity.insert(0,{"id":str(uuid.uuid4())[:8],"title":title,"message":msg,"kind":kind,"time":now()})
 del activity[60:]
def alert(sev,title,msg):
 c=db(); c.execute("insert into alerts(severity,title,message,created_at) values(?,?,?,?)",(sev,title,msg,now())); c.commit(); c.close()
 log(title,msg,sev)
def normalize(payload):
 p=deepcopy(payload)
 aliases={"temp":"temperature","temp_c":"temperature","temp_celsius":"temperature","humidity_percent":"humidity","hum":"humidity","battery_level":"battery"}
 for old,new in aliases.items():
  if old in p and new not in p: p[new]=p[old]
 try:
  if "temperature" in p: p["temperature"]=float(p["temperature"])
  if "humidity" in p: p["humidity"]=float(p["humidity"])
  if "battery" in p: p["battery"]=int(float(p["battery"]))
 except (ValueError,TypeError) as e: raise ValueError("Type casting failed: "+str(e))
 return p
def process_event(payload,seed=False):
 global schema_version
 eid=payload.get("event_id") or str(uuid.uuid4())
 payload["event_id"]=eid
 dev=next((d for d in devices if d["device_id"]==payload.get("device_id")),None)
 try:
  if not payload.get("device_id"): raise ValueError("Missing required field: device_id")
  p=normalize(payload)
  if "timestamp" not in p: p["timestamp"]=now()
  if "temperature" in p and not (-50<=p["temperature"]<=150): raise ValueError("Temperature outside allowed range")
  if "humidity" in p and not (0<=p["humidity"]<=100): raise ValueError("Humidity must be between 0 and 100")
  if "battery" in p and not (0<=p["battery"]<=100): raise ValueError("Battery must be between 0 and 100")
  c=db()
  c.execute("insert or ignore into raw_events(event_id,device_id,payload,schema_version,received_at) values(?,?,?,?,?)",(eid,p["device_id"],json.dumps(payload),schema_version,now()))
  loc=p.get("location") or (dev["location"] if dev else "Unknown")
  typ=dev["type"] if dev else "unknown"
  c.execute("""insert or ignore into processed_events(event_id,device_id,device_type,temperature,humidity,battery,location,firmware,schema_version,quality_status,payload,processed_at)
  values(?,?,?,?,?,?,?,?,?,?,?,?)""",(eid,p["device_id"],typ,p.get("temperature"),p.get("humidity"),p.get("battery"),loc,p.get("firmware_version",dev["firmware"] if dev else "unknown"),schema_version,"VALID",json.dumps(p),now()))
  c.commit(); c.close()
  if not seed: log("Event processed",f"{p['device_id']} • Schema V{schema_version} • Valid","SUCCESS")
  return True
 except Exception as e:
  c=db(); c.execute("insert into quarantine(event_id,payload,error,device_id,created_at) values(?,?,?,?,?)",(eid,json.dumps(payload),str(e),payload.get("device_id"),now())); c.commit(); c.close()
  if not seed: alert("WARNING","Record quarantined",str(e))
  return False
def generate():
 while True:
  time.sleep(1)
  if not pipeline["running"]: continue
  for _ in range(pipeline["rate"]):
   d=random.choice(devices)
   p={"device_id":d["device_id"],"timestamp":now(),"firmware_version":d["firmware"],"temperature":round(random.uniform(18,42),1),"humidity":round(random.uniform(25,85),1),"battery":random.randint(20,100)}
   if schema_version>=2: p.update({"signal_strength":random.randint(-90,-35),"location":d["location"]})
   if schema_version>=3: p.update({"temp_celsius":str(p.pop("temperature")),"humidity_percent":p.pop("humidity"),"battery_level":p.pop("battery"),"sensor_metadata":{"model":"DG-200","calibration":"2026-11-01"}})
   process_event(p)
threading.Thread(target=generate,daemon=True).start()
init_db()
@app.route("/")
def home(): return render_template("index.html")
@app.route("/api/overview")
def overview():
 c=db()
 counts={k:c.execute(f"select count(*) from {t}").fetchone()[0] for k,t in [("processed","processed_events"),("raw","raw_events"),("quarantine","quarantine"),("alerts","alerts")]}
 counts["resolved_alerts"]=c.execute("select count(*) from alerts where resolved=1").fetchone()[0]
 counts["devices"]=len(devices); counts["schema_version"]=schema_version
 counts["quality"]=round(counts["processed"]/(counts["processed"]+counts["quarantine"] or 1)*100,1)
 counts["rate"]=pipeline["rate"]; counts["running"]=pipeline["running"]
 counts["recent"]= [dict(r) for r in c.execute("select * from processed_events order by id desc limit 12")]
 counts["alerts_list"]=[dict(r) for r in c.execute("select * from alerts order by id desc limit 8")]
 counts["quarantine_list"]=[dict(r) for r in c.execute("select * from quarantine order by id desc limit 8")]
 counts["schema_history"]=schema_history
 c.close(); return jsonify(counts)
@app.route("/api/events")
def events():
 c=db(); rows=[dict(r) for r in c.execute("select * from processed_events order by id desc limit 100")]; c.close(); return jsonify(rows)
@app.route("/api/devices")
def get_devices(): return jsonify(devices)
@app.route("/api/activity")
def get_activity(): return jsonify(activity[:30])
@app.route("/api/pipeline",methods=["POST"])
def set_pipeline():
 data=request.json or {}
 if "running" in data: pipeline["running"]=bool(data["running"])
 if "rate" in data: pipeline["rate"]=max(1,min(20,int(data["rate"])))
 log("Pipeline control", "Pipeline "+("resumed" if pipeline["running"] else "paused"))
 return jsonify(pipeline)
@app.route("/api/simulate",methods=["POST"])
def simulate():
 global schema_version
 scenario=(request.json or {}).get("scenario","add_fields")
 old=schema_version; schema_version=min(3,schema_version+1)
 if schema_version==old: schema_version=1
 changed=[]
 if schema_version==2:
  changed=["signal_strength","location"]
  for d in devices: d["firmware"]="2.0"
 elif schema_version==3:
  changed=["temp_celsius (renamed from temperature)","humidity_percent","battery_level","sensor_metadata"]
  for d in devices: d["firmware"]="3.0"
 else:
  changed=["temperature","humidity","battery"]
  for d in devices: d["firmware"]="1.0"
 schema_history.append({"version":schema_version,"previous":old,"scenario":scenario,"fields":changed,"compatibility":"BACKWARD COMPATIBLE" if schema_version==2 else "POTENTIAL BREAKING CHANGE","time":now()})
 alert("WARNING","Schema drift detected",f"Firmware update introduced Schema V{schema_version}: {', '.join(changed)}")
 # emit a few immediately with changed schema
 for d in random.sample(devices, min(4,len(devices))):
  p={"device_id":d["device_id"],"timestamp":now(),"firmware_version":d["firmware"]}
  if schema_version==1: p.update({"temperature":round(random.uniform(20,35),1),"humidity":round(random.uniform(30,80),1),"battery":random.randint(50,100)})
  elif schema_version==2: p.update({"temperature":round(random.uniform(20,35),1),"humidity":round(random.uniform(30,80),1),"battery":random.randint(50,100),"signal_strength":-54,"location":d["location"]})
  else: p.update({"temp_celsius":str(round(random.uniform(20,35),1)),"humidity_percent":round(random.uniform(30,80),1),"battery_level":random.randint(50,100),"sensor_metadata":{"model":"DG-200"}})
  process_event(p)
 log("Schema registered",f"Schema V{schema_version} detected • {len(changed)} changes","WARNING")
 return jsonify({"version":schema_version,"previous":old,"fields":changed,"compatibility":schema_history[-1]["compatibility"]})
@app.route("/api/send",methods=["POST"])
def send():
 try: p=request.json
 except: p={}
 if not isinstance(p,dict): return jsonify({"error":"Payload must be a JSON object"}),400
 ok=process_event(p)
 return jsonify({"success":ok,"message":"Processed" if ok else "Quarantined"})
@app.route("/api/quarantine")
def quarantine():
 c=db(); r=[dict(x) for x in c.execute("select * from quarantine order by id desc limit 100")]; c.close(); return jsonify(r)
@app.route("/api/replay/<int:rid>",methods=["POST"])
def replay(rid):
 c=db(); r=c.execute("select * from quarantine where id=?",(rid,)).fetchone()
 if not r: c.close(); return jsonify({"error":"Not found"}),404
 p=json.loads(r["payload"]); p.pop("event_id",None)
 c.execute("update quarantine set replayed=1 where id=?",(rid,)); c.commit(); c.close()
 ok=process_event(p)
 log("Replay attempted",f"Quarantine record #{rid} {'processed' if ok else 'failed again'}")
 return jsonify({"success":ok})
@app.route("/api/alerts")
def alerts():
 c=db(); r=[dict(x) for x in c.execute("select * from alerts order by id desc limit 100")]; c.close(); return jsonify(r)
@app.route("/api/alerts/<int:aid>/resolve",methods=["POST"])
def resolve(aid):
 c=db(); c.execute("update alerts set resolved=1 where id=?",(aid,)); c.commit(); c.close(); return jsonify({"success":True})
@app.route("/api/schemas")
def schemas():
 return jsonify({"current":schema_version,"schemas":schemas,"history":schema_history})
@app.route("/api/reset",methods=["POST"])
def reset():
 global schema_version
 schema_version=1; schema_history.clear(); activity.clear(); pipeline["running"]=True
 c=db(); c.execute("delete from raw_events"); c.execute("delete from processed_events"); c.execute("delete from quarantine"); c.execute("delete from alerts"); c.commit(); c.close()
 for i in range(30):
  d=random.choice(devices); process_event({"device_id":d["device_id"],"timestamp":now(),"temperature":round(random.uniform(20,35),1),"humidity":round(random.uniform(35,75),1),"battery":random.randint(50,100)},seed=True)
 log("Demo reset","Pipeline reset to Schema V1","SUCCESS")
 return jsonify({"success":True})
if __name__=="__main__":
 app.run(debug=False,port=5000,threaded=True)

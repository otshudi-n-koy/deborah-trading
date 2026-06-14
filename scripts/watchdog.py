import subprocess
import datetime
import requests

# Watchdog n8n
result = subprocess.run(["docker","inspect","--format={{.State.Running}}","n8n-n8n-1"],capture_output=True,text=True)
if result.stdout.strip() != "true":
    subprocess.run(["docker","start","n8n-n8n-1"])
    print(str(datetime.datetime.now())+" - n8n redemarre")

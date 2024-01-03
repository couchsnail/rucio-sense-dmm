from rucio.client import Client
from datetime import datetime

CLIENT = Client()

def list_recent_rules(time_diff):
    rules = CLIENT.list_replication_rules()
    for rule in rules:
        if (rule["meta"] is not None) and ("sense" in rule["meta"]) and ((datetime.utcnow() - rule['created_at']).seconds < time_diff):
            yield rule

def do():
    for rule in list_recent_rules(100000):
        print(rule)
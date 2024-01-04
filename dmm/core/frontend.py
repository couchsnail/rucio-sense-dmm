from flask import Flask, jsonify
from dmm.db.session import databased
from dmm.utils.db import get_request_from_id

frontend = Flask(__name__)

@databased
@frontend.get('/query/{rule_id}')
def query(rule_id):
    req = get_request_from_id(rule_id)
    if req:
        return jsonify({"source": req.src_url, "destination": req.dst_url})
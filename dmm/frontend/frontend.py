from flask import Flask, Response, render_template
import logging
import time
import json

from dmm.db.session import databased
from dmm.utils.db import get_request_from_id, get_request_cursor

frontend_app = Flask(__name__, template_folder="templates")

@frontend_app.route("/query/<rule_id>", methods=["GET"])
@databased
def handle_client(rule_id, session=None):
    start_time = time.time()
    retry_interval = 5
    retry_timeout = 60
    logging.info(f"Received request for rule_id: {rule_id}")
    while True:
        try:
            req = get_request_from_id(rule_id, session=session)
            if req and req.src_url and req.dst_url:
                result = json.dumps({"source": req.src_url, "destination": req.dst_url})
                response = Response(result, content_type="application/json")
                response.headers.add("Content-Type", "application/json")
                return response
            elif req:
                current_time = time.time()
                if current_time - start_time > retry_timeout:
                    response = Response("", status=404)
                    response.headers.add("Content-Type", "text/plain")
                    return response
                else:
                    time.sleep(retry_interval)
            else:
                response = Response("", status=404)
                response.headers.add("Content-Type", "text/plain")
                return response
        except Exception as e:
            logging.error(f"Error processing client request: {str(e)}")
            response = Response("", status=500)
            response.headers.add("Content-Type", "text/plain")
            return response

@frontend_app.route("/status", methods=["GET", "POST"])
@databased
def get_dmm_status(session=None):
    cursor = get_request_cursor(session=session)
    data = cursor.fetchall() 
    try:
        return render_template("index.html", data=data)
    except Exception as e:
        logging.error(e)
        return "No requests found in DMM\n"
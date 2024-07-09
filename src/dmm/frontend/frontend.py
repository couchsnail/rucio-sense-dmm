from flask import Flask, Response, render_template, url_for, request
import logging
import json
import os

from dmm.db.session import databased
from dmm.utils.db import get_request_from_id, get_request_cursor
from dmm.utils.monit import prom_get_throughput_at_t, fts_submit_job_query

current_directory = os.path.dirname(os.path.abspath(__file__))
templates_folder = os.path.join(current_directory, "templates")
frontend_app = Flask(__name__, template_folder=templates_folder)

@frontend_app.route("/query/<rule_id>", methods=["GET"])
@databased
def handle_client(rule_id, session=None):
    logging.info(f"Received request for rule_id: {rule_id}")
    try:
        req = get_request_from_id(rule_id, session=session)
        if req and req.src_url and req.dst_url:
            result = json.dumps({"source": req.src_url, "destination": req.dst_url})
            response = Response(result, content_type="application/json")
            response.headers.add("Content-Type", "application/json")
            return response
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
        return "Problem in the DMM frontend\n"

@frontend_app.route("/details/<int:rule_id>", methods=["GET", "POST"])
@databased
def open_rule_details(rule_id,session=None):
    logging.info(f"Retrieving information for rule_id: {rule_id}")
    #Step 1: Get all the metrics from the original status page (possibly using client handling template from earlier)
    #Step 2: Call prom_get_throughput_at_t, fts_submit_job_query (separate template?) for specific rule
    try:
        req = get_request_from_id(rule_id, session=session)
        cursor = get_request_cursor(session=session)
        cursor.execute("SELECT rule_id, bandwidth, sense_circuit_status FROM requests WHERE (src_site=req.get('source') AND dst_site=req.get('destination')")
        data = cursor.fetchall()
        logging.debug(data)
        return render_template("details.html",data=data)
    except Exception as e:
        logging.error(e)
        return "Problem in the DMM frontend\n"

@frontend_app.route('/process_id', methods=['POST'])
@databased
def process_id(session=None):
    data = request.json
    rule_id = data.get('rule_id')
    logging.debug(f"Rule ID:{rule_id}")
    url = url_for('open_rule_details', rule_id=rule_id, _external=True)
    return url

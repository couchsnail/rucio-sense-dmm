from flask import Flask, Response, render_template, url_for, request
import logging
import json
import os
from datetime import datetime

from dmm.db.session import databased
from dmm.utils.db import get_request_from_id, get_request_cursor, get_requests, update_bytes_at_t
from dmm.utils.monit import prom_get_throughput_at_t, fts_submit_job_query, prom_get_all_bytes_at_t

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

@frontend_app.route("/", methods=["GET", "POST"])
@databased
def get_dmm_status(session=None):
    cursor = get_request_cursor(session=session)
    data = cursor.fetchall() 
    try:
        return render_template("index.html", data=data)
    except Exception as e:
        logging.error(e)
        return "Problem in the DMM frontend\n"

''' 
Need some way to integrate particular rule information from Prometheus 
Current metrics: transfer state (might be redunandt with circuit status), transfer start time,
                    transfer end time, transfer completion time, transfer operation time (how long it took),
                    transfer file size, volume of data transferred, number of retries, errors (if any)
Is there a way to constantly refresh this so people don't constantly have to reopen the page? 
'''
@frontend_app.route("/details/<rule_id>", methods=["GET", "POST"])
@databased
def open_rule_details(rule_id,session=None):
    logging.info(f"Retrieving information for rule_id: {rule_id}")
    try:
        req = get_request_from_id(rule_id, session=session)
        cursor = get_request_cursor(session=session)
        cursor.execute(f"SELECT rule_id, transfer_status, bandwidth, sense_circuit_status, src_ipv6_block, bandwidth, sense_provisioned_at FROM requests WHERE rule_id = '{req.rule_id}'")
        data = cursor.fetchone()
        rule_id, bandwidth, sense_circuit_status, src_ipv6, bandwidth, prov_time = data
        logging.debug(f"Cursor data: {data}")

        #Can edit this if needed for testing
        # if prov_time is not None:
        #     timestamp = round(datetime.timestamp(datetime.now()))
        #     current_volume = prom_get_all_bytes_at_t(timestamp, src_ipv6)
        #     update_bytes_at_t(req, current_volume, session=session)
        #     bandwidth_volumes = req.bytes_at_t
        #     total = sum(bandwidth_volumes.values())
        #     current_throughput = (total) / (50) #Change this to 5 * self.sense_daemon_frequency

        #     if abs(bandwidth - current_throughput) <= (0.10 * bandwidth):
        #         dmm_status = "Transfer throughput in excellent condition"
        #     elif abs(bandwidth - current_throughput) <= (0.2 * bandwidth):
        #         dmm_status = "Transfer throughput in OK condition"
        #     else:
        #         dmm_status = "Errors occurring in transfer"
        # else:
        #     dmm_status = "No bandwidth has been provisioned"
        
        return render_template("details.html",rule_id=rule_id, bandwidth=bandwidth, 
                                sense_circuit_status=sense_circuit_status)
                                #dmm_status=dmm_status)
        
        # fts_query = fts_submit_job_query(rule_id)
        #fts_query = fts_submit_job_query("95069e5365bd4381b9b2668ce739047b")
        # metrics = fts_query['_shards']
        # success_rate = metrics['successful'] / metrics['total']
        # logging.debug(f"Success rate: {success_rate}")

        # timestamps = fts_get_timestamps(fts_query)
        # complete_timestamps = [entry['tr_timestamp_complete'] for entry in timestamps]
        # max_complete_timestamp = max(complete_timestamps)
        # #Change this to value that would actually work
        # dif = (max_complete_timestamp - prov_time) / 20
        # prom_throughput = prom_get_throughput_at_t(src_ipv6, prov_time, t_avg_over=dif)

    except Exception as e:
        logging.error(e)
        return "Failed to retrieve rule info\n"

@frontend_app.route('/process_id', methods=['POST'])
@databased
def process_id(session=None):
    data = request.json
    rule_id = data.get('rule_id')
    logging.debug(f"Rule ID:{rule_id}")
    url = url_for('open_rule_details', rule_id=rule_id, _external=True)
    return url

# def fts_get_monit_metrics(rule_id):
#     result_query = fts_submit_job_query(rule_id)
#     transferred_vol = result_query['transferred_vol']
#     #transferred_vol @ timestamp
#     src_ip, dst_ip = result_query['src_hostname'], result_query['dst_hostname']
#     src_ipv6 = socket.getaddrinfo(src_ip,None)[-1][-1][0]
#     dst_ipv6 = socket.getaddrinfo(dst_ip,None)[-1][-1][0] 

'''
Potential trends to graph:
Correlation between src/dst sites and transfer rates
Success/failure rates at a given src/dst, given time
Throughput at given time (see high traffic) - time series graph
'''
@frontend_app.route('/statistics', methods=['GET','POST'])
@databased
def see_correlations(session=None):
    return render_template("correlation.html")

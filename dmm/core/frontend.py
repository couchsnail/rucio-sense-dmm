from flask import Flask, request, Response
import logging
import time
import json

from dmm.db.session import databased
from dmm.utils.db import get_request_from_id

frontend_app = Flask(__name__)

@frontend_app.route('/query/<rule_id>', methods=['GET'])
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
                response = Response(result, content_type='application/json')
                response.headers.add('Content-Type', 'application/json')
                return response
            elif req:
                current_time = time.time()
                if current_time - start_time > retry_timeout:
                    response = Response("", status=404)
                    response.headers.add('Content-Type', 'text/plain')
                    return response
                else:
                    time.sleep(retry_interval)
            else:
                response = Response("", status=404)
                response.headers.add('Content-Type', 'text/plain')
                return response
        except Exception as e:
            logging.error(f"Error processing client request: {str(e)}")
            response = Response("", status=500)
            response.headers.add('Content-Type', 'text/plain')
            return response
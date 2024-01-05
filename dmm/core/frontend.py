import logging
from dmm.db.session import databased
from dmm.utils.db import get_request_from_id

@databased
def handle_request(rule_id, session=None):
    req = get_request_from_id(rule_id, session=session)
    if req:
        return {"source": req.src_url, "destination": req.dst_url}
        
def handle_client(lock, connection, address):
    with lock:
        try:
            logging.info(f"Connection accepted from {address}")
            rule_id = connection.recv(1024).decode()
            if not rule_id:
                return
            if not rule_id.startswith("GET /query/"):
                return
            rule_id = rule_id.split("/query/")[1].split(" ")[0]
            logging.debug(f"Rucio request for rule_id: {rule_id}")
            result = handle_request(rule_id)
            connection.send(result.encode())
        except Exception as e:
            logging.error(f"Error processing client {address}: {str(e)}")
        finally:
            connection.close()
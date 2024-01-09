import logging
import socket
from dmm.db.session import databased
from dmm.utils.db import get_request_from_id

@databased        
def handle_client(lock, connection, address, session=None):
    try:
        logging.info(f"Connection accepted from {address}")
        rule_id = connection.recv(1024).decode()
        if not rule_id:
            return
        if not rule_id.startswith("GET /query/"):
            return
        with lock:
            rule_id = rule_id.split("/query/")[1].split(" ")[0]
            logging.debug(f"Rucio request for rule_id: {rule_id}")
            req = get_request_from_id(rule_id, session=session)
            if req:
                result = {"source": req.src_url, "destination": req.dst_url}
                connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + result.encode())
            else:
                connection.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\n")
    except Exception as e:
        logging.error(f"Error processing client {address}: {str(e)}")
    finally:
        connection.shutdown(socket.SHUT_RDWR)
        connection.close()
from dmm.utils.config import config_get, config_get_int

from dmm.core.handler import handle_client
from dmm.core.daemons import start_daemons

from multiprocessing import Process, Lock
import logging
import socket

class DMM:
    def __init__(self):
        # Config attrs
        self.host = config_get("dmm", "host", default="localhost")
        self.port = config_get_int("dmm", "port", default=5000)
        self.lock = Lock()

    def start(self):
        logging.info("Starting Daemons")
        start_daemons(self.lock)

        logging.info("Starting Handler")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((self.host, self.port))
            listener.listen(1)
            logging.info(f"Listening on {self.host}:{self.port}")
            while True:
                logging.info("Waiting for the next connection")
                connection, address = listener.accept()
                client_thread = Process(target=handle_client, args=(self.lock, connection, address))
                client_thread.start()
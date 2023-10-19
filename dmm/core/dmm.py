from dmm.utils.config import config_get, config_get_int

from dmm.core.handler import handle_client
from dmm.core.daemons import run_daemon, stager_daemon, decision_daemon, provision_daemon, reaper_daemon

from dmm.core.decision import NetworkGraph

from multiprocessing import Process, Lock
import logging
import socket

class DMM:
    def __init__(self):
        # Config attrs
        self.host = config_get("dmm", "host", default="localhost")
        self.port = config_get_int("dmm", "port", default=5000)
        self.daemon_frequency = config_get_int("dmm", "daemon_frequency", default=60)

        self.network_graph = NetworkGraph()
        self.lock = Lock()

    def start(self):
        logging.info("Starting Daemons")
        stager_process = Process(target=run_daemon, 
                                 args=(stager_daemon, self.lock, self.daemon_frequency), 
                                 name="STAGER")
        decision_process = Process(target=run_daemon, 
                                   args=(decision_daemon, self.lock, self.daemon_frequency), 
                                   kwargs={"network_graph": self.network_graph}, 
                                   name="DECISION")
        provision_process = Process(target=run_daemon, 
                                    args=(provision_daemon, self.lock, self.daemon_frequency), 
                                    name="PROVISIONER")
        reaper_process = Process(target=run_daemon, 
                                 args=(reaper_daemon, self.lock, self.daemon_frequency), 
                                 name="REAPER")

        stager_process.start()
        provision_process.start()
        decision_process.start()
        reaper_process.start()

        logging.info("Starting Handler")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen(1)
            logging.info(f"Listening on {self.host}:{self.port}")
            while True:
                logging.info("Waiting for the next connection")
                connection, address = listener.accept()
                client_thread = Process(target=handle_client, 
                                        args=(self.lock, connection, address), 
                                        name="HANDLER")
                client_thread.start()
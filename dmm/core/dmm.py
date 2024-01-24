from time import sleep
import logging
from multiprocessing import Process, Lock
import networkx as nx

from rucio.client import Client

from dmm.utils.config import config_get, config_get_int

from dmm.core.rucio import preparer, rucio_modifier, finisher
from dmm.core.sense import allocator, stager, provision, sense_modifier, reaper
from dmm.core.decision import decider
from dmm.core.monit import monit
from dmm.core.frontend import frontend_app

class DMM:
    def __init__(self):
        self.host = config_get("dmm", "host", default="localhost")
        self.port = config_get_int("dmm", "port", default=80)
        self.daemon_frequency = config_get_int("dmm", "daemon_frequency", default=60)
        self.cert = config_get("dmm", "cert")
        self.key = config_get("dmm", "key")

        self.network_graph = nx.MultiGraph()
        self.rucio_client = Client()
        self.lock = Lock()

    @staticmethod
    def run_daemon(daemon, lock, frequency, **kwargs):
        while True:
            logging.info(f"Running {daemon.__name__}")
            with lock:
                try:
                    daemon(**kwargs)
                except Exception as e:
                    logging.error(f"{daemon.__name__} {e}")
            sleep(frequency)
            logging.info(f"{daemon.__name__} sleeping for {frequency} seconds")

    def fork(self, daemons):
        for daemon, kwargs in daemons.items():
            if kwargs:
                proc = Process(target=self.run_daemon,
                    args=(daemon, self.lock, self.daemon_frequency),
                    kwargs=kwargs,
                    name=daemon.__name__)
            else:
                proc = Process(target=self.run_daemon,
                    args=(daemon, self.lock, self.daemon_frequency),
                    name=daemon.__name__)
            proc.start()

    def start(self):
        logging.info("Starting Daemons")
        rucio_daemons = {
            preparer: {"daemon_frequency": self.daemon_frequency, "client": self.rucio_client, "certs": (self.cert, self.key)}, 
            rucio_modifier: {"client": self.rucio_client}, 
            finisher: {"client": self.rucio_client}
        }
        self.fork(rucio_daemons)
        
        sense_daemons = {
            allocator: None,
            stager: None, 
            provision: None, 
            sense_modifier: None,
            reaper: None
        }
        self.fork(sense_daemons)
        
        dmm_daemons = {
            decider: {"network_graph": self.network_graph},
            monit: None
        }
        self.fork(dmm_daemons)

        frontend_app.run(port=80, host='0.0.0.0') 
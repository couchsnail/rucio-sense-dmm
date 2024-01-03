from time import sleep
import logging

from multiprocessing import Process, Lock
import networkx as nx

from rucio.client import Client

from dmm.utils.config import config_get, config_get_int

from dmm.core.rucio import preparer_daemon, rucio_modifier_daemon, submitter_daemon, finisher_daemon
from dmm.core.sense import stager_daemon, provision_daemon, sense_modifier_daemon, reaper_daemon
from dmm.core.decision import decision_daemon
from dmm.core.monit import monit_daemon

class DMM:
    def __init__(self):
        self.host = config_get("dmm", "host", default="localhost")
        self.port = config_get_int("dmm", "port", default=5000)
        self.daemon_frequency = config_get_int("dmm", "daemon_frequency", default=60)

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
            preparer_daemon: {"daemon_frequency": self.daemon_frequency, "client": self.rucio_client}, 
            submitter_daemon: None, 
            rucio_modifier_daemon: {"client": self.rucio_client}, 
            finisher_daemon: {"client": self.rucio_client}
        }
        self.fork(rucio_daemons)
        
        sense_daemons = {
            stager_daemon: None, 
            provision_daemon: None, 
            sense_modifier_daemon: None,
            reaper_daemon: None
        }
        self.fork(sense_daemons)
        
        dmm_daemons = {
            decision_daemon: {"network_graph": self.network_graph},
            monit_daemon: None
        }
        self.fork(dmm_daemons)

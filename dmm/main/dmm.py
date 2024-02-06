import logging
from multiprocessing import Lock
import networkx as nx
from waitress import serve

from rucio.client import Client

from dmm.utils.config import config_get, config_get_int
from dmm.utils.orchestrator import fork

from dmm.daemons.rucio import preparer, rucio_modifier, finisher
from dmm.daemons.sense import stager, provision, sense_modifier, canceller, deleter
from dmm.daemons.core import decider, allocator
from dmm.daemons.sites import refresh_site_db, free_unused_endpoints
from dmm.frontend.frontend import frontend_app

class DMM:
    def __init__(self):
        self.host = config_get("dmm", "host")
        self.port = config_get_int("dmm", "port")
        self.cert = config_get("dmm", "cert")
        self.key = config_get("dmm", "key")

        self.rucio_daemon_frequency = config_get_int("daemons", "rucio", default=60)
        self.dmm_daemon_frequency = config_get_int("daemons", "dmm", default=60)
        self.sense_daemon_frequency = config_get_int("daemons", "sense", default=60)
        self.database_builder_daemon_frequency = config_get_int("daemons", "db", default=7200)

        self.network_graph = nx.MultiGraph()
        self.rucio_client = Client()
        self.lock = Lock()

    def start(self):
        logging.info("Starting Daemons")
        database_builder_daemons = {
            refresh_site_db: {"certs": (self.cert, self.key)},
            free_unused_endpoints: None
        }
        fork(self.database_builder_daemon_frequency, self.lock, database_builder_daemons)

        rucio_daemons = {
            preparer: {"client": self.rucio_client}, 
            rucio_modifier: {"client": self.rucio_client}, 
            finisher: {"client": self.rucio_client}
        }
        fork(self.rucio_daemon_frequency, self.lock, rucio_daemons)
        
        sense_daemons = {
            stager: None, 
            provision: None, 
            sense_modifier: None,
            canceller: None,
            deleter: None
        }
        fork(self.sense_daemon_frequency, self.lock, sense_daemons)
        
        dmm_daemons = {
            decider: {"network_graph": self.network_graph},
            allocator: None,
        }
        fork(self.dmm_daemon_frequency, self.lock, dmm_daemons)

        serve(frontend_app, port=self.port, host=self.host)

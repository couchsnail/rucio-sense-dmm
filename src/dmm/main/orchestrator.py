import logging
from multiprocessing import Process
from time import sleep

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

def fork(frequency, lock, daemons):
    for daemon, kwargs in daemons.items():
        if kwargs:
            proc = Process(target=run_daemon,
                args=(daemon, lock, frequency),
                kwargs=kwargs,
                name=daemon.__name__)
        else:
            proc = Process(target=run_daemon,
                args=(daemon, lock, frequency),
                name=daemon.__name__)
        proc.start()
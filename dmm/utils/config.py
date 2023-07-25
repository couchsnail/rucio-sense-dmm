import configparser as ConfigParser
import os
import logging

__CONFIG = None

class Config:
    def __init__(self):
        self.parser = ConfigParser.ConfigParser()

        if 'DMM_CONFIG' in os.environ:
            self.configfile = os.environ['DMM_CONFIG']
        
        else:
            confdir = '/opt/dmm'
            config = os.path.join(confdir, 'dmm.cfg')
            self.configfile = config if os.path.exists(config) else None

        if not self.configfile:
            raise RuntimeError('configuration file not found.')
        
        if not self.parser.read(self.configfile) == [self.configfile]:
            raise RuntimeError('Could not load DMM configuration file.')

def get_config():
    """Factory function for the configuration class. Returns the ConfigParser instance."""
    global __CONFIG
    if __CONFIG is None:
        logging.info("Loading configuration")
        __CONFIG = Config()
    return __CONFIG.parser


def config_get(section, option, default=None, extract_function=ConfigParser.ConfigParser.get):
    global __CONFIG
    try:
        return extract_function(get_config(), section, option)

    except Exception as e:
        print(e)
    #    raise RuntimeError(f'{section} not found in DMM config')

def config_get_bool(section, option, default=None):
    return bool(config_get(section, option, extract_function=ConfigParser.ConfigParser.getboolean))

def config_get_int(section, option, default=None):
    return int(config_get(section, option, extract_function=ConfigParser.ConfigParser.getint))
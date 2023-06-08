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
                raise RuntimeError('Could not load DMM configuration file.')
            
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
    """
    Return the string value for a given option in a section

    First it looks at the configuration file and, if it is not found, check in the config table only if it is called
    from a server/daemon (and if check_config_table is set).

    :param section: the named section.
    :param option: the named option.
    :param default: the default value if not found.
    :param extract_function: The method of ConfigParser.ConfigParser to call to retrieve the value.

    :returns: the configuration value.
    """
    global __CONFIG
    try:
        return extract_function(get_config(), section, option)

    except:
        raise RuntimeError(f'{section} not found in DMM config')

def config_get_bool(section, option, default=None):
    return bool(config_get(section, option, extract_function=ConfigParser.ConfigParser.getboolean))

def config_get_int(section, option, default=None):
    return int(config_get(section, option, extract_function=ConfigParser.ConfigParser.getint))
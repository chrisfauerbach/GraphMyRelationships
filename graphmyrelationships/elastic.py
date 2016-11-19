"""
What does this module do?  
Does it do things?

"""

__all__ = []
__version__ = '0.1'
__author__ = 'Chris Fauerbach'
__email__ = 'chris@fauie.com'

from config import Config
import requests
import logging

class ElasticService():


    def __init__(self, config):
        """
        #param: config: Config
        """
        self.LOGGER = logging.getLogger(__name__)
        host = config.elastic_host
        port = config.elastic_port
        self.root_url = "http://{}:{}/".format( host, port)

    def list_indices(self):
        stats_url = "{}_stats?human&pretty".format(self.root_url)
        resp = requests.get(stats_url)
        obj = resp.json()
        indices = obj.get('indices')
        index_names = []

        for idx_name in indices:
            # log.info ( "ATTR: %s",  attr )
            index_names.append(idx_name)

        return index_names


    def list_types(self, idx):
        stats_url = "{}/{}/_mappings?human&pretty".format(self.root_url, idx)
        resp = requests.get(stats_url)
        obj = resp.json()
        self.LOGGER.debug( obj )
        mappings = obj.get(idx,{}).get('mappings',{})
        return mappings

        #indices = obj.get('indices')
        #index_names = []
#
        #for idx_name in indices:
            ## log.info ( "ATTR: %s",  attr )
            #index_names.append(idx_name)
#
        #return index_names

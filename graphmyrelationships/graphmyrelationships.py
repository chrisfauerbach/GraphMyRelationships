"""

Start coding to find things.
1)  Figure out index patterns   ( config file for now? )
2)  Find indexes that match
3)

"""

__all__ = []
__version__ = '0.1'
__author__ = 'Chris Fauerbach'
__email__ = 'chris@fauie.com'

import json
import logging
import sys
from neo4jpersist import Neo4J
import re

from config import Config, IndexPattern
from elastic import ElasticService



def begin_data_pull(config):

    idx_caching = {}

    neopersist  = Neo4J(config) #type: Neo4J
    elastic_source =  ElasticService(config)

    for pattern in CONFIG.get("INDEX_PATTERNS"):
        name = pattern.get("name")
        regex = pattern.get("regex")
        if not name:
            raise Exception("Unable to work with an index pattern that doesn't have a [name] attribute.")
        if not regex:
            raise Exception("Unable to work with an index pattern that doesn't have a [regex] attribute.")
        pattern = config.append_index_pattern(name, regex)
        neopersist.persist_pattern( pattern )


    elastic_indices = elastic_source.list_indices()
    for idx in elastic_indices:
        mappings = elastic_source.list_types(idx)
        idx_caching[idx] = mappings

        LOGGER.debug(mappings)
        for _type in mappings:
            LOGGER.debug("Looking at _Type: {} ".format(_type))
            neopersist.persist_type_and_index(idx, _type)
            for _property in mappings.get(_type).get('properties',{}):
                LOGGER.debug("Property!: %s", _property )
                neopersist.persist_type_and_property(_type, _property)

    for idx in elastic_indices:
        matched = False
        for pattern in config.index_patterns:
            name = pattern.name
            regex = pattern.regex
            compiled_regex = re.compile(regex)
            if compiled_regex.match( idx ):
                matched = True
                LOGGER.debug( "Comparing {} to {} with regex {} ".format( idx, name, regex) )
                LOGGER.debug("MATCHES")
                neopersist.persist_pattern_and_index( pattern, idx)
                mappings = idx_caching[idx]
                #mappings = elastic_source.list_types(idx)
                for _type in mappings:
                    neopersist.persist_pattern_and_type(pattern, _type)
                for _property in mappings.get(_type).get('properties',{}):
                    LOGGER.debug("Property!: %s", _property )
                    neopersist.persist_pattern_and_property(pattern,  _property)
        if not matched:
            LOGGER.error("UNMATCHED: {}".format(idx))


def read_config():
    global CONFIG
    try:
        with  open(CONFIG_FILE_NAME, 'rb') as config_file:
            contents = config_file.read()
            parsed = json.loads( contents )
            CONFIG = parsed
    except:
        LOGGER.exception("Unable to read config file.")

def validate(parameter, optional=False):
    if not optional:
        return CONFIG[parameter]
    else:
        if not parameter in CONFIG:
            LOGGER.warning("%s is marked as optional, and missing.", parameter)
        else:
            return CONFIG[parameter]

def main():
    read_config()
    config = Config()
    config.elastic_host = validate("ELASTIC_HOST")
    config.elastic_port = validate("ELASTIC_PORT")
    config.neo4j_host = validate("NEO4J_HOST")
    config.neo4j_port = validate("NEO4J_PORT")
    config.neo4j_user = validate("NEO4J_USERNAME", True)
    config.neo4j_password = validate("NEO4J_PASSWORD", True)

    validate("INDEX_PATTERNS")

    begin_data_pull( config )


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    CONFIG_FILE_NAME = "config.json"
    CONFIG = {}
    main()


"""
What does this module do?  
Does it do things?

"""

__all__ = []
__version__ = '0.1'
__author__ = 'Chris Fauerbach'
__email__ = 'chris@fauie.com'



class Config():
    neo4j_host = None
    neo4j_port = None
    neo4j_password = None
    neo4j_user = None
    elastic_host = None
    elastic_port = None
    index_patterns = []

    def __init__(self):
        pass

    def append_index_pattern(self, name, regex):

        pattern =  IndexPattern(name, regex)
        Config.index_patterns.append(pattern)
        return pattern


class IndexPattern():
    def __init__(self, name, regex):
        self.name = name
        self.regex = regex




import copy
from datetime import datetime
import os
import six
import uuid

from py2neo import ClientError, Graph, Node, TransientError
from neo4j import Transaction

HOST = "notebook.leafe.com"
PASSWORD = "placement"

_connection = None

def get_connection():
    global _connection
    if _connection is None:
        _connection = _connect()
    return _connection


class DotDict(dict):
    """
    Dictionary subclass that allows accessing keys via dot notation. This is
    useful for replacing full objects with lighter-weight dict representations.

    If the key is not present, an AttributeError is raised.
    """
    _att_mapper = {}
    _fail = object()

    def __init__(self, *args, **kwargs):
        super(DotDict, self).__init__(*args, **kwargs)

    def __getattr__(self, att):
        att = self._att_mapper.get(att, att)
        ret = self.get(att, self._fail)
        if ret is self._fail:
            raise AttributeError("'%s' object has no attribute '%s'" %
                    (self.__class__.__name__, att))
        return ret

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def pythonize(gr_node):
    """Takes a node returned from the graph and returns a python object with
    values converted to Python values.
    """
    def to_python_dt(val):
        if not val:
            return val
        if isinstance(val, six.string_types):
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S %Z")
        else:
            # Neo4j timestamp in milliseconds
            seconds = val / 1000
            return datetime.fromtimestamp(seconds)
        return val

    # 'gr_node' is a Neo4j Node object; extract the dict values from it.
    ret = dict(gr_node.items())
    # Convert datetime fields
    val = ret.get("created_at")
    if val:
        ret["created_at"] = to_python_dt(val)
    val = ret.get("updated_at")
    if val:
        ret["updated_at"] = to_python_dt(val)
    return DotDict(ret)


def _connect():
    """Connects to the Node4j server, using the environment variables if
    present, or 'localhost' if not.
    NOTE: This needs to be updated to use CONF settings
    """
    host = HOST or os.getenv("NEO4J_HOST") or "localhost"
    password = PASSWORD or os.getenv("NEO4J_PASSWORD") or "secret"
    return Graph(host=host, password=password)


def begin_transaction(g=None, autocommit=False):
    if not g:
        g = _connect()
    return g.begin(autocommit=autocommit)


def commit_transaction(tx):
    tx.commit()


def rollback_transaction(tx):
    tx.rollback()


def execute(query, tx=None, autocommit=True):
    """Execute a query. If a transaction is supplied via the `tx` parameter, it
    will be used. Otherwise, a new transaction will be created using the value
    for autocommit. Note that autocommit is ignored for existing transactions.
    """
    if tx is None:
        tx = begin_transaction(autocommit=autocommit)
    crs = tx.run(query)
    return crs.data()


def delete_all():
    g = _connect()
    tx = begin_transaction(g=g, autocommit=False)
    g.delete_all()
    tx.commit()


def gen_uuid():
    return str(uuid.uuid4())


def create_node(tx, typ, name_base, num=None, **kwargs):
    """Convenience method for creating a series of nodes with incrementing
    names and UUIDs.
    """
    if num is None:
        nm = name_base
    else:
        num = str(num).zfill(4)
        nm = "%s%s" % (name_base, num)
    u = str(uuid.uuid4())
    new_node = Node(typ, name=nm, uuid=u, **kwargs)
    tx.create(new_node)
    return new_node


def trait_args(traits):
    traits = traits if traits else []
    return {t: True for t in traits}


def get_root_node(node_uuid, relationship=":CONTAINS"):
    """Given the UUID of a node, returns the UUID of the "furthest" node from
    it in the specified relationship.
    """
    query = """
            MATCH p=(root)-[%s*0..99]->(nd {uuid: '%s'})
            WITH root, p, size(relationships(p)) AS relsize
            ORDER BY relsize DESC
            RETURN root.uuid AS root_uuid
    """ % (relationship, node_uuid)
    result = execute(query)
    if result:
        return result[0]["root_uuid"]

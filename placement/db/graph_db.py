from datetime import datetime
import os
import uuid

from py2neo import ClientError, Graph, Node

HOST = "notebook.leafe.com"
PASSWORD = "placement"


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


def pythonize(gdict):
    """Takes a dict returned from the graph and converts as needed to a python
    object.
    """
    def to_python_dt(text):
        if text:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S %Z")
        return text

    # Convert datetime fields
    val = gdict.get("created_at")
    if val:
        gdict["created_at"] = to_python_dt(val)
    val = gdict.get("updated_at")
    if val:
        gdict["updated_at"] = to_python_dt(val)
    return DotDict(gdict)


def connect():
    """Connects to the Node4j server, using the environment variables if
    present, or 'localhost' if not.
    NOTE: This needs to be updated to use CONF settings
    """
    host = HOST or os.getenv("NEO4J_HOST") or "localhost"
    password = PASSWORD or os.getenv("NEO4J_PASSWORD") or "secret"
    return Graph(host=host, password=password)


def begin_transaction(g=None, autocommit=True):
    if not g:
        g = connect()
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
    g = connect()
    tx = begin_transaction(g=g)
    g.delete_all()
    tx.commit()


def gen_uuid():
    return f"{uuid.uuid4()}"


def create_node(tx, typ, name_base, num=None, **kwargs):
    """Convenience method for creating a series of nodes with incrementing
    names and UUIDs.
    """
    if num is None:
        nm = name_base
    else:
        num = f"{num}".zfill(4)
        nm = f"{name_base}{num}"
    u = f"{uuid.uuid4()}"
    new_node = Node(typ, name=nm, uuid=u, **kwargs)
    tx.create(new_node)
    return new_node


def trait_args(traits):
    traits = traits if traits else []
    return {t: True for t in traits}

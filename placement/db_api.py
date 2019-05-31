#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Database context manager for placement database connection."""

import contextlib
import functools
import inspect

from oslo_log import log as logging
import neo4j

from placement.util import run_once

LOG = logging.getLogger(__name__)


# For development. These will be removed when this project moves forward.
HOST = "notebook.leafe.com"
USERNAME = "neo4j"
PASSWORD = "placement"


def transaction_context_provider(cls):
    """Decorate a class with ``session`` and ``connection`` attributes."""
    setattr(cls, 'transaction_ctx', None)
    for attr in ('session', 'connection', 'transaction'):
        setattr(cls, attr, None)
    return cls


class FakeFactory():
    _writer_engine = None

    def _start(self, *args, **kwargs):
        pass


class FakeEngine():
    url = "fake://fake"

    def _run_visitor(self, *args, **kwargs):
        # TODO(edleafe): figure out how to deal with this
        pass


class TransactionContext():
    def __init__(self, mode=None, independent=False):
        self.driver = None
        self.transaction = None
        self._mode = mode
        self._independent = independent

        # Hackish stuff to satisfy oslo_db
        self._factory = FakeFactory()

    def _clone(self, **kw):
        default_kw = {
            "independent": self._independent,
            "mode": self._mode,
        }
        default_kw.update(kw)
        return TransactionContext(**default_kw)

    @property
    def reader(self):
        return self._clone(mode="read")

    @property
    def writer(self):
        return self._clone(mode="write")

    @property
    def independent(self):
        """Modifier to start a transaction independent from any enclosing."""
        # TODO(edleafe): figure out how to deal with this
        return self._clone(independent=True)

    @property
    def savepoint(self):
        """Modifier to start a SAVEPOINT if a transaction already exists."""
        # TODO(edleafe): figure out how to deal with this
        return self

    def configure(self, *args, **kwargs):
        # TODO(edleafe): figure out how to deal with this
        pass

    def make_new_manager(self, *args, **kwargs):
        # TODO(edleafe): figure out how to deal with this
        return self

    def get_engine(self, *args, **kwargs):
        # TODO(edleafe): figure out how to deal with this
        return FakeEngine()

    def patch_engine(self, *args, **kwargs):
        # TODO(edleafe): figure out how to deal with this
        return FakeEngine()

    def patch_factory(self, factory_or_manager=None):
        # TODO(edleafe): figure out how to deal with this
        def reset(*args, **kwargs):
            pass
        return reset

    def __call__(self, fn):
        """Decorate a function."""
        argspec = inspect.getargspec(fn)
        context_index = 1 if argspec.args[0] in("self", "cls") else 0

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            context = args[context_index]
            with self._transaction_scope(context):
                return fn(*args, **kwargs)
        return wrapper

    @contextlib.contextmanager
    def _transaction_scope(self, context):
        if self.driver is None:
            self.driver = self._connect()
        with self.driver.session() as session:
            tx = context.transaction = session.begin_transaction()
            try:
                yield
                if self._mode == "write":
                    tx.commit()
            except Exception:
                tx.rollback()
            finally:
                if not tx.closed():
                    tx.rollback()

    def _connect(self):
        """Connects to the Node4j server, using the environment variables if
        present, or 'localhost' if not.
        NOTE: This needs to be updated to use CONF settings
        """
        host = HOST or os.getenv("NEO4J_HOST") or "localhost"
        uri = "bolt://%s:7687" % host
        username = USERNAME or os.getenv("NEO4J_USERNAME") or "neo4j"
        password = PASSWORD or os.getenv("NEO4J_PASSWORD") or "secret"
        driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))
        return driver


placement_context_manager = TransactionContext()


def _get_db_conf(conf_group):
    conf_dict = dict(conf_group.items())
    # Remove the 'sync_on_startup' conf setting, enginefacade does not use it.
    # Use pop since it might not be present in testing situations and we
    # don't want to care here.
    conf_dict.pop('sync_on_startup', None)
    return conf_dict


@run_once("TransactionFactory already started, not reconfiguring.",
          LOG.warning)
def configure(conf):
    placement_context_manager.configure(
        **_get_db_conf(conf.placement_database))


def get_placement_engine():
    return placement_context_manager.writer.get_engine()


@transaction_context_provider
class DbContext(object):
    """Stub class for db session handling outside of web requests."""

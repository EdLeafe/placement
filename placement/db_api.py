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
import queue

from oslo_log import log as logging
from oslo_utils import excutils
import py2neo

from placement.db import graph_db as db
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
    # Add the transaction queue
    setattr(cls, "tx_queue", queue.LifoQueue())
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
        self.transaction = None
        self._mode = mode
        self._independent = independent
        self.tx_queue = queue.LifoQueue()

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

    @staticmethod
    def _context_tx_active(ctx):
        if hasattr(ctx, "tx"):
            tx = ctx.tx
            if isinstance(tx, py2neo.Transaction):
                return not tx.finished()
        return False

    def __call__(self, fn):
        """Decorate a function."""

        def _print(*args):
            msg = " ".join([str(arg) for arg in args])
            with open("OUT", "a") as ff:
                ff.write("%s\n" % msg)

        argspec = inspect.getargspec(fn)
        context_index = 1 if argspec.args[0] in("self", "cls") else 0
        cxn = db.get_connection()

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            context = args[context_index]
            _print(str(fn), "CTX", id(context))
            if self._context_tx_active(context):
                _print("RESUING", id(context.tx), "CTX", id(context))
                return fn(*args, **kwargs)
            with cxn.begin() as tx:
                _print("BEGIN", id(tx), "CTX", id(context))
                context.tx = tx
                try:
                    ret = fn(*args, **kwargs)
                except Exception as e:
                    _print("EXC", id(tx), e, "CTX", id(context))
                    with excutils.save_and_reraise_exception():
                        pass
                finally:
                    _print("END", id(tx), "CTX", id(context), tx.finished())
                    pass
                return ret
        return wrapper

    @contextlib.contextmanager
    def _transaction_scope(self, context):
        cxn = db.get_connection()
        old_tx = context.tx if hasattr(context, "tx") else None
        tx = cxn.begin()
        tx.pop = tx.success = False
        context.tx = tx
        if old_tx:
            context.tx_queue.put(old_tx)
            tx.pop = True

        try:
            yield
            # Marking the transaction as successful will result in a COMMIT
            # when it is closed. When in read mode, we always want to rollback
            # any changes.
            tx.success = True   #(self._mode == "write") or self._independent
        except Exception as e:
            with excutils.save_and_reraise_exception():
                tx.success = False
        finally:
            if not tx.finished():
                if tx.success:
                    tx.commit()
                else:
                    tx.rollback()
            # Pop the transaction from the queue, if any
            if tx.pop:
                context.tx = context.tx_queue.get()


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
class DbContext(TransactionContext):
    """Stub class for db session handling outside of web requests."""
    def __init__(self, *args, **kwargs):
        super(DbContext, self).__init__(*args, **kwargs)
        self._independent = True

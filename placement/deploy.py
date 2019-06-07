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
"""Deployment handling for Placmenent API."""

import os

from microversion_parse import middleware as mp_middleware
import oslo_middleware
from oslo_middleware import cors

from placement import auth
from placement.db.sqlalchemy import migration
from placement import db_api
from placement import fault_wrap
from placement import handler
from placement import microversion
from placement.objects import resource_class
from placement.objects import trait
from placement import policy
from placement import requestlog
from placement import resource_class_cache as rc_cache
from placement import util


PROFILER_OUTPUT = os.environ.get('OS_WSGI_PROFILER')
if PROFILER_OUTPUT:
    # If werkzeug is not available this raises ImportError and the
    # process will not continue. This is intentional: we do not want
    # to make a permanent dependency on werkzeug.
    from werkzeug.contrib import profiler


def deploy(conf):
    """Assemble the middleware pipeline leading to the placement app."""
    if conf.api.auth_strategy == 'noauth2':
        auth_middleware = auth.NoAuthMiddleware
    else:
        # Do not use 'oslo_config_project' param here as the conf
        # location may have been overridden earlier in the deployment
        # process with OS_PLACEMENT_CONFIG_DIR in wsgi.py.
        auth_middleware = auth.filter_factory(
            {}, oslo_config_config=conf)

    # Pass in our CORS config, if any, manually as that's a)
    # explicit, b) makes testing more straightfoward, c) let's
    # us control the use of cors by the presence of its config.
    conf.register_opts(cors.CORS_OPTS, 'cors')
    if conf.cors.allowed_origin:
        cors_middleware = oslo_middleware.CORS.factory(
            {}, **conf.cors)
    else:
        cors_middleware = None

    context_middleware = auth.PlacementKeystoneContext
    req_id_middleware = oslo_middleware.RequestId
    microversion_middleware = mp_middleware.MicroversionMiddleware
    fault_middleware = fault_wrap.FaultWrapper
    request_log = requestlog.RequestLog

    application = handler.PlacementHandler(config=conf)

    # If PROFILER_OUTPUT is set, generate per request profile reports
    # to the directory named therein.
    if PROFILER_OUTPUT:
        application = profiler.ProfilerMiddleware(
            application, profile_dir=PROFILER_OUTPUT)

    # configure microversion middleware in the old school way
    application = microversion_middleware(
        application, microversion.SERVICE_TYPE, microversion.VERSIONS,
        json_error_formatter=util.json_error_formatter)

    # NOTE(cdent): The ordering here is important. The list is ordered
    # from the inside out. For a single request req_id_middleware is called
    # first and microversion_middleware last. Then the request is finally
    # passed to the application (the PlacementHandler). At that point
    # the response ascends the middleware in the reverse of the
    # order the request went in. This order ensures that log messages
    # all see the same contextual information including request id and
    # authentication information.
    for middleware in (fault_middleware,
                       request_log,
                       context_middleware,
                       auth_middleware,
                       cors_middleware,
                       req_id_middleware,
                       ):
        if middleware:
            application = middleware(application)

    # NOTE(mriedem): Ignore scope check UserWarnings from oslo.policy.
    if not conf.oslo_policy.enforce_scope:
        import warnings
        warnings.filterwarnings('ignore',
                                message="Policy .* failed scope check",
                                category=UserWarning)

    return application


def update_database(conf):
    """Do any database updates required at process boot time, such as
    updating the traits table.
    """
    if conf.placement_database.sync_on_startup:
        migration.upgrade('head')
    ctx = db_api.DbContext()
    trait.ensure_sync(ctx)
    resource_class.ensure_sync(ctx)
    rc_cache.ensure(ctx)


# NOTE(cdent): Althought project_name is no longer used because of the
# resolution of https://bugs.launchpad.net/nova/+bug/1734491, loadapp()
# is considered a public interface for the creation of a placement
# WSGI app so must maintain its interface. The canonical placement WSGI
# app is created by init_application in wsgi.py, but this is not
# required and in fact can be limiting. loadapp() may be used from
# fixtures or arbitrary WSGI frameworks and loaders.
def loadapp(config, project_name=None):
    """WSGI application creator for placement.

    :param config: An olso_config.cfg.ConfigOpts containing placement
                   configuration.
    :param project_name: oslo_config project name. Ignored, preserved for
                         backwards compatibility
    """
    application = deploy(config)
    policy.init(config)
    update_database(config)
    return application

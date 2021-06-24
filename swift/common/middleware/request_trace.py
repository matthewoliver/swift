# Copyright (c) 2021 NVIDIA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
The ``request trace`` middleware is responsible for initiating the tracing
on incoming requests. Once tracing it initiated on a request the trace lasts
from the back to this middleware, so make sure it's added towards the front
of the pipeline.

There are 2 ways to enable tracing of a request, the most useful way is via
a trace_sig that can be added to the query_string of a request. The
query_string needs to include 2 parameters, a trace_sig and a trace_expires.
The trace_sig is a hmac based of the secret `trace_key` which must be
specified in the middleware configuration and an expiry timestamp.

If the hmac matches the signature then tracing of the request will start.
The other approach is to enable tracing on every x'th request. This can be
done via the `trace_every_x` configuration option. This second option would
be used in places you can't send a trace_sig, like internal client.

The trace is exported out via the opentracing framework. As such you need
to provide a tracer client.

TOOD: add tracer client details.
"""
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.propagate import extract

from swift.common.digest import get_allowed_digests, DEFAULT_ALLOWED_DIGESTS
from swift.common.swob import Request
from swift.common.utils import get_logger, config_true_value, \
    non_negative_int
from swift.common.trace import wsgi_trace, TRACE_ACTIVATED_KEY, \
    enable_trace, TRACE_TRACER, append_current_span, pop_current_span, \
    TRACE_ROOT_SPAN
from swift.common.trace.tracer import init_tracer
import os


@wsgi_trace
class RequestTraceMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='tracer')
        self.trace_every_x = non_negative_int(conf.get('trace_every_x', 0))
        self.req_count = 0
        self.key = conf.get('trace_key')
        self.trace_name = conf.get('trace_name', 'swift')
        self.trace_name = "%s %d" % (self.trace_name, os.getpid())
        self.allowed_digests = conf.get(
            'allowed_digests', DEFAULT_ALLOWED_DIGESTS.split())

        # grab the tracer module
        self.tracer_module = conf.get('trace_exporter_module')
        self.tracer_config = conf.get('trace_exporter_config')

    def increment_req_count(self):
        self.req_count += 1
        if self.trace_every_x == 1:
            return 1
        if self.req_count > self.trace_every_x:
            self.req_count = self.req_count % self.trace_every_x
        return self.req_count

    def __call__(self, env, start_response):
        req = Request(env)
        if self.key and req.params.get("trace_sig"):
            enable_trace(env, self.key, self.allowed_digests)
        elif self.trace_every_x:
            if self.increment_req_count() == self.trace_every_x:
                env[TRACE_ACTIVATED_KEY] = 'yes'
        # Check to see if we should continue a trace, the trace should
        # have been injected into the headers. We pass along an
        # X-Backend-Tracing-Continue header if it does. Because different
        # tracers use different headers keys.
        extract_context = False
        if config_true_value(
                req.headers.get('X-Backend-Tracing-Continue', 'no')):
            env[TRACE_ACTIVATED_KEY] = 'yes'
            extract_context = True

        if config_true_value(env.get(TRACE_ACTIVATED_KEY, 'no')):
            tracer = init_tracer(
                self.tracer_module, self.tracer_config, self.trace_name,
                self.logger)
            env[TRACE_TRACER] = tracer
            txid = req.environ.get('swift.trans_id',
                                   req.environ.get('HTTP_X_TRANS_ID', '-'))
            extracted_context = None
            if extract_context:
                extracted_context = extract(req.headers)

            with tracer.start_as_current_span(
                    "%s (%s)" % (txid, req.method),
                    context=extracted_context,
                    end_on_exit=False) as span:
                span.set_attribute(SpanAttributes.HTTP_METHOD, req.method)
                span.set_attribute(SpanAttributes.HTTP_URL, req.path)
                span.set_attribute('txid', txid)
                span.set_attribute('swift.source',
                                   req.environ.get('swift.source', ''))
                append_current_span(env, span)
                # we'll use this span to attach any spans that might loose
                # its parent, like to log_request span that isn't created
                # until other spans have already closed. Think of it as a
                # catch all
                env[TRACE_ROOT_SPAN] = span

                def trace_response(status, response_headers, exc_info=None):
                    status_int = status
                    if not isinstance(status_int, int):
                        status_int = int(status_int.split(' ', 1)[0])
                    span.set_attribute(
                        SpanAttributes.HTTP_STATUS_CODE, status_int)
                    span.end()
                    pop_current_span(env, span)
                    return start_response(status, response_headers, exc_info)

                return self.app(env, trace_response)
        else:
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    logger = get_logger(conf, log_route='request_trace')
    allowed_digests, deprecated_digests = get_allowed_digests(
        conf.get('allowed_digests', '').split(), logger)
    info = {'allowed_digests': sorted(allowed_digests)}
    conf.update(info)

    def trace_filter(app):
        return RequestTraceMiddleware(app, conf)
    return trace_filter

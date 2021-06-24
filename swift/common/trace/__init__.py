# Copyright (c) 2022 NVIDIA
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
import functools
import hashlib
import hmac
import json
import time

from calendar import timegm
from itertools import chain
from collections import OrderedDict
from contextlib import contextmanager
from urllib.parse import parse_qs

from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry import trace

from swift.common import digest
from swift.common import utils

TRACE_ACTIVATED_KEY = "swift.trace"
TRACE_DATA = "swift.trace_data"
TRACE_TRACER = 'swift.trace_tracer'
TRACE_ROOT_SPAN = 'swift.trace_root_span'
TRACE_ENV_KEYS = [TRACE_ACTIVATED_KEY, TRACE_DATA, TRACE_TRACER]


def get_tracer(env):
    if isinstance(env, dict):
        return env.get(TRACE_TRACER)


def is_tracing(env=None):
    if not env:
        return trace_get_current_span(env) is not None
    return utils.config_true_value(env.get(TRACE_ACTIVATED_KEY, 'no'))


def get_trace_headers(env=None):
    trace_headers = {}
    if is_tracing(env):
        span = trace_get_current_span(env)
        if span:
            inject(trace_headers)
            trace_headers['X-Backend-Tracing-Continue'] = 'yes'
    return trace_headers


def append_current_span(env, span):
    env.setdefault(TRACE_DATA, OrderedDict()).setdefault(
        'span_stack', []).append(span)


def pop_current_span(env, span):
    span_stack = env.setdefault(TRACE_DATA, OrderedDict()).setdefault(
        'span_stack', [])
    if span in span_stack:
        span_stack.remove(span)


@contextmanager
def trace_force_current_span(env, force=False):
    current_span = trace_get_current_span(env)
    if not current_span:
        # set it to the fallback if there is one.
        if env.get(TRACE_ROOT_SPAN):
            current_span = env[TRACE_ROOT_SPAN]
            force = True
    if force and current_span:
        with trace.use_span(current_span, end_on_exit=False):
            yield
    else:
        yield


@contextmanager
def new_trace_span(env, key_name, force_current_span=False):
    tracer = get_tracer(env)
    if tracer and is_tracing(env):
        with trace_force_current_span(env, force=force_current_span):
            with tracer.start_as_current_span(key_name) as span:
                append_current_span(env, span)
                yield span
                pop_current_span(env, span)
    else:
        yield


def trace_add(key, value, env=None):
    if is_tracing(env):
        current_span = trace_get_current_span(env)
        if current_span:
            if isinstance(value, dict):
                value = json.dumps(value)
            current_span.set_attribute(key, value)


def trace_exception(ex, env=None):
    if is_tracing(env):
        span = trace_get_current_span(env)
        if span:
            span.record_exception(ex)


def enable_trace(env, key, allowed_digests):
    trace_sig = trace_expires = None
    qs = parse_qs(env.get('QUERY_STRING', ''), keep_blank_values=True)
    if 'trace_sig' in qs:
        trace_sig = qs['trace_sig'][0]
    if 'trace_expires' in qs:
        try:
            trace_expires = int(qs['trace_expires'][0])
        except ValueError:
            try:
                trace_expires = timegm(time.strptime(
                    qs['trace_expires'][0], utils.EXPIRES_ISO8601_FORMAT))
            except ValueError:
                trace_expires = 0
        if trace_expires < time.time():
            trace_expires = 0
    try:
        hash_name, signature = digest.extract_digest_and_algorithm(trace_sig)
    except ValueError:
        return

    if hash_name not in allowed_digests:
        return

    if trace_expires and get_trace_hmac(key, trace_expires, hash_name) \
            == trace_sig:
        env[TRACE_ACTIVATED_KEY] = 'yes'


def get_trace_hmac(key, expires, hash_algorithm):
    parts = ["trace", str(expires)]
    formats = [b"%s", b"%s"]

    if not isinstance(key, bytes):
        key = key.encode('utf8')

    message = b'\n'.join(
        fmt % (part if isinstance(part, bytes)
               else part.encode("utf-8"))
        for fmt, part in zip(formats, parts))

    digest = functools.partial(hashlib.new, hash_algorithm)

    return hmac.new(key, message, digest).hexdigest()


def trace_get_current_span(env=None):
    span = None
    if env:
        span_stack = env.get(TRACE_DATA, {}).get('span_stack', [])
        for s in reversed(span_stack):
            if s and not s.end_time:
                # We have the latest span
                return s
    if not isinstance(trace.get_current_span(), trace.NonRecordingSpan):
        span = trace.get_current_span()
    return span


def trace_function(func=None, force_span=False):
    """
    Returns a decorator that wraps the given function in a span. The tracer
    that creates spans lives in a wsgi request env. So we can only decorate
    funtions that have some kind of request env passed in.
    So the decorator needs to look for a wsgi env or swift.swob.Request object
    in the args/kargs sent in to the function and uses the first it finds.
    """
    if func is None:
        return functools.partial(trace_function, force_span=force_span)

    method = func.__name__

    @functools.wraps(func)
    def _trace_span(ctrl, *args, **kwargs):
        # find the wsgi environment, so we can check to see if we're
        # tracing
        env = None
        req_args = [a for a in chain(args, kwargs.values())
                    if hasattr(a, "environ")]
        env_args = [a for a in chain(args, kwargs.values())
                    if isinstance(a, dict) and a.get("REQUEST_METHOD")]
        if req_args:
            env = req_args[0].environ
        elif env_args:
            env = env_args[0]

        if env and is_tracing(env):
            # we have the env. And it's tracing. So let's grab the tracer
            tracer = get_tracer(env)
            if tracer:
                with trace_force_current_span(env, force_span):
                    with tracer.start_as_current_span(method):
                        return func(ctrl, *args, **kwargs)
        else:
            return func(ctrl, *args, **kwargs)

    return _trace_span


def wsgi_trace(wsgi_class):
    orig_call = wsgi_class.__call__

    def _wsgi_trace_call(cls, env, start_response):
        if not utils.config_true_value(env.get(TRACE_ACTIVATED_KEY)):
            return orig_call(cls, env, start_response)
        tracer = get_tracer(env)
        if not tracer:
            # failed to get tracer from the env (might have to log something)
            return orig_call(cls, env, start_response)
        # trace has been activated. So collect data.
        trace_data = env.setdefault(TRACE_DATA, OrderedDict())
        wsgi_name = wsgi_class.__name__
        call_stack = trace_data.setdefault('call_stack', [])
        if wsgi_name in call_stack:
            wsgi_name = "%s_%d" % (
                wsgi_name, len([i for i in call_stack
                                if i.startswith(wsgi_name)]))
        call_stack.append(wsgi_name)
        with tracer.start_as_current_span(wsgi_name,
                                          end_on_exit=False) as span:

            def trace_response(status, response_headers, exc_info=None):
                status_int = status
                if not isinstance(status_int, int):
                    status_int = int(status_int.split(' ', 1)[0])

                span.set_attribute(
                    SpanAttributes.HTTP_STATUS_CODE, status_int)
                span.end()
                pop_current_span(env, span)
                return start_response(status, response_headers, exc_info)

            append_current_span(env, span)
            trace_add("swift.source", env.get('swift.source', ''), env)
            # call the child call
            resp = orig_call(cls, env, trace_response)
        return resp

    wsgi_class.__call__ = _wsgi_trace_call
    return wsgi_class

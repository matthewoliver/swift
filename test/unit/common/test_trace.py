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
import unittest

from operator import attrgetter

from swift.common import trace, utils
from swift.common.swob import HTTPOk
from test.unit import get_debug_tracer_and_exporter, with_tempdir
from test.debug_logger import debug_logger


class TestTraceMixin(unittest.TestCase):
    @trace.wsgi_trace
    class FakeTraceApp(object):
        def __call__(self, env, start_response):
            trace.trace_add('where_am_i', 'FakeTraceApp', env)
            resp = HTTPOk()
            resp(env, start_response)
            return resp

    @trace.wsgi_trace
    class TraceMiddleware1(object):
        def __init__(self, app):
            self.app = app

        def __call__(self, env, start_response):
            trace.trace_add('where_am_i', 'TraceMiddleware1', env)
            return self.app(env, start_response)

    @trace.wsgi_trace
    class TraceMiddleware2(TraceMiddleware1):

        def __call__(self, env, start_response):
            trace.trace_add('where_am_i', 'TraceMiddleware2', env)
            return self.app(env, start_response)

    @trace.wsgi_trace
    class TraceMiddlewareShort(TraceMiddleware1):

        def __call__(self, env, start_response):
            trace.trace_add('where_am_i', 'TraceMiddlewareShort', env)
            if env.get('short'):
                resp = HTTPOk()
                resp(env, start_response)
                return resp
            return self.app(env, start_response)

    def setUp(self):
        self.logger = debug_logger('trace')
        self.app = \
            self.TraceMiddleware1(
                self.TraceMiddleware2(
                    self.TraceMiddlewareShort(
                        self.FakeTraceApp())))

    @with_tempdir
    def test_trace(self, tmpdir):
        env = {}

        def start_response(*args, **kwargs):
            pass

        in_memory_tracer, in_memory = get_debug_tracer_and_exporter()

        # first lets not include the 'swift.trace' or TRACE_ACTIVATED_KEY to
        # the env so no trace will happen
        resp = self.app(env, start_response)
        self.assertTrue(resp.is_success)
        self.assertEqual(200, resp.status_int)
        self.assertFalse(env.get(trace.TRACE_ACTIVATED_KEY))

        # same if the trace env is not a config_true_value
        env[trace.TRACE_ACTIVATED_KEY] = 'no'
        resp = self.app(env, start_response)
        self.assertTrue(resp.is_success)
        self.assertEqual(200, resp.status_int)
        self.assertFalse(utils.config_true_value(
            env.get(trace.TRACE_ACTIVATED_KEY)))

        # now we do a full trace
        env[trace.TRACE_ACTIVATED_KEY] = 'yes'
        env[trace.TRACE_TRACER] = in_memory_tracer
        env_with_trace_data = env.copy()
        resp = self.app(env_with_trace_data, start_response)
        self.assertTrue(resp.is_success)
        self.assertEqual(200, resp.status_int)
        self.assertTrue(env_with_trace_data.get('swift.trace_data'))

        wsgis = ['TraceMiddleware1', 'TraceMiddleware2',
                 'TraceMiddlewareShort', 'FakeTraceApp']
        self.assertEqual(env_with_trace_data['swift.trace_data']['call_stack'],
                         wsgis)

        def assert_traces(wsgi_middlewares, finished_spans):
            self.assertEqual(list(reversed(wsgis)),
                             [s.name for s in finished_spans])
            finished_spans_map = {s.name: s for s in finished_spans}

            # the attributes are set
            for app in wsgi_middlewares:
                self.assertIn(
                    'where_am_i', finished_spans_map[app].attributes)
                self.assertEqual(
                    app, finished_spans_map[app].attributes['where_am_i'])

            # the first wsgi has the eariest start and latest end time
            min_start = min(map(attrgetter('start_time'), finished_spans))
            max_end = max(map(attrgetter('_end_time'), finished_spans))
            self.assertEqual(
                min_start, finished_spans_map[wsgi_middlewares[0]].start_time)
            self.assertEqual(
                max_end, finished_spans_map[wsgi_middlewares[0]]._end_time)

            # the last wsgi (the app in this case) has the latest start and
            # earliest end time
            max_start = max(map(attrgetter('start_time'), finished_spans))
            min_end = min(map(attrgetter('_end_time'), finished_spans))
            self.assertEqual(
                max_start, finished_spans_map[wsgi_middlewares[-1]].start_time)
            self.assertEqual(
                min_end, finished_spans_map[wsgi_middlewares[-1]]._end_time)

        # Now let's look at the in memory trace exporter.
        assert_traces(wsgis, in_memory.get_finished_spans())

        # if we hit a middleware that responds before reaching the app
        # (shortcut), then obviously the proxy wont be visited
        in_memory_tracer, in_memory = get_debug_tracer_and_exporter()
        env['short'] = 'yes'
        env[trace.TRACE_TRACER] = in_memory_tracer
        env_with_trace_data = env.copy()
        resp = self.app(env_with_trace_data, start_response)
        self.assertTrue(resp.is_success)
        self.assertEqual(200, resp.status_int)
        self.assertTrue(env_with_trace_data.get('swift.trace_data'))
        # Notice no FakeTraceApp in wsgi's visited this time.
        wsgis = ['TraceMiddleware1', 'TraceMiddleware2',
                 'TraceMiddlewareShort']
        self.assertEqual(env_with_trace_data['swift.trace_data']['call_stack'],
                         wsgis)

        assert_traces(wsgis, in_memory.get_finished_spans())


if __name__ == '__main__':
    unittest.main()

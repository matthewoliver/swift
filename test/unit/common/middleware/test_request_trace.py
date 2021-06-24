# Copyright (c) 2023 NVIDIA
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
from unittest import mock

from swift.common.digest import DEFAULT_ALLOWED_DIGESTS
from swift.common.middleware import request_trace
from swift.common.swob import Request
from swift.common.trace import TRACE_ACTIVATED_KEY
from test.unit import activate_tracing


class FakeApp(object):
    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return [b'Some Content']


def start_response(*args):
    pass


class TestRequestTrace(unittest.TestCase):
    def test_config(self):
        # First test the defaults
        rt = request_trace.RequestTraceMiddleware(FakeApp, {})
        self.assertEqual(rt.trace_every_x, 0)
        self.assertIsNone(rt.key)
        self.assertTrue(rt.trace_name.startswith('swift'))
        self.assertEqual(rt.allowed_digests, DEFAULT_ALLOWED_DIGESTS.split())
        self.assertIsNone(rt.tracer_module)
        self.assertIsNone(rt.tracer_config)

        # Now let's change some settings
        config = {'trace_every_x': 1000,
                  'trace_key': 'my_key',
                  'trace_name': 'my awesome tracy thing',
                  'trace_exporter_config': 'my_config.json',
                  'trace_exporter_module': 'egg://some.random_trace:exporter'}
        rt = request_trace.RequestTraceMiddleware(FakeApp, config)
        self.assertEqual(rt.trace_every_x, 1000)
        self.assertEqual(rt.key, 'my_key')
        self.assertTrue(rt.trace_name.startswith('my awesome tracy thing'))
        self.assertEqual(rt.tracer_module, 'egg://some.random_trace:exporter')
        self.assertEqual(rt.tracer_config, 'my_config.json')

        # trace_every_x needs to a none negitive int >= 0, floats will be
        # converted to an int.
        for trace_every_x in (-1, -1.6, 'bad'):
            config = {'trace_every_x': trace_every_x}
            with self.assertRaises(ValueError):
                rt = request_trace.RequestTraceMiddleware(FakeApp, config)

    def test_trace_attributes(self):
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'PUT'})

        # first if there is no txid or swift_source they have defaults
        test_tracer, in_memory_spans = activate_tracing(req.environ)
        app = request_trace.RequestTraceMiddleware(FakeApp(), {})
        with mock.patch('swift.common.middleware.request_trace.init_tracer',
                        return_value=test_tracer):
            req.get_response(app)
        # get the initial/main span
        main_span = in_memory_spans.get_finished_spans()[-2]
        self.assertEqual(main_span.attributes['http.status_code'], 200)
        self.assertFalse(main_span.attributes['swift.source'])
        self.assertEqual(main_span.attributes['http.method'], 'PUT')
        self.assertEqual(main_span.attributes['http.url'], '/v1/a/c')
        self.assertEqual(main_span.attributes['txid'], '-')
        self.assertEqual(main_span.name, '- (PUT)')

        # Add a txid to the request and a swift.source and that'll be pulled
        # out and into the trace
        req.environ['HTTP_X_TRANS_ID'] = 'tx123456789'
        req.environ['swift.source'] = 'X'
        test_tracer, in_memory_spans = activate_tracing(req.environ)
        app = request_trace.RequestTraceMiddleware(FakeApp(), {})
        with mock.patch('swift.common.middleware.request_trace.init_tracer',
                        return_value=test_tracer):
            req.get_response(app)
        # get the initial/main span
        main_span = in_memory_spans.get_finished_spans()[-2]
        self.assertEqual(main_span.attributes['http.status_code'], 200)
        self.assertEqual(main_span.attributes['swift.source'], 'X')
        self.assertEqual(main_span.attributes['http.method'], 'PUT')
        self.assertEqual(main_span.attributes['http.url'], '/v1/a/c')
        self.assertEqual(main_span.attributes['txid'], 'tx123456789')
        self.assertEqual(main_span.name, 'tx123456789 (PUT)')

    def test_trace_after_x(self):
        app = request_trace.RequestTraceMiddleware(FakeApp(),
                                                   {'trace_every_x': 5})

        def do_request():
            req = Request.blank('/v1/a/c',
                                environ={'REQUEST_METHOD': 'PUT'})
            activate_tracing(req.environ)
            # Turn off trace_activated so trace_after_x can turn it on.
            req.environ.pop(TRACE_ACTIVATED_KEY)
            return req.get_response(app)

        # Sanity, counter should be at zero
        self.assertEqual(app.req_count, 0)
        for attempt in range(4):
            # We're tracing every 5'th request so the first 4 should fail
            resp = do_request()
            self.assertNotIn(TRACE_ACTIVATED_KEY, resp.request.environ)
            self.assertEqual(app.req_count, attempt + 1)

        # run it again, it will be the 5th, so it'll be activated
        resp = do_request()
        self.assertIn(TRACE_ACTIVATED_KEY, resp.request.environ)
        self.assertEqual(resp.request.environ[TRACE_ACTIVATED_KEY], 'yes')
        self.assertEqual(app.req_count, 5)

        # and every 5th will be activated
        for i in range(100):
            resp = do_request()
            if app.req_count % 5 == 0:
                self.assertEqual(resp.request.environ[TRACE_ACTIVATED_KEY],
                                 'yes')
            else:
                self.assertNotIn(TRACE_ACTIVATED_KEY, resp.request.environ)


if __name__ == '__main__':
    unittest.main()

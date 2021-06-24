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
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
try:
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    JAEGER_LOADED = True
except ImportError:
    JAEGER_LOADED = False

from swift.common.exceptions import TracerLoadException
import json


def set_jaeger_exporter(config, service_name, logger=None):
    if JAEGER_LOADED:
        try:
            config = json.load(open(config))
        except json.JSONDecodeError as decode_error:
            if logger:
                logger.error('Failed to decode jaeger exporter config: %s',
                             str(decode_error))
            # TODO: to something proper here
            return
        for key, value in list(config.items()):
            if str(value).isnumeric():
                config[key] = int(value)

        trace_provider = TracerProvider(
            resource=Resource.create({SERVICE_NAME: service_name}))
        jaeger_exporter = JaegerExporter(**config)
        span_processor = BatchSpanProcessor(jaeger_exporter)
        trace_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(trace_provider)
    else:
        raise TracerLoadException(
            'OpenTelemetry Jaeger exporter module not installed')

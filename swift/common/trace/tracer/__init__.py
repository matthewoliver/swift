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
"""
Tracer stuff
"""

from opentelemetry import trace

from swift.common.utils import load_pkg_resource

loaded_exporters = []


def init_tracer(module, config, name, logger):
    if module is None:
        logger.debug('Falling back to OpenTelemetry noop tracer')
        return trace.NoOpTracerProvider().get_tracer("noop")

    if name not in loaded_exporters:

        try:
            set_exporter_method = load_pkg_resource(
                'swift.trace_exporter', module)
        except ImportError as err:
            raise Exception(
                'Unable to load tracer module %s: %s' %
                (module, err))

        set_exporter_method(config, name, logger)
        loaded_exporters.append(name)
    return trace.get_tracer(name)

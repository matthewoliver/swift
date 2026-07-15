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

import os

from swift.common.utils.timestamp import NormalTimestamp


DEFAULT_SWIFT_DIR = '/etc/swift'
DEFAULT_RING_MANAGER_STATE_DIR = os.path.join(
    DEFAULT_SWIFT_DIR, 'ring-manager-state')


def normal_timestamp(timestamp=None):
    if timestamp is None:
        return NormalTimestamp.now()
    if isinstance(timestamp, NormalTimestamp):
        return timestamp
    return NormalTimestamp(timestamp)


def normal_timestamp_internal(timestamp=None):
    return normal_timestamp(timestamp).internal


def normal_timestamp_float(timestamp=None):
    return float(normal_timestamp(timestamp))

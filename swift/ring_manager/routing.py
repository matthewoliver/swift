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

import re


READ_ONLY_METHODS = ('GET', 'HEAD', 'OPTIONS')


class Route(object):
    def __init__(self, pattern, methods, handler, read_only_methods=None):
        self.regex = re.compile(pattern)
        self.methods = tuple(methods)
        self.handler = handler
        if read_only_methods is None:
            read_only_methods = set(method for method in self.methods
                                    if method in READ_ONLY_METHODS)
            if 'GET' in self.methods:
                read_only_methods.add('HEAD')
        self.read_only_methods = tuple(read_only_methods)

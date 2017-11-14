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
    cmdline utility to get the current node's stats
"""

import json
import optparse
import os
import sys
import time

from itertools import chain

from swift.cli.recon import seconds2timeunit
from swift.common.utils import readconf


class SwiftStats(object):

    def _listdir(self, path):
        try:
            return os.listdir(path)
        except OSError as e:
            print('ERROR: Unable to access %(path)s: %(error)s' %
                  {'path': path, 'error': e})
            return []

    def _print_line(self, indent, key, *items):
        if '|' in key:
            key, m_type = key.split('|')
        else:
            m_type = None
        if m_type == 'ms' and self.human:
            m_type = None
            items = list(items)
            for i, item in enumerate(items):
                item, unit = seconds2timeunit(item / 1000.0)
                items[i] = '%.2f%s' % (item, unit[0])
        params = [' ' * indent, key]
        params.extend(items)
        if len(items) == 0:
            print('%s%s' % tuple(params))
        else:
            if m_type == 'c':
                table_str = '%s%50s ' + ' '.join(['%20i'] * len(items))
            elif m_type == 'ms':
                table_str = '%s%50s ' + ' '.join(['%20.2f'] * len(items))
            else:
                table_str = '%s%50s ' + ' '.join(['%20s'] * len(items))
            print(table_str % tuple(params))

    def display_report(self, recon_dir, files_iter):
        indent = 2
        now = time.time()
        self._print_line(indent, '-' * 52, '-' * 20, '-' * 20, '-' * 20)
        self._print_line(indent + 2, "METRIC", "1 min", "5 min", "15 min")
        self._print_line(indent, '-' * 52, '-' * 20, '-' * 20, '-' * 20)
        for statfile in files_iter:
            self._print_line(indent, statfile[:0 - len('.stats')])
            try:
                data = json.load(open(os.path.join(recon_dir, statfile)))
                if now - (60 * 15) > data['time']:
                    # don't report stale data.
                    continue
                for key in sorted(data['1'].keys()):
                    if isinstance(data['1'][key], list):
                        continue
                    self._print_line(indent + 2, key, data['1'][key],
                                     data['5'][key], data['15'][key])
            except ValueError:
                continue
            finally:
                print('')

    def parse_recon_dir(self, recon_dir):
        account_stats = []
        container_stats = []
        obj_stats = []
        other_stats = []
        for statfile in self._listdir(recon_dir):
            if not statfile.endswith('.stats'):
                continue
            if statfile.startswith('account'):
                account_stats.append(statfile)
            elif statfile.startswith('container'):
                container_stats.append(statfile)
            elif statfile.startswith('obj'):
                obj_stats.append(statfile)
            else:
                other_stats.append(statfile)
        files_iter = chain(sorted(account_stats),
                           sorted(container_stats),
                           sorted(obj_stats),
                           sorted(other_stats))

        self.display_report(recon_dir, files_iter)


    def main(self):
        """
        Retrieve stats from the current node.
        """
        usage = '''
        usage: %prog [<config> | <path/to/recon_cache>]
        [--human-readable]


        ex: %prog /etc/swift/object-server.conf
        '''
        args = optparse.OptionParser(usage)
        args.add_option('--human-readable', action="store_true",
                        help="Use human readable suffix for timing stats")
        options, arguments = args.parse_args()
        self.human = options.human_readable

        use_config = False
        recon_dir = '/var/cache/swift'
        if arguments:
            if os.path.isdir(arguments[0]) and \
                    not arguments[0].endswith('conf'):
                recon_dir = arguments[0]
            elif arguments[0].endswith('conf'):
                use_config = True
            else:
                print('Error, you must specify either a dir or config')
                sys.exit(1)

        if use_config:
            conf = readconf(arguments[0])
            recon_dir = conf.get('recon_cache_path', recon_dir)

        self.parse_recon_dir(recon_dir)


def main():
    try:
        stats = SwiftStats()
        stats.main()
    except KeyboardInterrupt:
        print('\n')
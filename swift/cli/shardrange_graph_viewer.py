from __future__ import print_function
import argparse
import json
import sys
import time
import re
from collections import OrderedDict
from hashlib import md5

from swift.common.utils import Timestamp, get_logger, ShardRange

try:
    from graphviz import Graph
except ImportError:
    print("graphviz python module not installed.")
    sys.exit(1)


def _from_container_info():
    GROK_START = "Name: "
    GROK_END = "Meta Timestamp: "
    record_started = False
    records = []
    record = []
    shard_ranges = []
    try:
        for line in sys.stdin:
            line = line.strip()
            if line.startswith(GROK_START):
                record_started = True
            elif line.startswith(GROK_END):
                record.append(line)
                records.append(record)
                record = []
                record_started = False
            if record_started:
                record.append(line)
    except Exception as ex:
        print(str(ex))
    finally:
        sys.stdin.flush()
        sys.stdin.close()

    pattern = ('(Name:|lower:|upper:|Object Count:|Bytes Used:|'
               'State:|Created at:|Meta Timestamp:)')

    for record in records:
        pairs = re.split(pattern, "\n".join(record))[1:]
        rec_dict = OrderedDict(
            (pairs[i * 2], pairs[i * 2 + 1]) for i in range(8))
        # some cleanups
        for comma in ('lower:', 'Object Count:', 'Bytes Used:'):
            if rec_dict[comma].strip().endswith(','):
                rec_dict[comma] = rec_dict[comma].strip()[:-1]
        if rec_dict['lower:'].strip() == "''":
            rec_dict['lower:'] = ''
        if rec_dict['upper:'].strip() == "''":
            rec_dict['upper:'] = ''
        shard_ranges.append(
            ShardRange(rec_dict['Name:'].strip(),
                       _info_bracket_data_to_data(
                           rec_dict['Created at:'].strip()),
                       rec_dict['lower:'].strip(),
                       rec_dict['upper:'].strip(),
                       rec_dict['Object Count:'].strip(),
                       rec_dict['Bytes Used:'].strip(),
                       _info_bracket_data_to_data(
                            rec_dict['Meta Timestamp:'].strip()),
                       state=_info_bracket_data_to_data(
                            rec_dict['State:'].strip())))
    return shard_ranges, []


def _info_bracket_data_to_data(ts):
    if ts:
        ts = ts.split()
        if len(ts) == 2 and ts[-1].startswith('('):
            return ts[-1][1:-1]
    return None


def _from_json():
    try:
        shard_ranges = []
        ranges = json.load(sys.stdin)
        for sr in ranges:
            if isinstance(sr['state'], str):
                if sr['state'].isdigit():
                    sr['state'] = int(sr['state'])
                else:
                    sr['state'] = ShardRange.STATES_BY_NAME[sr['state']]
            shard_ranges.append(ShardRange.from_dict(sr))
        return shard_ranges, []
    except ValueError:
        print('Failed to load json data')
        return [], []


def main(args=None):
    parser = argparse.ArgumentParser(
        description='Display ShardRange graphs')
    parser.add_argument('graph_name', nargs='?', help='Name of the graph')
    parser.add_argument('-i', '--info', default=False, action='store_true',
                        help='Data is from container-info')
    parser.add_argument('-f', '--format', default="svg",
                        choices=['svg', 'png', 'pdf'],
                        help="Format of the graph to produce")

    args = parser.parse_args(args)

    if not args.graph_name:
        args.graph_name = "ShardRange Graph - {}".format(time.time())

    if args.info:
        shard_ranges, good_path = _from_container_info()
    else:
        shard_ranges, good_path = _from_json()

    if not shard_ranges:
        print("No shard range data detected")
        sys.exit(1)

    graph = Graph(args.graph_name)

    for sr in shard_ranges:
        lower = sr.lower_str if sr.lower_str != '' else "MIN"
        upper = sr.upper_str if sr.upper_str != '' else "MAX"
        lower_key = md5(lower.encode('utf-8')).hexdigest()
        upper_key = md5(upper.encode('utf-8')).hexdigest()

        graph.node(lower_key, label=lower)
        graph.node(upper_key, label=upper)
        edge_name = '%s %s' % (sr.name, sr.object_count)
        graph.edge(lower_key, upper_key, edge_name)

    graph.render("{}-{}.gv".format("container_info", time.time()),
                 view=True, format=args.format)


if __name__ == '__main__':
    exit(main())
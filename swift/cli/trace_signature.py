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

import sys
import argparse
import time

from swift.common.wsgi import get_trace_hmac


def main(args):
    parser = argparse.ArgumentParser(
        description='generate trace signature')
    parser.add_argument('trace_key', type=str, help=(
        'key to use when generating HMAC signature'))
    parser.add_argument('expires', type=int,
                        help='Set the expiry (in seconds)')
    parser.add_argument('--hash', choices=['sha1', 'sha256', 'sha512'],
                        default='sha256', help='hash digest to use')

    args = parser.parse_args(args)
    if not args.trace_key and not args.expires:
        print("key and expires required")
        sys.exit(1)
    expiry = int(time.time() + args.expires)
    sig = get_trace_hmac(args.trace_key, expiry, args.hash)
    sig = "%s:%s" % (args.hash, sig)
    print('trace_sig=%s&trace_expires=%s' % (sig, expiry))


if __name__ == "__main__":
    main(sys.argv[1:])

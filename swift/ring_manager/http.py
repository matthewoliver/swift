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

import json

from swift.common.swob import HTTPRequestEntityTooLarge, Response


def json_body(data):
    return json.dumps(data, sort_keys=True).encode('ascii')


def json_response(req, data, status=200, headers=None):
    return Response(
        request=req, status=status, headers=headers,
        body=json_body(data), content_type='application/json')


def json_error(req, status_class, message):
    return status_class(
        request=req, body=json_body({'error': message}),
        content_type='application/json')


def json_request_body(req, max_size):
    if req.content_length is not None and req.content_length > max_size:
        raise json_error(
            req, HTTPRequestEntityTooLarge,
            'Request body must be no larger than %d bytes' % max_size)
    try:
        body = req.body_file.read(max_size + 1)
        if len(body) > max_size:
            raise json_error(
                req, HTTPRequestEntityTooLarge,
                'Request body must be no larger than %d bytes' % max_size)
        if not body:
            return {}
        data = json.loads(body.decode('utf-8'))
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ValueError('Request body must be valid JSON')
    if not isinstance(data, dict):
        raise ValueError('Request body must be a JSON object')
    return data


def collection_response(req, objects):
    return json_response(req, {
        'meta': {
            'limit': len(objects),
            'next': None,
            'offset': 0,
            'previous': None,
            'total_count': len(objects),
        },
        'objects': objects,
    })

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

import errno
import json
import os

from swift.common.swob import HTTPNotFound, HTTPRequestEntityTooLarge, \
    Response, multi_range_iterator


DEFAULT_FILE_CHUNK_SIZE = 64 * 1024


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


def remove_file(path):
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


class RingManagerFileIterable(object):
    def __init__(self, path, chunk_size=DEFAULT_FILE_CHUNK_SIZE,
                 unlink_on_close=False):
        self.path = path
        self.chunk_size = chunk_size
        self.unlink_on_close = unlink_on_close
        self._fp = open(path, 'rb')
        self._suppress_file_closing = False

    def __iter__(self):
        try:
            while True:
                chunk = self._fp.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            if not self._suppress_file_closing:
                self.close()

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None
        if self.unlink_on_close and self.path:
            remove_file(self.path)
            self.path = None

    def app_iter_range(self, start, stop):
        self._fp.seek(start)
        length = stop - start if stop is not None else None
        try:
            while length is None or length > 0:
                read_size = self.chunk_size
                if length is not None:
                    read_size = min(read_size, length)
                chunk = self._fp.read(read_size)
                if not chunk:
                    break
                if length is not None:
                    length -= len(chunk)
                yield chunk
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        if not ranges:
            yield b''
            return
        if not isinstance(content_type, bytes):
            content_type = content_type.encode('utf8')
        if not isinstance(boundary, bytes):
            boundary = boundary.encode('ascii')
        try:
            self._suppress_file_closing = True
            for chunk in multi_range_iterator(
                    ranges, content_type, boundary, size,
                    self.app_iter_range):
                yield chunk
        finally:
            self._suppress_file_closing = False
            self.close()


def artifact_file_response(req, file_info,
                           file_iterable_cls=RingManagerFileIterable):
    headers = {}
    etag = file_info.get('md5')
    if etag:
        headers['Etag'] = etag
    if file_info.get('sha256'):
        headers['X-Checksum-Sha256'] = file_info['sha256']
    try:
        stat_result = os.stat(file_info['path'])
    except OSError as err:
        if err.errno in (errno.ENOENT, errno.EACCES, errno.EPERM):
            return HTTPNotFound(request=req)
        raise
    if etag and req.if_none_match and etag in req.if_none_match:
        return Response(
            request=req, status=304, body=b'', headers=headers,
            content_type=file_info.get(
                'content_type', 'application/octet-stream'))
    content_length = file_info.get('bytes')
    if content_length is None:
        content_length = stat_result.st_size
    headers['Content-Length'] = str(content_length)
    if req.method == 'HEAD':
        return Response(
            request=req, body=None, headers=headers,
            conditional_response=True, conditional_etag=etag,
            content_type=file_info.get(
                'content_type', 'application/octet-stream'))
    try:
        app_iter = file_iterable_cls(file_info['path'])
    except IOError as err:
        if err.errno in (errno.ENOENT, errno.EACCES, errno.EPERM):
            return HTTPNotFound(request=req)
        raise
    return Response(
        request=req, app_iter=app_iter, headers=headers,
        conditional_response=True, conditional_etag=etag,
        content_type=file_info.get(
            'content_type', 'application/octet-stream'))


def builder_file_response(req, record, remove_file_callback=remove_file,
                          file_iterable_cls=RingManagerFileIterable):
    headers = {
        'Etag': record['md5'],
        'X-Checksum-Sha256': record['sha256'],
        'X-Ring-Builder-Version': str(record['builder_version']),
        'Content-Length': str(record['bytes']),
    }
    if req.method == 'HEAD':
        app_iter = None
        if record.get('unlink_on_close'):
            remove_file_callback(record.get('path'))
    else:
        try:
            app_iter = file_iterable_cls(
                record['path'],
                unlink_on_close=record.get('unlink_on_close', False))
        except IOError as err:
            if record.get('unlink_on_close'):
                remove_file_callback(record.get('path'))
            if err.errno in (errno.ENOENT, errno.EACCES, errno.EPERM):
                return HTTPNotFound(request=req)
            raise
    return Response(
        request=req, app_iter=app_iter, headers=headers,
        conditional_response=True, conditional_etag=record['md5'],
        content_type='application/octet-stream')

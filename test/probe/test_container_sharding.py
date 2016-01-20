from unittest import main
from uuid import uuid4
from datetime import timedelta
import time

from swiftclient import client
from swiftclient.client import Connection as SwiftClientConnection

from swift.common.utils import PivotTree, pivot_to_pivot_container
from swift.common.internal_client import InternalClient
from swift.common.wsgi import ConfigString
from swift.container.sharder import ic_conf_body
from test.probe.common import ReplProbeTest


class ProbeTestContainer(object):
    def __init__(self, swift_client):
        """Client wrapper focused on container manipulation

        :param account: the account for this container
        :param swift_client: a swiftclient setup for requests to an account
        """
        self.container = 'container-%s' % uuid4()
        self.swift = swift_client
        self.swift.put_container(self.container)

    @property
    def name(self):
        return self.container

    def __del__(self):
        self.delete()

    def _objects(self, lower, upper=None):
        """Generate names for objects in the format object-##### etc

        If only one argument is given, it is used as the max and 0 as the min

        :param lower: the lower bounds for object names
        :param upper: the upper bounds for object names
        """
        if upper is None:
            upper = lower
            lower = 0
        for i in xrange(lower, upper):
            yield 'object-%s' % format(i, '04')

    def put_objects(self, lower, upper=None, data_generator=lambda x: x):
        """Put objects into the container

         for lower and upper see _objects

        :param data generator: a function that returns data to put in an object
                               the function should accept a single parameter
                               (the object name) and return a string
        """
        for object_name in self._objects(lower, upper):
            data = str(data_generator(object_name))
            self.swift.put_object(self.container, object_name, data)

    def head_objects(self, lower, upper=None):
        for object_name in self._objects(lower, upper):
            yield self.swift.head_object(self.container, object_name)

    def delete_objects(self, lower=None, upper=None):
        """Remove objects from the container

        if both lower and upper are given, they will be used for creating the
        list of containers to delete instead of getting the container and
        deleting all returned objects
        """
        if lower is not None:
            objects = self._objects(lower, upper)
        else:
            resp = self.swift.get_container(self.container)
            objects = [x['name'] for x in resp[1]]

        for object_name in objects:
            self.swift.delete_object(self.container, object_name)

    def delete(self):
        """Delete the container and its contents"""
        self.delete_objects()
        self.swift.delete_container(self.container)

    def get_pivots(self):
        """Get the pivot nodes

        :return: dictionary of {pivot: pivot data}
        """
        # swiftclient only supports certain query string arguments
        # To get around this we use HTTPConnection directly
        # This is an awful hack and we should look for a better way
        query = '?nodes=pivot&format=json'
        info, connection = self.swift.http_connection()
        if not self.swift.token:
            self.swift.get_auth()
        path = info.path + '/' + self.container + query
        header = {'X-Auth-Token': self.swift.token}
        resp = connection.request('GET', path, '', header)
        return dict((x['name'], x) for x in resp.json())


class TestContainerShardingOff(ReplProbeTest):
    # This needs to match the sharding_size in the conf file
    shard_size = 100

    def setUp(self):
        super(TestContainerShardingOff, self).setUp()
        self.swift = SwiftClientConnection(preauthurl=self.url,
                                           preauthtoken=self.token)

    def _wait_for(self, check, error_message,
                  timeout=timedelta(minutes=3),
                  interval=timedelta(seconds=10)):
        """Wait for a condition to be true with a timeout

        :param check: a function that takes nothing and returns a bool
        :param error_message: the exception message if the timeout occurs
        :param timeout: a timedelta for the amount of time to wait
        :param interval: a timedelta for how often to poll the check
        """
        self.get_to_final_state()

        try_until = time.time() + timeout.total_seconds()
        while True:
            time.sleep(interval.total_seconds())
            if check():
                break
            if time.time() > try_until:
                message = ('Timeout: %s; ' % timeout) + error_message
                print message
                raise Exception(message)

    def test_shard_once(self):
        self._check_sharding_with_objects(101)

    def test_shard_multiple_times(self):
        self._check_sharding_with_objects(500)

    def _check_sharding_with_objects(self, obj_count):
        # Create container, fill with objects
        container = ProbeTestContainer(self.swift)
        container.put_objects(obj_count)
        self.shard_if_needed(self.account, container.name)
        container.delete()

    def test_basic_CRUD(self):
        container = ProbeTestContainer(self.swift)

        # Create all objects and force a shard to occur
        container.put_objects(200)
        self.shard_if_needed(self.account, container.name)

        # Delete objects
        container.delete_objects(0, 50)

        def object_count(target_number):
            resp = self.swift.head_container(container.name)
            count = int(resp.get('x-container-object-count'))
            return count == target_number

        self._wait_for(lambda: object_count(150),
                       'objects not removed after delete')

        # Get all the etags and then update the contents of the objects
        etags = [response.get('etag')
                 for response
                 in container.head_objects(50, 100)]
        container.put_objects(50, 100, lambda name: name + '+1000')

        def etags_differ():
            new_tags = [response.get('etag')
                        for response
                        in container.head_objects(50, 100)]
            return len(set(new_tags).intersection(set(etags))) == 0

        self._wait_for(etags_differ, 'objects did not successfully update')

        # add 300 objects and shard
        container.put_objects(200, 500)
        self.shard_if_needed(self.account, container.name)

        # Delete objects in the container and the container itself
        container.delete()
        with self.assertRaises(Exception):
            self.get_container(container.name)


class TestContainerShardingOn(TestContainerShardingOff):
    sharding_enabled = True

    def test_tree_structure(self):
        container = ProbeTestContainer(self.swift)

        container.put_objects(500)
        self.shard_if_needed(self.account, container.name)

        pivots = container.get_pivots()
        tree = PivotTree()
        for pivot in pivots.keys():
            tree.add(pivot)

        # check that a left child is always <= parent and right is always >
        def consistent_ordering(node):
            if node is None:
                return
            if node.left:
                self.assertTrue(node.key >= node.left.key)
                consistent_ordering(node.left)
            if node.right:
                self.assertTrue(node.key < node.right.key)
                consistent_ordering(node.right)

        consistent_ordering(tree._root)

        # Check that each level of the pivot tree has no more than the
        # expected number of pivots (can expose consistency issues)
        levels = {}
        for pivot in pivots.values():
            index = int(pivot['bytes'])
            levels[index] = levels.get(index, 0) + 1

        for level, count in levels.iteritems():
            self.assertTrue(count <= (2 ** level))

    def test_check_hidden_containers_deleted(self):
        container = ProbeTestContainer(self.swift)

        container.put_objects(101)
        self.shard_if_needed(self.account, container.name)
        pivots = container.get_pivots().keys()
        container.delete()

        ic = InternalClient(ConfigString(ic_conf_body), 'Probe Test', 1)

        for pivot in pivots:
            for weight in (-1, 1):
                shard_account, shard_container = pivot_to_pivot_container(
                    self.account, container.name, pivot, weight)
                path = ic.make_path(shard_account, shard_container)
                # the container is deleted so we should not get 200 series
                ic.make_request('GET', path, {}, (3,4,5))

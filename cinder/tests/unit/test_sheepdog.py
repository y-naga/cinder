
# Copyright (c) 2013 Zelin.io
# Copyright (C) 2015 Nippon Telegraph and Telephone Corporation.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import errno

import mock
from oslo_concurrency import processutils
from oslo_utils import fileutils
from oslo_utils import importutils
from oslo_utils import units

from cinder.backup import driver as backup_driver
from cinder import context
from cinder import db
from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import test
from cinder.tests.unit import fake_backup
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder import utils
from cinder.volume import configuration as conf
from cinder.volume.drivers import sheepdog
from cinder.volume import utils as volutils

SHEEP_ADDR = '127.0.0.1'
SHEEP_PORT = 7000


class SheepdogDriverTestDataGenerator(object):
    def __init__(self):
        self.TEST_VOLUME = self._make_fake_volume(self.TEST_VOL_DATA)
        self.TEST_CLONED_VOLUME = self._make_fake_volume(
            self.TEST_CLONED_VOL_DATA)
        self.TEST_SNAPSHOT = self._make_fake_snapshot(
            self.TEST_SNAPSHOT_DATA, self.TEST_VOLUME)
        self.TEST_BACKUP_VOLUME = self._make_fake_backup_volume(
            self.TEST_BACKUP_VOL_DATA)

    def sheepdog_cmd_error(self, cmd, exit_code, stdout, stderr):
        return (('(Command: %(cmd)s) '
                 '(Return Code: %(exit_code)s) '
                 '(Stdout: %(stdout)s) '
                 '(Stderr: %(stderr)s)') %
                {'cmd': cmd,
                 'exit_code': exit_code,
                 'stdout': stdout.replace('\n', '\\n'),
                 'stderr': stderr.replace('\n', '\\n')})

    def _make_fake_volume(self, volume_data):
        return fake_volume.fake_volume_obj(context.get_admin_context(),
                                           **volume_data)

    def _make_fake_snapshot(self, snapshot_data, src_volume):
        snapshot_obj = fake_snapshot.fake_snapshot_obj(
            context.get_admin_context(), **snapshot_data)
        snapshot_obj.volume = src_volume
        return snapshot_obj

    def _make_fake_backup_volume(self, backup_data):
        return fake_backup.fake_backup_obj(context.get_admin_context(),
                                           **backup_data)

    def cmd_dog_vdi_create(self, name, size):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'create', name,
                '%sG' % size, '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    def cmd_dog_vdi_delete(self, name):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'delete', name,
                '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    def cmd_dog_vdi_create_snapshot(self, vdiname, snapname):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'snapshot', '-s',
                snapname, '-a', SHEEP_ADDR, '-p', SHEEP_PORT, vdiname)

    def cmd_dog_vdi_delete_snapshot(self, vdiname, snapname):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'delete', '-s',
                snapname, '-a', SHEEP_ADDR, '-p', SHEEP_PORT, vdiname)

    def cmd_qemuimg_vdi_clone(self, src_vdiname, src_snapname, dst_vdiname,
                              size):
        return ('env', 'LC_ALL=C', 'LANG=C', 'qemu-img', 'create', '-b',
                'sheepdog:%(addr)s:%(port)s:%(src_vdiname)s:%(src_snapname)s' %
                {'addr': SHEEP_ADDR, 'port': SHEEP_PORT,
                 'src_vdiname': src_vdiname, 'src_snapname': src_snapname},
                'sheepdog:%(addr)s:%(port)s:%(dst_vdiname)s' %
                {'addr': SHEEP_ADDR, 'port': SHEEP_PORT,
                 'dst_vdiname': dst_vdiname}, '%sG' % size)

    def cmd_dog_vdi_resize(self, name, size):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'resize', name,
                size, '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    def cmd_dog_vdi_list(self, vdiname):
        return ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'vdi', 'list', vdiname,
                '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    CMD_DOG_CLUSTER_INFO = ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'cluster',
                            'info', '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    CMD_DOG_NODE_INFO = ('env', 'LC_ALL=C', 'LANG=C', 'dog', 'node', 'info',
                         '-r', '-a', SHEEP_ADDR, '-p', SHEEP_PORT)

    TEST_VOL_DATA = {
        'size': 1,
        'id': '00000000-0000-0000-0000-000000000001',
        'provider_auth': None,
        'host': 'host@backendsec#unit_test_pool',
        'project_id': 'project',
        'provider_location': 'location',
        'display_name': 'vol1',
        'display_description': 'unit test volume',
        'volume_type_id': None,
        'consistencygroup_id': None,
    }

    TEST_CLONED_VOL_DATA = {
        'size': 2,
        'id': '00000000-0000-0000-0000-000000000003',
        'provider_auth': None,
        'host': 'host@backendsec#unit_test_pool',
        'project_id': 'project',
        'provider_location': 'location',
        'display_name': 'vol3',
        'display_description': 'unit test cloned volume',
        'volume_type_id': None,
        'consistencygroup_id': None,
    }

    TEST_SNAPSHOT_DATA = {
        'id': '00000000-0000-0000-0000-000000000002',
    }

    TEST_BACKUP_VOL_DATA = {
        'volume_id': '00000000-0000-0000-0000-000000000001',
    }

    TEST_IMAGE_META = {
        'id': '00000000-0000-0000-0000-000000000005',
        'size': 2,
        'disk_format': 'raw',
    }

    TEST_VOLUME_STATS = {
        'volume_backend_name': 'sheepdog',
        'vendor_name': 'Open Source',
        'driver_version': '1.0.0',
        'storage_protocol': 'sheepdog',
        'total_capacity_gb': float(107287605248) / units.Gi,
        'free_capacity_gb': float(107287605248 - 3623897354) / units.Gi,
        'reserved_percentage': 0,
        'QoS_support': False,
    }

    TEST_VOLUME_STATS_ERROR = {
        'volume_backend_name': 'sheepdog',
        'vendor_name': 'Open Source',
        'driver_version': '1.0.0',
        'storage_protocol': 'sheepdog',
        'total_capacity_gb': 'unknown',
        'free_capacity_gb': 'unknown',
        'reserved_percentage': 0,
        'QoS_support': False,
    }

    TEST_EXISTING_REF = {
        'source-name': 'test-vdi',
        'source-id': '',
    }

    COLLIE_CLUSTER_INFO_0_5 = """\
Cluster status: running

Cluster created at Tue Jun 25 19:51:41 2013

Epoch Time           Version
2013-06-25 19:51:41      1 [127.0.0.1:7000, 127.0.0.1:7001, 127.0.0.1:7002]
"""

    COLLIE_CLUSTER_INFO_0_6 = """\
Cluster status: running, auto-recovery enabled

Cluster created at Tue Jun 25 19:51:41 2013

Epoch Time           Version
2013-06-25 19:51:41      1 [127.0.0.1:7000, 127.0.0.1:7001, 127.0.0.1:7002]
"""

    DOG_CLUSTER_RUNNING = """\
Cluster status: running, auto-recovery enabled

Cluster created at Thu Jun 18 17:24:56 2015

Epoch Time           Version [Host:Port:V-Nodes,,,]
2015-06-18 17:24:56      1 [127.0.0.1:7000:128, 127.0.0.1:7001:128,\
 127.0.0.1:7002:128]
"""

    DOG_CLUSTER_INFO_TO_BE_FORMATTED = """\
Cluster status: Waiting for cluster to be formatted
"""

    DOG_CLUSTER_INFO_WAITING_OTHER_NODES = """\
Cluster status: Waiting for other nodes to join cluster

Cluster created at Thu Jun 18 17:24:56 2015

Epoch Time           Version [Host:Port:V-Nodes,,,]
2015-06-18 17:24:56      1 [127.0.0.1:7000:128, 127.0.0.1:7001:128]
"""

    DOG_CLUSTER_INFO_SHUTTING_DOWN = """\
Cluster status: System is shutting down
"""

    DOG_VDI_CREATE_VDI_ALREADY_EXISTS = """\
Failed to create VDI %(vdiname)s: VDI exists already
"""

    DOG_VDI_SNAPSHOT_VDI_NOT_FOUND = """\
Failed to create snapshot for volume-00000000-0000-0000-0000-000000000001: \
No VDI found
"""

    DOG_VDI_SNAPSHOT_ALREADY_EXISTED = """\
Failed to create snapshot for volume-00000000-0000-0000-0000-000000000001, \
maybe snapshot id (0) or tag (snapshot-00000000-0000-0000-0000-000000000002) \
is existed
"""

    DOG_VDI_SNAPSHOT_TAG_NOT_FOUND = """\
Failed to open VDI volume-00000000-0000-0000-0000-000000000001 \
(snapshot id: 0 snapshot tag: snapshot-00000000-0000-0000-0000-000000000002): \
Failed to find requested tag
"""

    DOG_VDI_SNAPSHOT_VOLUME_NOT_FOUND = """\
Failed to open VDI volume-00000000-0000-0000-0000-000000000001 \
(snapshot id: 0 snapshot tag: snapshot-00000000-0000-0000-0000-000000000002): \
No VDI found
"""

    DOG_VDI_RESIZE_SIZE_SHRINK = """\
Shrinking VDIs is not implemented
"""

    DOG_VDI_RESIZE_TOO_LARGE = """\
New VDI size is too large. This volume's max size is 4398046511104
"""

    DOG_NODE_INFO = """
0 107287605248 3623897354 103663707894 3%
Total 107287605248 3623897354 103663707894 3% 54760833024
"""

    DOG_NODE_INFO_ERROR_GET_NO_NODE_INFO = """\
Cannot get information from any nodes
"""

    DOG_VDI_LIST = """\
= test-vdi 0 10737418240 0 0 1442379755 7be7f9 3
"""

    DOG_VDI_LIST_WITH_SNAPSHOT = """\
s test-vdi 1 10737418240 0 0 1442379755 7be7f9 3
= test-vdi 0 16106127360 0 0 1442381817 7be7fb 3
"""

    DOG_VDI_LIST_CLONE = """\
c test-clone 0 10737418240 0 0 1442381753 ce8575 3
"""

    DOG_VDI_LIST_SIZE_10G_MORE_1 = """\
= test-vdi 0 10737418241 0 0 1442379755 7be7f9 3
"""

    DOG_VDI_LIST_SIZE_11G_LESS_1 = """\
= test-vdi 0 11811160063 0 0 1442379755 7be7f9 3
"""

    DOG_COMMAND_ERROR_VDI_NOT_EXISTS = """\
Failed to open VDI %(vdiname)s (snapshot id: 0 snapshot tag: ): No VDI found
"""

    DOG_COMMAND_ERROR_FAIL_TO_CONNECT = """\
failed to connect to 127.0.0.1:7000: Connection refused
failed to connect to 127.0.0.1:7000: Connection refused
Failed to get node list
"""

    QEMU_IMG_VDI_ALREADY_EXISTS = """\
qemu-img: sheepdog:volume-00000000-0000-0000-0000-000000000001: \
VDI exists already,
"""

    QEMU_IMG_VDI_NOT_FOUND = """\
qemu-img: sheepdog:volume-00000000-0000-0000-0000-000000000003: \
cannot get vdi info, No vdi found, \
volume-00000000-0000-0000-0000-000000000001 \
snapshot-00000000-0000-0000-0000-000000000002
"""

    QEMU_IMG_SNAPSHOT_NOT_FOUND = """\
qemu-img: sheepdog:volume-00000000-0000-0000-0000-000000000003: \
cannot get vdi info, Failed to find the requested tag, \
volume-00000000-0000-0000-0000-000000000001 \
snapshot-00000000-0000-0000-0000-000000000002
"""

    QEMU_IMG_SIZE_TOO_LARGE = """\
qemu-img: sheepdog:volume-00000000-0000-0000-0000-000000000001: \
An image is too large. The maximum image size is 4096GB
"""

    QEMU_IMG_FAILED_TO_CONNECT = """\
qemu-img: sheepdog::volume-00000000-0000-0000-0000-000000000001: \
Failed to connect socket: Connection refused
"""

    QEMU_IMG_FILE_NOT_FOUND = """\
qemu-img: Could not open '/tmp/volume-00000001': \
Could not open '/tmp/volume-00000001': \
No such file or directory
"""

    QEMU_IMG_PERMISSION_DENIED = """\
qemu-img: Could not open '/tmp/volume-00000001': \
Could not open '/tmp/volume-00000001': \
Permission denied
"""

    QEMU_IMG_ERROR_INVALID_FORMAT = """\
qemu-img: Unknown file format 'dummy'
"""

    QEMU_IMG_INVALID_DRIVER = """\
qemu-img: Could not open '/tmp/volume-00000001': \
Unknown driver 'dummy'
"""

    IS_CLONEABLE_SNAPSHOT = """\
s a720b3c0-d1f0-11e1-9b23-0800200c9a66 1 1 1 1 1 1 1 glance-image 22
"""

    IS_CLONEABLE_VOLUME = """\
  a720b3c0-d1f0-11e1-9b23-0800200c9a66 1 1 1 1 1 1 1 22
"""


class FakeImageService(object):
    def download(self, context, image_id, path):
        pass


class SheepdogIOWrapperTestCase(test.TestCase):
    def setUp(self):
        super(SheepdogIOWrapperTestCase, self).setUp()
        self.volume = {'name': 'volume-2f9b2ff5-987b-4412-a91c-23caaf0d5aff'}
        self.snapshot_name = 'snapshot-bf452d80-068a-43d7-ba9f-196cf47bd0be'

        self.vdi_wrapper = sheepdog.SheepdogIOWrapper(
            self.volume)
        self.snapshot_wrapper = sheepdog.SheepdogIOWrapper(
            self.volume, self.snapshot_name)

        self.execute = mock.MagicMock()
        self.mock_object(processutils, 'execute', self.execute)

    def test_init(self):
        self.assertEqual(self.volume['name'], self.vdi_wrapper._vdiname)
        self.assertIsNone(self.vdi_wrapper._snapshot_name)
        self.assertEqual(0, self.vdi_wrapper._offset)

        self.assertEqual(self.snapshot_name,
                         self.snapshot_wrapper._snapshot_name)

    def test_execute(self):
        cmd = ('cmd1', 'arg1')
        data = 'data1'

        self.vdi_wrapper._execute(cmd, data)

        self.execute.assert_called_once_with(*cmd, process_input=data)

    def test_execute_error(self):
        cmd = ('cmd1', 'arg1')
        data = 'data1'
        self.mock_object(processutils, 'execute',
                         mock.MagicMock(side_effect=OSError))

        args = (cmd, data)
        self.assertRaises(exception.VolumeDriverException,
                          self.vdi_wrapper._execute,
                          *args)

    def test_read_vdi(self):
        self.vdi_wrapper.read()
        self.execute.assert_called_once_with(
            'dog', 'vdi', 'read', self.volume['name'], 0, process_input=None)

    def test_read_vdi_invalid(self):
        self.vdi_wrapper._valid = False
        self.assertRaises(exception.VolumeDriverException,
                          self.vdi_wrapper.read)

    def test_write_vdi(self):
        data = 'data1'

        self.vdi_wrapper.write(data)

        self.execute.assert_called_once_with(
            'dog', 'vdi', 'write',
            self.volume['name'], 0, len(data),
            process_input=data)
        self.assertEqual(len(data), self.vdi_wrapper.tell())

    def test_write_vdi_invalid(self):
        self.vdi_wrapper._valid = False
        self.assertRaises(exception.VolumeDriverException,
                          self.vdi_wrapper.write, 'dummy_data')

    def test_read_snapshot(self):
        self.snapshot_wrapper.read()
        self.execute.assert_called_once_with(
            'dog', 'vdi', 'read', '-s', self.snapshot_name,
            self.volume['name'], 0,
            process_input=None)

    def test_seek(self):
        self.vdi_wrapper.seek(12345)
        self.assertEqual(12345, self.vdi_wrapper.tell())

        self.vdi_wrapper.seek(-2345, whence=1)
        self.assertEqual(10000, self.vdi_wrapper.tell())

        # This results in negative offset.
        self.assertRaises(IOError, self.vdi_wrapper.seek, -20000, whence=1)

    def test_seek_invalid(self):
        seek_num = 12345
        self.vdi_wrapper._valid = False
        self.assertRaises(exception.VolumeDriverException,
                          self.vdi_wrapper.seek, seek_num)

    def test_flush(self):
        # flush does nothing.
        self.vdi_wrapper.flush()
        self.assertFalse(self.execute.called)

    def test_fileno(self):
        self.assertRaises(IOError, self.vdi_wrapper.fileno)


class SheepdogClientTestCase(test.TestCase):
    def setUp(self):
        super(SheepdogClientTestCase, self).setUp()
        self._cfg = conf.Configuration(None)
        self._cfg.sheepdog_store_address = SHEEP_ADDR
        self._cfg.sheepdog_store_port = SHEEP_PORT
        self.driver = sheepdog.SheepdogDriver(configuration=self._cfg)
        db_driver = self.driver.configuration.db_driver
        self.db = importutils.import_module(db_driver)
        self.driver.db = self.db
        self.driver.do_setup(None)
        self.test_data = SheepdogDriverTestDataGenerator()
        self.client = self.driver.client
        self._vdiname = self.test_data.TEST_VOLUME.name
        self._vdisize = self.test_data.TEST_VOLUME.size
        self._src_vdiname = self.test_data.TEST_SNAPSHOT.volume_name
        self._snapname = self.test_data.TEST_SNAPSHOT.name
        self._dst_vdiname = self.test_data.TEST_CLONED_VOLUME.name
        self._dst_vdisize = self.test_data.TEST_CLONED_VOLUME.size

    @mock.patch.object(utils, 'execute')
    def test_run_dog_success(self, fake_execute):
        args = ('cluster', 'info')
        expected_cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        fake_execute.return_value = ('', '')
        self.client._run_dog(*args)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_dog_command_not_found(self, fake_logger, fake_execute):
        args = ('cluster', 'info')
        expected_msg = 'No such file or directory'
        expected_errno = errno.ENOENT
        fake_execute.side_effect = OSError(expected_errno, expected_msg)
        self.assertRaises(OSError, self.client._run_dog, *args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_dog_operation_not_permitted(self, fake_logger, fake_execute):
        args = ('cluster', 'info')
        expected_msg = 'Operation not permitted'
        expected_errno = errno.EPERM
        fake_execute.side_effect = OSError(expected_errno, expected_msg)
        self.assertRaises(OSError, self.client._run_dog, *args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_dog_unknown_error(self, fake_logger, fake_execute):
        args = ('cluster', 'info')
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        exit_code = 1
        stdout = 'stdout dummy'
        stderr = 'stderr dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        fake_execute.side_effect = processutils.ProcessExecutionError(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client._run_dog, *args)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(utils, 'execute')
    def test_run_qemu_img_success(self, fake_execute):
        # multiple part of args match the prefix and
        # volume name is matched the prefix unfortunately
        expected_cmd = ('env', 'LC_ALL=C', 'LANG=C',
                        'qemu-img', 'create', '-b',
                        'sheepdog:%(addr)s:%(port)s:sheepdog:snap' %
                        {'addr': SHEEP_ADDR, 'port': SHEEP_PORT},
                        'sheepdog:%(addr)s:%(port)s:clone' %
                        {'addr': SHEEP_ADDR, 'port': SHEEP_PORT}, '10G')
        fake_execute.return_value = ('', '')
        self.client._run_qemu_img('create', '-b', 'sheepdog:sheepdog:snap',
                                  'sheepdog:clone', '10G')
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_qemu_img_command_not_found(self, fake_logger, fake_execute):
        args = ('create', 'dummy')
        expected_msg = 'No such file or directory'
        expected_errno = errno.ENOENT
        fake_execute.side_effect = OSError(expected_errno, expected_msg)
        self.assertRaises(OSError, self.client._run_qemu_img, *args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_qemu_img_unknown_os_error(self, fake_logger, fake_execute):
        args = ('create', 'dummy')
        expected_msg = 'unknown'
        expected_errno = errno.EPERM
        fake_execute.side_effect = OSError(expected_errno, expected_msg)
        self.assertRaises(OSError, self.client._run_qemu_img, *args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(utils, 'execute')
    @mock.patch.object(sheepdog, 'LOG')
    def test_run_qemu_img_execution_error(self, fake_logger, fake_execute):
        args = ('create', 'dummy')
        cmd = ('qemu-img', 'create', 'dummy')
        exit_code = 1
        stdout = 'stdout dummy'
        stderr = 'stderr dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        fake_execute.side_effect = processutils.ProcessExecutionError(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client._run_qemu_img, *args)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_success(self, fake_logger, fake_execute):
        stdout = self.test_data.DOG_CLUSTER_RUNNING
        stderr = ''
        expected_cmd = ('cluster', 'info')
        fake_execute.return_value = (stdout, stderr)
        self.client.check_cluster_status()
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertTrue(fake_logger.debug.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_check_cluster_status_v0_5(self, fake_execute):
        stdout = self.test_data.COLLIE_CLUSTER_INFO_0_5
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        self.client.check_cluster_status()

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_check_cluster_status_v0_6(self, fake_execute):
        stdout = self.test_data.COLLIE_CLUSTER_INFO_0_6
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        self.client.check_cluster_status()

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_not_formatted(self, fake_logger,
                                                fake_execute):
        stdout = self.test_data.DOG_CLUSTER_INFO_TO_BE_FORMATTED
        stderr = ''
        expected_reason = _('Cluster is not formatted. '
                            'You should probably perform '
                            '"dog cluster format".')
        fake_execute.return_value = (stdout, stderr)
        ex = self.assertRaises(exception.SheepdogError,
                               self.client.check_cluster_status)
        self.assertEqual(expected_reason, ex.kwargs['reason'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_waiting_to_join_cluster(self, fake_logger,
                                                          fake_execute):
        stdout = self.test_data.DOG_CLUSTER_INFO_WAITING_OTHER_NODES
        stderr = ''
        expected_reason = _('Waiting for all nodes to join cluster. '
                            'Ensure all sheep daemons are running.')
        fake_execute.return_value = (stdout, stderr)
        ex = self.assertRaises(exception.SheepdogError,
                               self.client.check_cluster_status)
        self.assertEqual(expected_reason, ex.kwargs['reason'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_shutting_down(self, fake_logger,
                                                fake_execute):
        stdout = self.test_data.DOG_CLUSTER_INFO_SHUTTING_DOWN
        stderr = ''
        expected_reason = _('Invalid sheepdog cluster status.')
        fake_execute.return_value = (stdout, stderr)
        ex = self.assertRaises(exception.SheepdogError,
                               self.client.check_cluster_status)
        self.assertEqual(expected_reason, ex.kwargs['reason'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_fail_to_connect(self, fake_logger,
                                                  fake_execute):
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.check_cluster_status)
        self.assertEqual(expected_msg, ex.msg)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_check_cluster_status_unknown_error(self, fake_logger,
                                                fake_execute):
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'stdout_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.check_cluster_status)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_create_success(self, fake_execute):
        expected_cmd = ('vdi', 'create', self._vdiname, '%sG' % self._vdisize)
        fake_execute.return_value = ('', '')
        self.client.create(self._vdiname, self._vdisize)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_fail_to_connect(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_create(self._vdiname, self._vdisize)
        exit_code = 2
        stdout = ''
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.create,
                               self._vdiname, self._vdisize)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_vdi_already_exists(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_create(self._vdiname, self._vdisize)
        exit_code = 1
        stdout = ''
        stderr = (self.test_data.DOG_VDI_CREATE_VDI_ALREADY_EXISTS %
                  {'vdiname': self._vdiname})
        expected_msg = self.test_data.sheepdog_cmd_error(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.create,
                               self._vdiname, self._vdisize)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_unknown_error(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_create(self._vdiname, self._vdisize)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(
            cmd=cmd, exit_code=exit_code, stdout=stdout, stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.create,
                               self._vdiname, self._vdisize)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_delete_success(self, fake_execute):
        expected_cmd = ('vdi', 'delete', self._vdiname)
        fake_execute.return_value = ('', '')
        self.client.delete(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_vdi_not_found(self, fake_logger, fake_execute):
        stdout = ''
        stderr = (self.test_data.DOG_COMMAND_ERROR_VDI_NOT_EXISTS %
                  {'vdiname': self._vdiname})
        fake_execute.return_value = (stdout, stderr)
        self.client.delete(self._vdiname)
        self.assertTrue(fake_logger.warning.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_delete_fail_to_connect_bugcase(self, fake_execute):
        # NOTE(tishizaki): Sheepdog's bug case.
        # details are written to Sheepdog driver code.
        stdout = ''
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_reason = (_('Failed to connect to sheep daemon. '
                           'addr: %(addr)s, port: %(port)s'),
                           {'addr': SHEEP_ADDR, 'port': SHEEP_PORT})
        fake_execute.return_value = (stdout, stderr)
        ex = self.assertRaises(exception.SheepdogError,
                               self.client.delete, self._vdiname)
        self.assertEqual(expected_reason, ex.kwargs['reason'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_fail_to_connect(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_delete(self._vdiname)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.delete, self._vdiname)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_unknown_error(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_delete(self._vdiname)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.delete, self._vdiname)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_create_snapshot_success(self, fake_execute):
        args = (self._src_vdiname, self._snapname)
        expected_cmd = ('vdi', 'snapshot', '-s', self._snapname,
                        self._src_vdiname)
        fake_execute.return_value = ('', '')
        self.client.create_snapshot(*args)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_snapshot_fail_to_connect(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_create_snapshot(*args)
        exit_code = 2
        stdout = ''
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.create_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_snapshot_vdi_not_found(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_create_snapshot(*args)
        exit_code = 1
        stdout = ''
        stderr = self.test_data.DOG_VDI_SNAPSHOT_VDI_NOT_FOUND
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.create_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_snapshot_snap_name_already_used(self, fake_logger,
                                                    fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_create_snapshot(*args)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_VDI_SNAPSHOT_ALREADY_EXISTED
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.create_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_snapshot_unknown_error(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_create_snapshot(*args)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = 'unknown_error'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.create_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_success(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        expected_cmd = ('vdi', 'delete', '-s', self._snapname,
                        self._src_vdiname)
        fake_execute.return_value = ('', '')
        self.client.delete_snapshot(*args)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_not_found(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        stdout = ''
        stderr = self.test_data.DOG_VDI_SNAPSHOT_TAG_NOT_FOUND
        fake_execute.return_value = (stdout, stderr)
        self.client.delete_snapshot(*args)
        self.assertTrue(fake_logger.warning.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_vdi_not_found(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        stdout = ''
        stderr = self.test_data.DOG_VDI_SNAPSHOT_VOLUME_NOT_FOUND
        fake_execute.return_value = (stdout, stderr)
        self.client.delete_snapshot(*args)
        self.assertTrue(fake_logger.warning.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_fail_to_connect_bugcase(self, fake_logger,
                                                     fake_execute):
        # NOTE(tishizaki): Sheepdog's bug case.
        # details are written to Sheepdog driver code.
        args = (self._src_vdiname, self._snapname)
        stdout = ''
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_reason = (_('Failed to connect to sheep daemon. '
                           'addr: %(addr)s, port: %(port)s'),
                           {'addr': SHEEP_ADDR, 'port': SHEEP_PORT})
        fake_execute.return_value = (stdout, stderr)
        ex = self.assertRaises(exception.SheepdogError,
                               self.client.delete_snapshot, *args)
        self.assertEqual(expected_reason, ex.kwargs['reason'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_fail_to_connect(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_delete_snapshot(*args)
        exit_code = 2
        stdout = ''
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.delete_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_snapshot_unknown_error(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname)
        cmd = self.test_data.cmd_dog_vdi_delete_snapshot(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'unknown_error'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.delete_snapshot, *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    def test_clone_success(self, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        src_volume = 'sheepdog:%(src_vdiname)s:%(snapname)s' % {
            'src_vdiname': self._src_vdiname, 'snapname': self._snapname}
        dst_volume = 'sheepdog:%s' % self._dst_vdiname
        expected_cmd = ('create', '-b', src_volume, dst_volume,
                        '%sG' % self._dst_vdisize)
        fake_execute.return_code = ("", "")
        self.client.clone(*args)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    def test_clone_success_without_size(self, fake_execute):
        args = (self._src_vdiname, self._snapname, self._dst_vdiname)
        src_volume = 'sheepdog:%(src_vdiname)s:%(snapname)s' % {
            'src_vdiname': self._src_vdiname, 'snapname': self._snapname}
        dst_volume = 'sheepdog:%s' % self._dst_vdiname
        expected_cmd = ('create', '-b', src_volume, dst_volume)
        fake_execute.return_code = ("", "")
        self.client.clone(*args)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_fail_to_connect(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_FAILED_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_dst_vdi_already_exists(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_VDI_ALREADY_EXISTS
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_src_vdi_not_found(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_VDI_NOT_FOUND
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_src_snapshot_not_found(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_SNAPSHOT_NOT_FOUND
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_too_large_volume_size(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_SIZE_TOO_LARGE
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_unknown_error(self, fake_logger, fake_execute):
        args = (self._src_vdiname, self._snapname,
                self._dst_vdiname, self._dst_vdisize)
        cmd = self.test_data.cmd_qemuimg_vdi_clone(*args)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError, self.client.clone,
                               *args)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_resize_success(self, fake_execute):
        expected_cmd = ('vdi', 'resize', self._vdiname, 10 * 1024 ** 3)
        fake_execute.return_value = ('', '')
        self.client.resize(self._vdiname, 10)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_resize_fail_to_connect(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_resize(self._vdiname, 10 * 1024 ** 3)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.resize, self._vdiname, 10)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_resize_vdi_not_found(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_resize(self._vdiname, 10 * 1024 ** 3)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = (self.test_data.DOG_COMMAND_ERROR_VDI_NOT_EXISTS %
                  {'vdiname': self._vdiname})
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.resize, self._vdiname, 1)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_resize_shrinking_not_supported(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_resize(self._vdiname, 1 * 1024 ** 3)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_VDI_RESIZE_SIZE_SHRINK
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.resize, self._vdiname, 1)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_resize_too_large_size(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_resize(self._vdiname, 5 * 1024 ** 4)
        exit_code = 64
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_VDI_RESIZE_TOO_LARGE
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.resize, self._vdiname, 5120)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_resize_unknown_error(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_resize(self._vdiname, 10 * 1024 ** 3)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.resize, self._vdiname, 10)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    def test_convert_success(self, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        expected_cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
                        src_path, dst_path)
        fake_execute.return_value = ('', '')
        self.client.convert(src_path, dst_path)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_fail_to_connect(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_FAILED_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_src_vdi_not_found(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_VDI_NOT_FOUND
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_src_file_not_found(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_FILE_NOT_FOUND
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_dst_vdi_already_exists(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_VDI_ALREADY_EXISTS
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_permission_denied(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_PERMISSION_DENIED
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_invalid_image_format(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_ERROR_INVALID_FORMAT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_invalid_image_driver(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = self.test_data.QEMU_IMG_INVALID_DRIVER
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_qemu_img')
    @mock.patch.object(sheepdog, 'LOG')
    def test_convert_unknown_error(self, fake_logger, fake_execute):
        src_path = 'src_path'
        dst_path = 'dst_path'
        cmd = ('convert', '-f', 'raw', '-t', 'none', '-O', 'raw',
               src_path, dst_path)
        exit_code = 1
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.convert, src_path, dst_path)
        self.assertTrue(fake_logger.error.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_parse_location')
    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_is_cloneable_success(self, fake_execute, fake_parse_loc):
        image_meta = self.test_data.TEST_IMAGE_META
        expected_cmd = ('vdi', 'list', '-r', image_meta['id'])
        image_location = 'sheepdog://%s' % image_meta['id']
        stdout = self.test_data.IS_CLONEABLE_SNAPSHOT
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        fake_parse_loc.return_value = image_meta['id']
        self.assertTrue((True, True),
                        self.client._is_cloneable(image_location,
                                                  image_meta))
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(1, fake_execute.call_count)

    @mock.patch.object(sheepdog.SheepdogClient, '_parse_location')
    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_is_cloneable_invalid_image_format(self, fake_logger, fake_execute,
                                               fake_parse_loc):
        image_meta = self.test_data.TEST_IMAGE_META
        image_location = 'sheepdog://%s' % image_meta['id']
        stdout = ''
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        invalid_image_meta = {'id': image_meta['id'],
                              'size': image_meta['size'], 'disk_format': 'iso'}
        self.assertEqual((False, False),
                         self.client._is_cloneable(image_location,
                                                   invalid_image_meta))
        self.assertEqual(0, fake_execute.call_count)
        self.assertTrue(fake_logger.debug.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_parse_location')
    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_is_cloneable_invalid_loc_format(self, fake_logger, fake_execute,
                                             fake_parse_loc):
        image_meta = self.test_data.TEST_IMAGE_META
        image_location = 'sheepdog://%s' % image_meta['id']
        stdout = ''
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        error = exception.ImageUnacceptable(image_id='invalid_image',
                                            reason=_('invalid'))
        fake_parse_loc.side_effect = error
        self.assertEqual((False, False),
                         self.client._is_cloneable(image_location,
                                                   image_meta))
        self.assertEqual(0, fake_execute.call_count)
        self.assertTrue(fake_logger.debug.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_parse_location')
    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_is_cloneable_image_id_not_found(self, fake_logger, fake_execute,
                                             fake_parse_loc):
        image_meta = self.test_data.TEST_IMAGE_META
        image_location = 'sheepdog://%s' % image_meta['id']
        expected_cmd = ('vdi', 'list', '-r', image_meta['id'])
        stdout = ''
        stderr = ''
        fake_parse_loc.return_value = image_meta['id']
        fake_execute.return_value = (stdout, stderr)
        self.assertEqual((False, False),
                         self.client._is_cloneable(image_location,
                                                   image_meta))
        self.assertEqual(1, fake_execute.call_count)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertTrue(fake_logger.debug.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_parse_location')
    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_is_cloneable_same_vdi_name(self, fake_logger, fake_execute,
                                        fake_parse_loc):
        # image_id is found in Sheepdog cluster but isn't a glance-image
        image_meta = self.test_data.TEST_IMAGE_META
        image_location = 'sheepdog://%s' % image_meta['id']
        expected_cmd = ('vdi', 'list', '-r', image_meta['id'])
        stdout = self.test_data.IS_CLONEABLE_VOLUME
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        fake_parse_loc.return_value = image_meta['id']
        self.assertEqual((True, False),
                         self.client._is_cloneable(image_location,
                                                   image_meta))
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertTrue(fake_logger.debug.called)

    def test_parse_location_success(self):
        image_meta = self.test_data.TEST_IMAGE_META
        loc_success = 'sheepdog://%s' % image_meta['id']
        name = self.client._parse_location(loc_success)
        self.assertEqual(image_meta['id'], name)

    def test_parse_location_none(self):
        loc_none = None
        exc = self.assertRaises(exception.ImageUnacceptable,
                                self.client._parse_location, loc_none)
        self.assertEqual('Image None is unacceptable: image_location is NULL.',
                         exc.msg)

    def test_parse_location_invalid_prefix(self):
        loc_not_found = 'fail'
        exc = self.assertRaises(exception.ImageUnacceptable,
                                self.client._parse_location, loc_not_found)
        self.assertEqual('Image fail is unacceptable: Not stored in sheepdog.',
                         exc.msg)

    def test_parse_location_not_include_image_id(self):
        loc_format_err = 'sheepdog://'
        exc = self.assertRaises(exception.ImageUnacceptable,
                                self.client._parse_location, loc_format_err)
        self.assertEqual('Image sheepdog:// is unacceptable: '
                         'Blank components.', exc.msg)

    def test_parse_location_multiple_fields(self):
        loc_format_err2 = 'sheepdog://f/f'
        exc = self.assertRaises(exception.ImageUnacceptable,
                                self.client._parse_location, loc_format_err2)
        self.assertEqual('Image sheepdog://f/f is unacceptable:'
                         ' Not a sheepdog image.', exc.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_disk_capacity_success(self, fake_execute):
        expected_cmd = ('node', 'info', '-r')
        total_gb = self.test_data.TEST_VOLUME_STATS['total_capacity_gb']
        free_gb = self.test_data.TEST_VOLUME_STATS['free_capacity_gb']
        used_gb = total_gb - free_gb
        expected = (total_gb, used_gb, free_gb)
        fake_execute.return_value = (self.test_data.DOG_NODE_INFO, '')
        actual = self.client.get_disk_capacity()
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(expected, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_disk_capacity_failed_to_connect(self, fake_logger,
                                                 fake_execute):
        cmd = self.test_data.CMD_DOG_NODE_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.get_disk_capacity)
        self.assertTrue(fake_logger.exception.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_disk_capacity_failed_to_get_node_info(self, fake_logger,
                                                       fake_execute):
        cmd = self.test_data.CMD_DOG_NODE_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_NODE_INFO_ERROR_GET_NO_NODE_INFO
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.get_disk_capacity)
        self.assertTrue(fake_logger.exception.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_disk_capacity_unknown_error(self, fake_logger, fake_execute):
        cmd = self.test_data.CMD_DOG_NODE_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'unknown'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.get_disk_capacity)
        self.assertTrue(fake_logger.exception.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_disk_capacity_parse_stdout_error(self, fake_logger,
                                                  fake_execute):
        stdout = 'stdout_dummy'
        stderr = ''
        fake_execute.return_value = (stdout, stderr)
        fake_execute.side_effect = None
        self.assertRaises(AttributeError, self.client.get_disk_capacity)
        self.assertTrue(fake_logger.exception.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_success(self, fake_execute):
        expected_cmd = ('vdi', 'list', '-r', self._vdiname)
        fake_execute.return_value = (self.test_data.DOG_VDI_LIST, '')

        actual = self.client.get_vdi_size(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(10, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_with_snapshot_success(self, fake_execute):
        expected_cmd = ('vdi', 'list', '-r', self._vdiname)
        fake_execute.return_value = (self.test_data.DOG_VDI_LIST_WITH_SNAPSHOT,
                                     '')
        actual = self.client.get_vdi_size(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(15, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_clone_success(self, fake_execute):
        expected_cmd = ('vdi', 'list', '-r', self._vdiname)
        fake_execute.return_value = (self.test_data.DOG_VDI_LIST_CLONE, '')

        actual = self.client.get_vdi_size(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(10, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_round_up_1_success(self, fake_execute):
        expected_cmd = ('vdi', 'list', '-r', self._vdiname)
        fake_execute.return_value = (
            self.test_data.DOG_VDI_LIST_SIZE_10G_MORE_1, '')

        actual = self.client.get_vdi_size(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(11, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_round_up_2_success(self, fake_execute):
        expected_cmd = ('vdi', 'list', '-r', self._vdiname)
        fake_execute.return_value = (
            self.test_data.DOG_VDI_LIST_SIZE_11G_LESS_1, '')

        actual = self.client.get_vdi_size(self._vdiname)
        fake_execute.assert_called_once_with(*expected_cmd)
        self.assertEqual(11, actual)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    def test_get_vdi_size_not_found_error(self, fake_execute):
        stdout = ''
        stderr = ''
        fake_execute.return_value = (stdout, stderr)

        self.assertRaises(exception.NotFound, self.client.get_vdi_size,
                          self._vdiname)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_vdi_size_stdout_parse_error(self, fake_logger, fake_execute):
        stdout = 'dummy'
        stderr = ''
        fake_execute.return_value = (stdout, stderr)

        self.assertRaises(IndexError, self.client.get_vdi_size, self._vdiname)
        self.assertTrue(fake_logger.exception.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_vdi_size_failed_to_connect(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_list(self._vdiname)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = self.test_data.DOG_COMMAND_ERROR_FAIL_TO_CONNECT
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))

        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.get_vdi_size, self._vdiname)
        self.assertTrue(fake_logger.exception.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog, 'LOG')
    def test_get_vdi_size_unknown_error(self, fake_logger, fake_execute):
        cmd = self.test_data.cmd_dog_vdi_list(self._vdiname)
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'unknown'
        expected_msg = self.test_data.sheepdog_cmd_error(cmd=cmd,
                                                         exit_code=exit_code,
                                                         stdout=stdout,
                                                         stderr=stderr)
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code,
            stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))

        ex = self.assertRaises(exception.SheepdogCmdError,
                               self.client.get_vdi_size, self._vdiname)
        self.assertTrue(fake_logger.exception.called)
        self.assertEqual(expected_msg, ex.msg)

    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    def test_rename_success(self, fake_create_snapshot, fake_clone,
                            fake_delete_snapshot, fake_delete):
        old_vdi_name = self._vdiname
        new_vdi_name = self.test_data.TEST_EXISTING_REF['source-name']
        snapshot_name = 'temp-rename-snapshot-' + old_vdi_name

        self.client.rename(old_vdi_name, new_vdi_name)
        fake_create_snapshot.assert_called_once_with(old_vdi_name,
                                                     snapshot_name)
        fake_clone.assert_called_once_with(old_vdi_name, snapshot_name,
                                           new_vdi_name)
        fake_delete_snapshot.assert_called_once_with(old_vdi_name,
                                                     snapshot_name)
        fake_delete.assert_called_once_with(old_vdi_name)

    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(sheepdog, 'LOG')
    def test_rename_failed_create_snapshot(self, fake_logger,
                                           fake_create_snapshot, fake_clone,
                                           fake_delete_snapshot, fake_delete):
        old_vdi_name = self._vdiname
        new_vdi_name = self.test_data.TEST_EXISTING_REF['source-name']
        fake_create_snapshot.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='out dummy', stderr='err dummy')

        self.assertRaises(exception.SheepdogCmdError,
                          self.client.rename, old_vdi_name, new_vdi_name)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(sheepdog, 'LOG')
    def test_rename_failed_clone(self, fake_logger, fake_create_snapshot,
                                 fake_clone, fake_delete_snapshot,
                                 fake_delete):
        old_vdi_name = self._vdiname
        new_vdi_name = self.test_data.TEST_EXISTING_REF['source-name']
        snapshot_name = 'temp-rename-snapshot-' + old_vdi_name
        fake_clone.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='out dummy', stderr='err dummy')

        self.assertRaises(exception.SheepdogCmdError,
                          self.client.rename, old_vdi_name, new_vdi_name)
        fake_delete_snapshot.assert_called_once_with(old_vdi_name,
                                                     snapshot_name)
        self.assertTrue(fake_logger.error.called)


class SheepdogDriverTestCase(test.TestCase):
    def setUp(self):
        super(SheepdogDriverTestCase, self).setUp()
        self._cfg = conf.Configuration(None)
        self._cfg.sheepdog_store_address = SHEEP_ADDR
        self._cfg.sheepdog_store_port = SHEEP_PORT
        self.driver = sheepdog.SheepdogDriver(configuration=self._cfg)
        db_driver = self.driver.configuration.db_driver
        self.db = importutils.import_module(db_driver)
        self.driver.db = self.db
        self.driver.do_setup(None)
        self.test_data = SheepdogDriverTestDataGenerator()
        self.client = self.driver.client
        self._vdiname = self.test_data.TEST_VOLUME.name
        self._vdisize = self.test_data.TEST_VOLUME.size
        self._src_vdiname = self.test_data.TEST_SNAPSHOT.volume_name
        self._snapname = self.test_data.TEST_SNAPSHOT.name
        self._dst_vdiname = self.test_data.TEST_CLONED_VOLUME.name
        self._dst_vdisize = self.test_data.TEST_CLONED_VOLUME.size

    @mock.patch.object(sheepdog.SheepdogClient, 'check_cluster_status')
    def test_check_for_setup_error(self, fake_execute):
        self.driver.check_for_setup_error()
        fake_execute.assert_called_once_with()

    @mock.patch.object(sheepdog.SheepdogClient, 'create')
    def test_create_volume(self, fake_execute):
        self.driver.create_volume(self.test_data.TEST_VOLUME)
        fake_execute.assert_called_once_with(self._vdiname, self._vdisize)

    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    def test_delete_volume(self, fake_execute):
        self.driver.delete_volume(self.test_data.TEST_VOLUME)
        fake_execute.assert_called_once_with(self._vdiname)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_disk_capacity')
    def test_update_volume_stats_success(self, fake_execute):
        total_gb = self.test_data.TEST_VOLUME_STATS['total_capacity_gb']
        free_gb = self.test_data.TEST_VOLUME_STATS['free_capacity_gb']
        used_gb = total_gb - free_gb
        expected = self.test_data.TEST_VOLUME_STATS
        fake_execute.return_value = (total_gb, used_gb, free_gb)
        actual = self.driver.get_volume_stats(True)
        self.assertDictMatch(expected, actual)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_disk_capacity')
    @mock.patch.object(sheepdog, 'LOG')
    def test_update_volume_stats_update_stats_error(self, fake_logger,
                                                    fake_execute):
        cmd = self.test_data.CMD_DOG_NODE_INFO
        exit_code = 2
        stdout = 'stdout_dummy'
        stderr = 'stderr_dummy'
        expected = self.test_data.TEST_VOLUME_STATS_ERROR
        fake_execute.side_effect = exception.SheepdogCmdError(
            cmd=cmd, exit_code=exit_code, stdout=stdout.replace('\n', '\\n'),
            stderr=stderr.replace('\n', '\\n'))
        actual = self.driver.get_volume_stats(True)
        self.assertTrue(fake_logger.exception.called)
        self.assertDictMatch(expected, actual)

    @mock.patch.object(image_utils, 'temporary_file')
    @mock.patch.object(image_utils, 'fetch_verify_image')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    @mock.patch.object(sheepdog.SheepdogClient, 'resize')
    @mock.patch.object(sheepdog.SheepdogClient, 'convert')
    def test_copy_image_to_volume_success(self, fake_convert, fake_resize,
                                          fake_delete, fake_fetch_verify_image,
                                          fake_temp_file):
        fake_context = {}
        fake_volume = self.test_data.TEST_VOLUME
        fake_image_service = mock.Mock()
        image_meta = self.test_data.TEST_IMAGE_META

        # check execute correctly.
        self.driver.copy_image_to_volume(fake_context, fake_volume,
                                         fake_image_service, image_meta['id'])
        fake_delete.assert_called_once_with(fake_volume.name)
        fake_convert.assert_called_once_with(
            mock.ANY, 'sheepdog:%s' % fake_volume.name)
        fake_resize.assert_called_once_with(fake_volume.name, fake_volume.size)

    @mock.patch.object(image_utils, 'temporary_file')
    @mock.patch.object(image_utils, 'fetch_verify_image')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete')
    @mock.patch.object(sheepdog.SheepdogClient, 'resize')
    @mock.patch.object(sheepdog.SheepdogClient, 'convert')
    def test_copy_image_to_volume_resize_error(self, fake_convert, fake_resize,
                                               fake_delete,
                                               fake_fetch_verify_image,
                                               fake_temp_file):
        fake_context = {}
        fake_volume = self.test_data.TEST_VOLUME
        fake_image_service = mock.Mock()
        image_meta = self.test_data.TEST_IMAGE_META
        fake_resize.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='dummy', stderr='dummy')
        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.copy_image_to_volume, fake_context,
                          fake_volume, fake_image_service, image_meta['id'])
        fake_delete.assert_called_with(fake_volume.name)
        self.assertEqual(2, fake_delete.call_count)

    @mock.patch.object(fileutils, 'open')
    @mock.patch.object(sheepdog.SheepdogClient, 'convert')
    def test_copy_volume_to_image_success(self, fake_execute, fake_file_open):
        context = {}
        src_volume = self.test_data.TEST_VOLUME
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_service = mock.Mock()

        expected_cmd = ('sheepdog:%s' % src_volume.name, mock.ANY)
        self.driver.copy_volume_to_image(context, src_volume,
                                         fake_image_service, image_meta)
        fake_execute.assert_called_once_with(*expected_cmd)

    @mock.patch.object(fileutils, 'open')
    @mock.patch.object(sheepdog, 'LOG')
    @mock.patch.object(sheepdog.SheepdogClient, 'convert')
    def test_copy_volume_to_image_update_error(self, fake_execute, fake_logger,
                                               fake_file_open):
        context = {}
        src_volume = self.test_data.TEST_VOLUME
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_service = mock.Mock()
        fake_image_service.update.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='dummy', stderr='dummy')
        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.copy_volume_to_image, context,
                          src_volume, fake_image_service, image_meta)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    def test_create_cloned_volume(self, fake_delete_snapshot,
                                  fake_clone, fake_create_snapshot):
        src_vol = self.test_data.TEST_VOLUME
        cloned_vol = self.test_data.TEST_CLONED_VOLUME

        self.driver.create_cloned_volume(cloned_vol, src_vol)
        snapshot_name = 'temp-snapshot-' + src_vol.name
        fake_create_snapshot.assert_called_once_with(src_vol.name,
                                                     snapshot_name)
        fake_clone.assert_called_once_with(src_vol.name, snapshot_name,
                                           cloned_vol.name, cloned_vol.size)
        fake_delete_snapshot.assert_called_once_with(src_vol.name,
                                                     snapshot_name)

    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    @mock.patch.object(sheepdog, 'LOG')
    def test_create_cloned_volume_failure(self, fake_logger,
                                          fake_delete_snapshot,
                                          fake_clone, fake_create_snapshot):
        src_vol = self.test_data.TEST_VOLUME
        cloned_vol = self.test_data.TEST_CLONED_VOLUME
        snapshot_name = 'temp-snapshot-' + src_vol.name

        fake_clone.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='dummy', stderr='dummy')
        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.create_cloned_volume,
                          cloned_vol, src_vol)
        fake_delete_snapshot.assert_called_once_with(src_vol.name,
                                                     snapshot_name)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    def test_create_snapshot(self, fake_create_snapshot):
        snapshot = self.test_data.TEST_SNAPSHOT
        self.driver.create_snapshot(snapshot)
        fake_create_snapshot.assert_called_once_with(snapshot.volume_name,
                                                     snapshot.name)

    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    def test_delete_snapshot(self, fake_delete_snapshot):
        snapshot = self.test_data.TEST_SNAPSHOT
        self.driver.delete_snapshot(snapshot)
        fake_delete_snapshot.assert_called_once_with(snapshot.volume_name,
                                                     snapshot.name)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    def test_clone_image_from_glance(self, fake_clone, fake_execute):
        context = {}
        fake_volume = self.test_data.TEST_VOLUME
        stdout = self.test_data.IS_CLONEABLE_SNAPSHOT
        stderr = ''
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_location = 'sheepdog://%s' % image_meta['id']
        image_location = (fake_image_location, None)
        image_service = ''
        expected_provider_location = 'sheepdog:%s' % fake_volume.name

        fake_execute.return_value = (stdout, stderr)
        model_updated, cloned = self.driver.clone_image(
            context, fake_volume, image_location, image_meta, image_service)

        self.assertEqual(1, fake_clone.call_count)
        self.assertTrue(cloned)
        self.assertEqual(expected_provider_location,
                         model_updated['provider_location'])

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog.SheepdogDriver, 'create_cloned_volume')
    def test_clone_image_from_volume(self, fake_create_cloned_volume,
                                     fake_execute):
        context = {}
        fake_volume = self.test_data.TEST_VOLUME
        stdout = self.test_data.IS_CLONEABLE_VOLUME
        stderr = ''
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_location = 'sheepdog://%s' % image_meta['id']
        image_location = (fake_image_location, None)
        image_service = ''
        expected_provider_location = 'sheepdog:%s' % fake_volume.name

        fake_execute.return_value = (stdout, stderr)
        model_updated, cloned = self.driver.clone_image(
            context, fake_volume, image_location, image_meta, image_service)

        self.assertEqual(1, fake_create_cloned_volume.call_count)
        self.assertTrue(cloned)
        self.assertEqual(expected_provider_location,
                         model_updated['provider_location'])

    @mock.patch.object(sheepdog.SheepdogClient, '_is_cloneable')
    def test_clone_image_failure(self, fake_is_cloneable):
        context = {}
        fake_volume = {}
        image_location = ('image_location', None)
        image_meta = {}
        image_service = ''
        fake_is_cloneable.return_value = (False, False)
        result = self.driver.clone_image(
            context, fake_volume, image_location, image_meta, image_service)
        self.assertEqual(({}, False), result)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog.SheepdogClient, 'clone')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_image_create_from_snapshot_fail(self, fake_logger,
                                                   fake_create_volume,
                                                   fake_execute):
        context = {}
        fake_volume = self.test_data.TEST_VOLUME
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_location = 'sheepdog://%s' % image_meta['id']
        image_location = (fake_image_location, None)
        image_service = ''
        stdout = self.test_data.IS_CLONEABLE_SNAPSHOT
        stderr = ''
        fake_create_volume.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout=stdout, stderr=stderr)
        fake_execute.return_value = (stdout, stderr)
        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.clone_image,
                          context, fake_volume, image_location,
                          image_meta, image_service)
        self.assertEqual(1, fake_create_volume.call_count)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogClient, '_run_dog')
    @mock.patch.object(sheepdog.SheepdogDriver, 'create_cloned_volume')
    @mock.patch.object(sheepdog, 'LOG')
    def test_clone_image_create_from_volume_fail(self, fake_logger,
                                                 fake_create_volume,
                                                 fake_execute):
        context = {}
        fake_volume = self.test_data.TEST_VOLUME
        image_meta = self.test_data.TEST_IMAGE_META
        fake_image_location = 'sheepdog://%s' % image_meta['id']
        image_location = (fake_image_location, None)
        image_service = ''
        stdout = self.test_data.IS_CLONEABLE_VOLUME
        stderr = ''
        fake_create_volume.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout=stdout, stderr=stderr)
        fake_execute.return_value = (stdout, stderr)
        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.clone_image,
                          context, fake_volume, image_location,
                          image_meta, image_service)
        self.assertEqual(1, fake_create_volume.call_count)
        self.assertTrue(fake_logger.error.called)

    def test_create_volume_from_snapshot(self):
        dst_volume = self.test_data.TEST_CLONED_VOLUME
        snapshot = self.test_data.TEST_SNAPSHOT
        with mock.patch.object(self.client, 'clone') as fake_execute:
            self.driver.create_volume_from_snapshot(dst_volume, snapshot)
            fake_execute.assert_called_once_with(self._src_vdiname,
                                                 self._snapname,
                                                 self._dst_vdiname,
                                                 self._dst_vdisize)

    def test_local_path(self):
        fake_volume = self.test_data.TEST_VOLUME
        expected_path = 'sheepdog:%s' % fake_volume.name

        ret = self.driver.local_path(fake_volume)
        self.assertEqual(expected_path, ret)

    @mock.patch.object(sheepdog.SheepdogClient, 'resize')
    @mock.patch.object(sheepdog, 'LOG')
    def test_extend_volume(self, fake_logger, fake_execute):
        self.driver.extend_volume(self.test_data.TEST_VOLUME, 10)
        fake_execute.assert_called_once_with(self._vdiname, 10)
        self.assertTrue(fake_logger.debug.called)

    @mock.patch.object(db, 'volume_get')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(backup_driver, 'BackupDriver')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    def test_backup_volume_success(self, fake_delete_snapshot,
                                   fake_backup_service, fake_create_snapshot,
                                   fake_volume_get):
        fake_context = {}
        fake_volume = self.test_data.TEST_VOLUME
        fake_backup = self.test_data.TEST_BACKUP_VOLUME
        fake_backup_service = mock.Mock()
        fake_volume_get.return_value = fake_volume

        self.driver.backup_volume(fake_context,
                                  fake_backup,
                                  fake_backup_service)

        self.assertEqual(1, fake_create_snapshot.call_count)
        self.assertEqual(2, fake_delete_snapshot.call_count)
        self.assertEqual(fake_create_snapshot.call_args,
                         fake_delete_snapshot.call_args)

        call_args, call_kwargs = fake_backup_service.backup.call_args
        call_backup, call_sheepdog_fd = call_args
        self.assertEqual(fake_backup, call_backup)
        self.assertIsInstance(call_sheepdog_fd, sheepdog.SheepdogIOWrapper)

    @mock.patch.object(db, 'volume_get')
    @mock.patch.object(sheepdog, 'LOG')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(backup_driver, 'BackupDriver')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    def test_backup_volume_fail_to_create_snap(self, fake_delete_snapshot,
                                               fake_backup_service,
                                               fake_create_snapshot,
                                               fake_logger, fake_volume_get):
        fake_context = {}
        fake_volume = self.test_data.TEST_VOLUME
        fake_backup = self.test_data.TEST_BACKUP_VOLUME
        fake_volume_get.return_value = fake_volume
        fake_create_snapshot.side_effect = exception.SheepdogCmdError(
            cmd='dummy', exit_code=1, stdout='dummy', stderr='dummy')

        self.assertRaises(exception.SheepdogCmdError,
                          self.driver.backup_volume,
                          fake_context,
                          fake_backup,
                          fake_backup_service)
        self.assertEqual(1, fake_create_snapshot.call_count)
        self.assertEqual(1, fake_delete_snapshot.call_count)
        self.assertEqual(fake_create_snapshot.call_args,
                         fake_delete_snapshot.call_args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(db, 'volume_get')
    @mock.patch.object(sheepdog, 'LOG')
    @mock.patch.object(sheepdog.SheepdogClient, 'create_snapshot')
    @mock.patch.object(backup_driver, 'BackupDriver')
    @mock.patch.object(sheepdog.SheepdogClient, 'delete_snapshot')
    def test_backup_volume_fail_to_backup_vol(self, fake_delete_snapshot,
                                              fake_backup_service,
                                              fake_create_snapshot,
                                              fake_logger, fake_volume_get):
        fake_context = {}
        fake_volume = self.test_data.TEST_VOLUME
        fake_backup = self.test_data.TEST_BACKUP_VOLUME
        fake_volume_get.return_value = fake_volume

        class BackupError(Exception):
            pass

        fake_backup_service.backup.side_effect = BackupError()

        self.assertRaises(BackupError,
                          self.driver.backup_volume,
                          fake_context,
                          fake_backup,
                          fake_backup_service)
        self.assertEqual(1, fake_create_snapshot.call_count)
        self.assertEqual(2, fake_delete_snapshot.call_count)
        self.assertEqual(fake_create_snapshot.call_args,
                         fake_delete_snapshot.call_args)
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(backup_driver, 'BackupDriver')
    def test_restore_backup(self, fake_backup_service):
        fake_context = {}
        fake_backup = self.test_data.TEST_BACKUP_VOLUME
        fake_volume = self.test_data.TEST_VOLUME

        self.driver.restore_backup(
            fake_context, fake_backup, fake_volume, fake_backup_service)

        call_args, call_kwargs = fake_backup_service.restore.call_args
        call_backup, call_volume_id, call_sheepdog_fd = call_args
        self.assertEqual(fake_backup, call_backup)
        self.assertEqual(fake_volume.id, call_volume_id)
        self.assertIsInstance(call_sheepdog_fd, sheepdog.SheepdogIOWrapper)

    @mock.patch.object(sheepdog.SheepdogClient, 'rename')
    @mock.patch.object(volutils, 'check_already_managed_volume')
    def test_manage_existing_success(self, fake_check_already_managed_volume,
                                     fake_rename):
        fake_volume = self.test_data.TEST_VOLUME
        fake_ref = self.test_data.TEST_EXISTING_REF
        fake_check_already_managed_volume.return_value = False

        self.driver.manage_existing(fake_volume, fake_ref)
        fake_check_already_managed_volume.assert_called_once_with(
            self.db, fake_ref['source-name'])
        fake_rename.assert_called_once_with(fake_ref['source-name'],
                                            fake_volume.name)

    @mock.patch.object(sheepdog.SheepdogClient, 'rename')
    @mock.patch.object(volutils, 'check_already_managed_volume')
    def test_manage_existing_fail_already_managed_volume(
            self, fake_check_already_managed_volume, fake_rename):
        fake_volume = self.test_data.TEST_VOLUME
        fake_ref = self.test_data.TEST_EXISTING_REF
        fake_check_already_managed_volume.return_value = True
        self.assertRaises(exception.ManageExistingAlreadyManaged,
                          self.driver.manage_existing,
                          fake_volume, fake_ref)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_vdi_size')
    def test_manage_existing_get_size_success(self, fake_get_vdi_size):
        fake_get_vdi_size.return_value = 10
        fake_ref = self.test_data.TEST_EXISTING_REF

        size = self.driver.manage_existing_get_size(self.test_data.TEST_VOLUME,
                                                    fake_ref)
        fake_get_vdi_size.assert_called_once_with(fake_ref['source-name'])
        self.assertEqual(10, size)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_vdi_size')
    def test_manage_existing_get_size_sourcename_key_empty(
            self, fake_get_vdi_size):
        fake_ref = {'source-id': ''}

        self.assertRaises(exception.ManageExistingInvalidReference,
                          self.driver.manage_existing_get_size,
                          self.test_data.TEST_VOLUME, fake_ref)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_vdi_size')
    def test_manage_existing_get_size_sourcename_value_empty(
            self, fake_get_vdi_size):
        fake_ref = {'source-name': '', 'source-id': ''}

        self.assertRaises(exception.ManageExistingInvalidReference,
                          self.driver.manage_existing_get_size,
                          self.test_data.TEST_VOLUME, fake_ref)

    @mock.patch.object(sheepdog.SheepdogClient, 'get_vdi_size')
    def test_manage_existing_get_size_vdi_not_found(
            self, fake_get_vdi_size):
        fake_get_vdi_size.side_effect = exception.NotFound()

        self.assertRaises(exception.ManageExistingInvalidReference,
                          self.driver.manage_existing_get_size,
                          self.test_data.TEST_VOLUME,
                          self.test_data.TEST_EXISTING_REF)

    @mock.patch.object(sheepdog.SheepdogClient, 'rename')
    def test_unmanage_success(self, fake_rename):
        fake_volume = self.test_data.TEST_VOLUME
        new_volume_name = fake_volume.name + '-unmanaged'

        self.driver.unmanage(fake_volume)
        fake_rename.assert_called_once_with(fake_volume.name, new_volume_name)

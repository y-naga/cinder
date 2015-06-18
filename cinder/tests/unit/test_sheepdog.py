
# Copyright (c) 2013 Zelin.io
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


import contextlib

import mock
from oslo_concurrency import processutils
from oslo_utils import importutils
from oslo_utils import units
import six

from cinder import exception
from cinder.i18n import _, _LE
from cinder.image import image_utils
from cinder import test
from cinder.volume import configuration as conf
from cinder.volume.drivers import sheepdog


SHEEP_ADDR = '127.0.0.1'
SHEEP_PORT = 7000

COLLIE_NODE_INFO = """
0 107287605248 3623897354 3%
Total 107287605248 3623897354 3% 54760833024
"""

class SheepdogDriverTestData(object):
    def CMD_DOG_CLUSTER_INFO(self):
        return ('dog', 'cluster', 'info', '--address', SHEEP_ADDR, '--port',
                str(SHEEP_PORT))

    def CMD_DOG_VDI_CREATE(self, name, size):
        return ('dog', 'vdi', 'create', name, '%sG' % size , '--address',
                SHEEP_ADDR, '--port', str(SHEEP_PORT))

    TEST_VOLUME = {
        'name': 'volume-00000001',
        'size': 1,
        'volume_name': '1',
        'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66',
        'provider_auth': None,
        'host': 'host@backendsec#unit_test_pool',
        'project_id': 'project',
        'provider_location': 'location',
        'display_name': 'vol1',
        'display_description': 'unit test volume',
        'volume_type_id': None,
        'consistencygroup_id': None
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

    COLLIE_CLUSTER_INFO_WAITING_FORMAT = """\
Cluster status: Waiting for cluster to be formatted
"""

    COLLIE_CLUSTER_INFO_WAITING_OTHER_NODES = """\
Cluster status: Waiting for other nodes to join cluster

Cluster created at Thu Jun 18 10:28:34 2015

Epoch Time           Version [Host:Port:V-Nodes,,,]
2015-06-18 10:28:34      1 [127.0.0.1:7000:128, 127.0.0.1:7001:128]
"""

    COLLIE_CLUSTER_INFO_SYSTEM_ERROR = """\
Cluster status: System error
"""

class FakeImageService(object):
    def download(self, context, image_id, path):
        pass


class SheepdogTestCase(test.TestCase):
    def setUp(self):
        super(SheepdogTestCase, self).setUp()
        self.driver = sheepdog.SheepdogDriver(
            configuration = conf.Configuration(None))
        self.sheep_addr = '127.0.0.1'
        self.sheep_port = 7000

        db_driver = self.driver.configuration.db_driver
        self.db = importutils.import_module(db_driver)
        self.driver.db = self.db
        self.driver.do_setup(None)
        self.test_data = SheepdogDriverTestData()

    def test_command_execute_success(self):
        cmd = ('dog', 'vdi', 'create', 'sample', '1G')
        with mock.patch.object(self.driver, '_execute') as fake_execute:
            fake_execute.return_value = ('', '')
            self.driver._command_execute(*cmd)
        fake_execute.assert_called_once_with(*cmd)

    def test_command_execute_failed(self):
        cmd = ('dog', 'vdi', 'create', 'sample', '1G')
        exit_code = 1
        stdout = 'stdout\nline2\nline3'
        stderr = 'stderr\nline2\nline3'
        expect_stdout = 'stdout\\nline2\\nline3'
        expect_stderr = 'stderr\\nline2\\nline3'
        with mock.patch.object(self.driver, '_execute') as fake_execute:
            fake_execute.side_effect = processutils.ProcessExecutionError(
                cmd = cmd, exit_code = exit_code, stdout = stdout,
                stderr = stderr)
            ex = self.assertRaises(exception.SheepdogCmdException,
                    self.driver._command_execute, *cmd)
            self.assertEqual(cmd, ex.kwargs['cmd'])
            self.assertEqual(exit_code, ex.kwargs['rc'])
            self.assertEqual(expect_stdout, ex.kwargs['out'])
            self.assertEqual(expect_stderr, ex.kwargs['err'])

    def test_sheep_args(self):
        args = ('--address', self.sheep_addr, '--port', str(self.sheep_port))
        self.assertEqual(args, self.driver._sheep_args())

    def test_update_volume_stats(self):
        def fake_stats(*args):
            return COLLIE_NODE_INFO, ''
        self.stubs.Set(self.driver, '_execute', fake_stats)
        expected = dict(
            volume_backend_name='sheepdog',
            vendor_name='Open Source',
            dirver_version=self.driver.VERSION,
            storage_protocol='sheepdog',
            total_capacity_gb=float(107287605248) / units.Gi,
            free_capacity_gb=float(107287605248 - 3623897354) / units.Gi,
            reserved_percentage=0,
            QoS_support=False)
        actual = self.driver.get_volume_stats(True)
        self.assertDictMatch(expected, actual)

    def test_update_volume_stats_error(self):
        def fake_stats(*args):
            raise processutils.ProcessExecutionError()
        self.stubs.Set(self.driver, '_execute', fake_stats)
        expected = dict(
            volume_backend_name='sheepdog',
            vendor_name='Open Source',
            dirver_version=self.driver.VERSION,
            storage_protocol='sheepdog',
            total_capacity_gb='unknown',
            free_capacity_gb='unknown',
            reserved_percentage=0,
            QoS_support=False)
        actual = self.driver.get_volume_stats(True)
        self.assertDictMatch(expected, actual)

    def test_check_for_setup_error_0_5(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            with mock.patch.object(sheepdog, 'LOG') as fake_logger:
                fake_command_execute.return_value = (
                    self.test_data.COLLIE_CLUSTER_INFO_0_5, '')
                self.driver.check_for_setup_error()
        fake_command_execute.assert_called_once_with(
            *self.test_data.CMD_DOG_CLUSTER_INFO())
        fake_logger.debug.assert_called_with('Sheepdog cluster is running.')

    def test_check_for_setup_error_0_6(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            with mock.patch.object(sheepdog, 'LOG') as fake_logger:
                fake_command_execute.return_value = (
                    self.test_data.COLLIE_CLUSTER_INFO_0_6, '')
                self.driver.check_for_setup_error()
        fake_command_execute.assert_called_once_with(
            *self.test_data.CMD_DOG_CLUSTER_INFO())
        fake_logger.debug.assert_called_with('Sheepdog cluster is running.')

    def test_check_for_setup_error_waiting_format(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.return_value = (
                self.test_data.COLLIE_CLUSTER_INFO_WAITING_FORMAT, '')
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.check_for_setup_error)
            self.assertIn('Sheepdog cluster is not formatted. ' \
                        'Please format the Sheepdog cluster.', ex.msg)

    def test_check_for_setup_error_waiting_other_nodes(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.return_value = (
                self.test_data.COLLIE_CLUSTER_INFO_WAITING_OTHER_NODES, '')
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.check_for_setup_error)
            self.assertIn('All nodes does not join to Sheepdog cluster. ' \
                        'Please start sheep process of all nodes.', ex.msg)

    def test_check_for_setup_error_shutting_down(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.return_value = (
                self.test_data.COLLIE_CLUSTER_INFO_SYSTEM_ERROR, '')
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.check_for_setup_error)
            self.assertIn('Sheepdog cluster is not running. %s' % \
                    self.test_data.COLLIE_CLUSTER_INFO_SYSTEM_ERROR, ex.msg)

    def test_check_for_setup_error_cmd_notfound(self):
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        rc = 127
        out = ''
        err = 'dog: command not found'
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.check_for_setup_error)
            self.assertIn('Sheepdog is not installed.', ex.msg)

    def test_check_for_setup_error_failed_connected(self):
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        rc = 2
        out = ''
        err = 'failed to connect to 1.1.1.1'
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.check_for_setup_error)
            self.assertIn('Failed to connect sheep process.', ex.msg)

    def test_check_for_setup_error_failed_uncatched(self):
        cmd = self.test_data.CMD_DOG_CLUSTER_INFO
        rc = 1
        out = 'stdout'
        err = 'uncatched error'
        expect_msg = _('Sheepdog driver command exception: %s '
            '(Return Code: %s) (Stdout: %s).(Stderr: %s)' % \
            (cmd, rc, out, err))
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.SheepdogCmdException,
                    self.driver.check_for_setup_error)
            self.assertEqual(expect_msg, ex.msg)

    def test_copy_image_to_volume(self):
        @contextlib.contextmanager
        def fake_temp_file():
            class FakeTmp(object):
                def __init__(self, name):
                    self.name = name
            yield FakeTmp('test').name

        def fake_try_execute(obj, *command, **kwargs):
            return True

        self.stubs.Set(image_utils, 'temporary_file', fake_temp_file)
        self.stubs.Set(image_utils, 'fetch_verify_image',
                       lambda w, x, y, z: None)
        self.stubs.Set(image_utils, 'convert_image',
                       lambda x, y, z: None)
        self.stubs.Set(sheepdog.SheepdogDriver,
                       '_try_execute',
                       fake_try_execute)
        self.driver.copy_image_to_volume(None, {'name': 'test',
                                                'size': 1},
                                         FakeImageService(), None)

    def test_copy_volume_to_image(self):
        fake_context = {}
        fake_volume = {'name': 'volume-00000001'}
        fake_image_service = mock.Mock()
        fake_image_service_update = mock.Mock()
        fake_image_meta = {'id': '10958016-e196-42e3-9e7f-5d8927ae3099'}

        patch = mock.patch.object
        with patch(self.driver, '_try_execute') as fake_try_execute:
            with patch(fake_image_service,
                       'update') as fake_image_service_update:
                self.driver.copy_volume_to_image(fake_context,
                                                 fake_volume,
                                                 fake_image_service,
                                                 fake_image_meta)

                expected_cmd = ('qemu-img',
                                'convert',
                                '-f', 'raw',
                                '-t', 'none',
                                '-O', 'raw',
                                'sheepdog:%s' % fake_volume['name'],
                                mock.ANY)
                fake_try_execute.assert_called_once_with(*expected_cmd)
                fake_image_service_update.assert_called_once_with(
                    fake_context, fake_image_meta['id'], mock.ANY, mock.ANY)

    def test_copy_volume_to_image_nonexistent_volume(self):
        fake_context = {}
        fake_volume = {
            'name': 'nonexistent-volume-82c4539e-c2a5-11e4-a293-0aa186c60fe0'}
        fake_image_service = mock.Mock()
        fake_image_meta = {'id': '10958016-e196-42e3-9e7f-5d8927ae3099'}

        # The command is expected to fail, so we don't want to retry it.
        self.driver._try_execute = self.driver._execute

        args = (fake_context, fake_volume, fake_image_service, fake_image_meta)
        expected_errors = (processutils.ProcessExecutionError, OSError)
        self.assertRaises(expected_errors,
                          self.driver.copy_volume_to_image,
                          *args)

    def test_create_cloned_volume(self):
        src_vol = {
            'project_id': 'testprjid',
            'name': six.text_type('volume-00000001'),
            'size': '20',
            'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66',
        }
        target_vol = {
            'project_id': 'testprjid',
            'name': six.text_type('volume-00000002'),
            'size': '20',
            'id': '582a1efa-be6a-11e4-a73b-0aa186c60fe0',
        }

        with mock.patch.object(self.driver,
                               '_try_execute') as mock_exe:
            self.driver.create_cloned_volume(target_vol, src_vol)

            snapshot_name = src_vol['name'] + '-temp-snapshot'
            qemu_src_volume_name = "sheepdog:%s" % src_vol['name']
            qemu_snapshot_name = '%s:%s' % (qemu_src_volume_name,
                                            snapshot_name)
            qemu_target_volume_name = "sheepdog:%s" % target_vol['name']
            calls = [
                mock.call('qemu-img', 'snapshot', '-c',
                          snapshot_name, qemu_src_volume_name),
                mock.call('qemu-img', 'create', '-b',
                          qemu_snapshot_name,
                          qemu_target_volume_name,
                          '%sG' % target_vol['size']),
            ]
            mock_exe.assert_has_calls(calls)

    def test_create_cloned_volume_failure(self):
        fake_name = six.text_type('volume-00000001')
        fake_size = '20'
        fake_vol = {'project_id': 'testprjid', 'name': fake_name,
                    'size': fake_size,
                    'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66'}
        src_vol = fake_vol

        patch = mock.patch.object
        with patch(self.driver, '_try_execute',
                   side_effect=processutils.ProcessExecutionError):
            with patch(self.driver, 'create_snapshot'):
                with patch(self.driver, 'delete_snapshot'):
                    self.assertRaises(exception.VolumeBackendAPIException,
                                      self.driver.create_cloned_volume,
                                      fake_vol,
                                      src_vol)

    def test_clone_image_success(self):
        context = {}
        fake_name = six.text_type('volume-00000001')
        fake_size = '2'
        fake_vol = {'project_id': 'testprjid', 'name': fake_name,
                    'size': fake_size,
                    'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66'}

        image_location = ('sheepdog:192.168.1.111:7000:Alice', None)
        image_id = "caa4ffd0-fake-fake-fake-f8631a807f5a"
        image_meta = {'id': image_id, 'size': 1, 'disk_format': 'raw'}
        image_service = ''

        patch = mock.patch.object
        with patch(self.driver, '_try_execute', return_value=True):
            with patch(self.driver, 'create_cloned_volume'):
                with patch(self.driver, '_resize'):
                    model_updated, cloned = self.driver.clone_image(
                        context, fake_vol, image_location,
                        image_meta, image_service)

        self.assertTrue(cloned)
        self.assertEqual("sheepdog:%s" % fake_name,
                         model_updated['provider_location'])

    def test_clone_image_failure(self):
        context = {}
        fake_vol = {}
        image_location = ('image_location', None)
        image_meta = {}
        image_service = ''

        with mock.patch.object(self.driver, '_is_cloneable',
                               lambda *args: False):
            result = self.driver.clone_image(
                context, fake_vol, image_location, image_meta, image_service)
            self.assertEqual(({}, False), result)

    def test_is_cloneable(self):
        uuid = '87f1b01c-f46c-4537-bd5d-23962f5f4316'
        location = 'sheepdog:ip:port:%s' % uuid
        image_meta = {'id': uuid, 'size': 1, 'disk_format': 'raw'}
        invalid_image_meta = {'id': uuid, 'size': 1, 'disk_format': 'iso'}

        with mock.patch.object(self.driver, '_try_execute') as try_execute:
            self.assertTrue(
                self.driver._is_cloneable(location, image_meta))
            expected_cmd = ('collie', 'vdi', 'list',
                            '--address', 'ip',
                            '--port', 'port',
                            uuid)
            try_execute.assert_called_once_with(*expected_cmd)

            # check returning False without executing a command
            self.assertFalse(
                self.driver._is_cloneable('invalid-location', image_meta))
            self.assertFalse(
                self.driver._is_cloneable(location, invalid_image_meta))
            self.assertEqual(1, try_execute.call_count)

        error = processutils.ProcessExecutionError
        with mock.patch.object(self.driver, '_try_execute',
                               side_effect=error) as fail_try_execute:
            self.assertFalse(
                self.driver._is_cloneable(location, image_meta))
            fail_try_execute.assert_called_once_with(*expected_cmd)

    def test_extend_volume(self):
        fake_name = u'volume-00000001'
        fake_size = '20'
        fake_vol = {'project_id': 'testprjid', 'name': fake_name,
                    'size': fake_size,
                    'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66'}

        self.mox.StubOutWithMock(self.driver, '_resize')
        size = int(fake_size) * units.Gi
        self.driver._resize(fake_vol, size=size)

        self.mox.ReplayAll()
        self.driver.extend_volume(fake_vol, fake_size)

        self.mox.VerifyAll()

    def test_create_volume_success(self):
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.return_value = ('', '')
            self.driver.create_volume(self.test_data.TEST_VOLUME)
        fake_command_execute.assert_called_once_with(
            *self.test_data.CMD_DOG_VDI_CREATE(
                self.test_data.TEST_VOLUME['name'],
                self.test_data.TEST_VOLUME['size']))

    def test_create_volume_failed_vdi_already_exist(self):
        cmd = self.test_data.CMD_DOG_VDI_CREATE(
                self.test_data.TEST_VOLUME['name'],
                self.test_data.TEST_VOLUME['size'])
        rc = 1
        out = ''
        err = 'Failed to create VDI %s: VDI exists already' % \
               self.test_data.TEST_VOLUME['name']
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.create_volume, self.test_data.TEST_VOLUME)
            self.assertIn('Volume already exists. %s' % \
                self.test_data.TEST_VOLUME['name'], ex.msg)

    def test_create_volume_failed_connected(self):
        cmd = self.test_data.CMD_DOG_VDI_CREATE(
                self.test_data.TEST_VOLUME['name'],
                self.test_data.TEST_VOLUME['size'])
        rc = 2
        out = ''
        err = 'failed to connect to 1.1.1.1'
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.create_volume, self.test_data.TEST_VOLUME)
            self.assertIn('Failed to connect sheep process.', ex.msg)

    def test_create_volume_failed_diskfull(self):
        cmd = self.test_data.CMD_DOG_VDI_CREATE(
                self.test_data.TEST_VOLUME['name'],
                self.test_data.TEST_VOLUME['size'])
        rc = 1
        out = ''
        err = 'fail 8011111100000000, Server has no space for new objects'
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.create_volume, self.test_data.TEST_VOLUME)
            self.assertIn('Server has no space for new objects', ex.msg)

    def test_create_volume_failed_uncatched(self):
        cmd = self.test_data.CMD_DOG_VDI_CREATE(
                self.test_data.TEST_VOLUME['name'],
                self.test_data.TEST_VOLUME['size'])
        rc = 1
        out = 'stdout'
        err = 'uncatched_err'
        expect_msg = _('Sheepdog driver command exception: %s '
                '(Return Code: %s) (Stdout: %s).(Stderr: %s)' % \
                (cmd, rc, out, err))
        with mock.patch.object(self.driver, '_command_execute') as \
                fake_command_execute:
            fake_command_execute.side_effect = exception.SheepdogCmdException(
                cmd = cmd, rc = rc, out = out, err = err)
            ex = self.assertRaises(exception.VolumeBackendAPIException,
                    self.driver.create_volume, self.test_data.TEST_VOLUME)
            self.assertEqual(expect_msg, ex.msg)

    def test_create_volume_from_snapshot(self):
        fake_name = u'volume-00000001'
        fake_size = '10'
        fake_vol = {'project_id': 'testprjid', 'name': fake_name,
                    'size': fake_size,
                    'id': 'a720b3c0-d1f0-11e1-9b23-0800200c9a66'}

        ss_uuid = '00000000-0000-0000-0000-c3aa7ee01536'
        fake_snapshot = {'volume_name': fake_name,
                         'name': 'volume-%s' % ss_uuid,
                         'id': ss_uuid,
                         'size': fake_size}

        with mock.patch.object(sheepdog.SheepdogDriver,
                               '_try_execute') as mock_exe:
            self.driver.create_volume_from_snapshot(fake_vol, fake_snapshot)
            args = ['qemu-img', 'create', '-b',
                    "sheepdog:%s:%s" % (fake_snapshot['volume_name'],
                                        fake_snapshot['name']),
                    "sheepdog:%s" % fake_vol['name'],
                    "%sG" % fake_vol['size']]
            mock_exe.assert_called_once_with(*args)

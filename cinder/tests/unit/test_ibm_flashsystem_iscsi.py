# Copyright 2015 IBM Corp.
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
#

"""
Tests for the IBM FlashSystem iSCSI volume driver.
"""

import mock
from oslo_concurrency import processutils
from oslo_log import log as logging
from oslo_utils import excutils
import six

import random

from cinder import context
from cinder import exception
from cinder import test
from cinder.tests.unit import test_ibm_flashsystem as fscommon
from cinder import utils
from cinder.volume import configuration as conf
from cinder.volume.drivers.ibm import flashsystem_iscsi
from cinder.volume import volume_types

LOG = logging.getLogger(__name__)


class FlashSystemManagementSimulator(fscommon.FlashSystemManagementSimulator):
    def __init__(self):
        # Default protocol is iSCSI
        self._protocol = 'iSCSI'
        self._volumes_list = {}
        self._hosts_list = {}
        self._mappings_list = {}
        self._next_cmd_error = {
            'lsnode': '',
            'lssystem': '',
            'lsmdiskgrp': ''
        }
        self._errors = {
            # CMMVC50000 is a fake error which indicates that command has not
            # got expected results. This error represents kinds of CLI errors.
            'CMMVC50000': ('', 'CMMVC50000 The command can not be executed '
                               'successfully.')
        }


class FlashSystemFakeISCSIDriver(flashsystem_iscsi.FlashSystemISCSIDriver):
    def __init__(self, *args, **kwargs):
        super(FlashSystemFakeISCSIDriver, self).__init__(*args, **kwargs)

    def set_fake_storage(self, fake):
        self.fake_storage = fake

    def _ssh(self, cmd, check_exit_code=True):
        ret = None
        try:
            LOG.debug('Run CLI command: %s', cmd)
            utils.check_ssh_injection(cmd)
            ret = self.fake_storage.execute_command(cmd, check_exit_code)
            (stdout, stderr) = ret
            LOG.debug('CLI output:\n stdout: %(stdout)s\n stderr: '
                      '%(stderr)s', {'stdout': stdout, 'stderr': stderr})

        except processutils.ProcessExecutionError as e:
            with excutils.save_and_reraise_exception():
                LOG.debug('CLI Exception output:\n stdout: %(out)s\n '
                          'stderr: %(err)s', {'out': e.stdout,
                                              'err': e.stderr})
        return ret


class FlashSystemISCSIDriverTestCase(test.TestCase):

    def _set_flag(self, flag, value):
        group = self.driver.configuration.config_group
        self.driver.configuration.set_override(flag, value, group)

    def _reset_flags(self):
        self.driver.configuration.local_conf.reset()
        for k, v in self._def_flags.items():
            self._set_flag(k, v)

    def _generate_vol_info(self,
                           vol_name,
                           vol_size=10,
                           vol_status='available'):
        rand_id = six.text_type(random.randint(10000, 99999))
        if not vol_name:
            vol_name = 'test_volume%s' % rand_id

        return {'name': vol_name,
                'size': vol_size,
                'id': '%s' % rand_id,
                'volume_type_id': None,
                'status': vol_status,
                'mdisk_grp_name': 'mdiskgrp0'}

    def _generate_snap_info(self,
                            vol_name,
                            vol_id,
                            vol_size,
                            vol_status,
                            snap_status='available'):
        rand_id = six.text_type(random.randint(10000, 99999))
        return {'name': 'test_snap_%s' % rand_id,
                'id': rand_id,
                'volume': {'name': vol_name,
                           'id': vol_id,
                           'size': vol_size,
                           'status': vol_status},
                'volume_size': vol_size,
                'status': snap_status,
                'mdisk_grp_name': 'mdiskgrp0'}

    def setUp(self):
        super(FlashSystemISCSIDriverTestCase, self).setUp()

        self._def_flags = {'san_ip': 'hostname',
                           'san_login': 'username',
                           'san_password': 'password',
                           'flashsystem_connection_protocol': 'iSCSI',
                           'flashsystem_multipath_enabled': False,
                           'flashsystem_multihostmap_enabled': True,
                           'iscsi_ip_address': '192.168.1.10',
                           'flashsystem_iscsi_portid': 1}

        self.connector = {
            'host': 'flashsystem',
            'wwnns': ['0123456789abcdef', '0123456789abcdeg'],
            'wwpns': ['abcd000000000001', 'abcd000000000002'],
            'initiator': 'iqn.123456'}

        self.sim = FlashSystemManagementSimulator()
        self.driver = FlashSystemFakeISCSIDriver(
            configuration=conf.Configuration(None))
        self.driver.set_fake_storage(self.sim)

        self._reset_flags()
        self.ctxt = context.get_admin_context()
        self.driver.do_setup(None)
        self.driver.check_for_setup_error()

        self.sleeppatch = mock.patch('eventlet.greenthread.sleep')
        self.sleeppatch.start()

    def tearDown(self):
        self.sleeppatch.stop()
        super(FlashSystemISCSIDriverTestCase, self).tearDown()

    def test_flashsystem_do_setup(self):
        # case 1: set as iSCSI
        self.sim.set_protocol('iSCSI')
        self._set_flag('flashsystem_connection_protocol', 'iSCSI')
        self.driver.do_setup(None)
        self.assertEqual('iSCSI', self.driver._protocol)

        # clear environment
        self.sim.set_protocol('iSCSI')
        self._reset_flags()

    def test_flashsystem_validate_connector(self):
        conn_neither = {'host': 'host'}
        conn_iscsi = {'host': 'host', 'initiator': 'foo'}
        conn_both = {'host': 'host', 'initiator': 'foo', 'wwpns': 'bar'}

        protocol = self.driver._protocol

        # case 1: when protocol is iSCSI
        self.driver._protocol = 'iSCSI'
        self.driver.validate_connector(conn_iscsi)
        self.driver.validate_connector(conn_both)
        self.assertRaises(exception.InvalidConnectorException,
                          self.driver.validate_connector, conn_neither)

        # clear environment
        self.driver._protocol = protocol

    def test_flashsystem_connection(self):
        # case 1: initialize_connection/terminate_connection with iSCSI
        self.sim.set_protocol('iSCSI')
        self._set_flag('flashsystem_connection_protocol', 'iSCSI')
        self.driver.do_setup(None)
        vol1 = self._generate_vol_info(None)
        self.driver.create_volume(vol1)
        self.driver.initialize_connection(vol1, self.connector)
        self.driver.terminate_connection(vol1, self.connector)

        # clear environment
        self.driver.delete_volume(vol1)
        self.sim.set_protocol('iSCSI')
        self._reset_flags()

    def test_flashsystem_create_host(self):
        # case 1: create host with iqn
        self.sim.set_protocol('iSCSI')
        self._set_flag('flashsystem_connection_protocol', 'iSCSI')
        self.driver.do_setup(None)
        conn = {
            'host': 'flashsystem',
            'wwnns': ['0123456789abcdef', '0123456789abcdeg'],
            'wwpns': ['abcd000000000001', 'abcd000000000002'],
            'initiator': 'iqn.123456'}
        host = self.driver._create_host(conn)

        # case 2: delete host
        self.driver._delete_host(host)

        # clear environment
        self.sim.set_protocol('iSCSI')
        self._reset_flags()

    def test_flashsystem_get_vdisk_params(self):
        # case 1: use default params
        self.driver._get_vdisk_params(None)

        # case 2: use extra params from type
        opts1 = {'storage_protocol': 'iSCSI'}
        opts2 = {'capabilities:storage_protocol': 'iSCSI'}
        opts3 = {'storage_protocol': 'FC'}
        type1 = volume_types.create(self.ctxt, 'opts1', opts1)
        type2 = volume_types.create(self.ctxt, 'opts2', opts2)
        type3 = volume_types.create(self.ctxt, 'opts3', opts3)
        self.assertEqual(
            'iSCSI',
            self.driver._get_vdisk_params(type1['id'])['protocol'])
        self.assertEqual(
            'iSCSI',
            self.driver._get_vdisk_params(type2['id'])['protocol'])
        self.assertRaises(exception.InvalidInput,
                          self.driver._get_vdisk_params,
                          type3['id'])

        # clear environment
        volume_types.destroy(self.ctxt, type1['id'])
        volume_types.destroy(self.ctxt, type2['id'])
        volume_types.destroy(self.ctxt, type3['id'])

    def test_flashsystem_map_vdisk_to_host(self):
        # case 1: no host found
        vol1 = self._generate_vol_info(None)
        self.driver.create_volume(vol1)
        self.assertEqual(
            # lun id shoud begin with 1
            1,
            self.driver._map_vdisk_to_host(vol1['name'], self.connector))

        # case 2: host already exists
        vol2 = self._generate_vol_info(None)
        self.driver.create_volume(vol2)
        self.assertEqual(
            # lun id shoud be sequential
            2,
            self.driver._map_vdisk_to_host(vol2['name'], self.connector))

        # case 3: test if already mapped
        self.assertEqual(
            1,
            self.driver._map_vdisk_to_host(vol1['name'], self.connector))

        # clean environment
        self.driver._unmap_vdisk_from_host(vol1['name'], self.connector)
        self.driver._unmap_vdisk_from_host(vol2['name'], self.connector)
        self.driver.delete_volume(vol1)
        self.driver.delete_volume(vol2)

        # case 4: If there is no vdisk mapped to host, host should be removed
        self.assertEqual(
            None,
            self.driver._get_host_from_connector(self.connector))

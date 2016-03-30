# -*- encoding: utf-8 -*-

from ceilometer import publisher
from ceilometer.openstack.common import log
try:
    from oslo.utils import netutils as network_utils
except ImportError:
    from ceilometer.openstack.common import network_utils

try:
    from ceilometer.openstack.common.gettextutils import _
except ImportError:
    from ceilometer.i18n import _

import novaclient
from oslo_config import cfg
from novaclient import client as nova_client

import re
import socket
import time
import os
import urllib2, json

METRIC_KEYS = (
    'current_workload',
    'disk_available_least',
    'local_gb',
    'local_gb_used',
    'memory_mb',
    'memory_mb_used',
    'running_vms',
    'vcpus',
    'vcpus_used',
)

LOG = log.getLogger(__name__)

class ESPublisher(publisher.PublisherBase):

    def __init__(self, parsed_url):
        self.host, self.port = network_utils.parse_host_port(parsed_url.netloc,default_port=9200)
        self.hostnode = socket.gethostname().split('.')[0]
        conf = cfg.CONF.service_credentials
        self.conf = conf
        tenant = conf.os_tenant_id or conf.os_tenant_name
        self.nc = nova_client.Client(
            version=2,
            username=conf.os_username,
            api_key=conf.os_password,
            project_id=tenant,
            auth_url=conf.os_auth_url,
            region_name=conf.os_region_name,
            endpoint_type=conf.os_endpoint_type,
            service_type=cfg.CONF.service_types.nova,
            cacert=conf.os_cacert,
            insecure=conf.insecure,
            timeout=cfg.CONF.http_timeout,
            http_log_debug=cfg.CONF.nova_http_log_debug,
            no_cache=True)

    def output_metric(self, name, value):
        print '{}\t{}\t{}'.format(name, value, int(time.time()))


    def ESPush(self, metric):
        LOG.debug("Sending ElasticSearch metric:" + str(metric))

        url = 'http://%s:%s/metrics/metric/_bulk' % (self.host, self.port)

        LOG.debug(url)

        encoded_data = "\n".join(map(json.dumps, metric)) + "\n"
        req = urllib2.Request(url,data=encoded_data)
        f = urllib2.urlopen(req)

        LOG.debug(f.read())

    def publish_samples(self, context, samples):
        for sample in samples:

            stats_time = time.time()

            region = self.conf.os_region_name

            msg = sample.as_dict()

            resource_id = msg['resource_id']
            project_id = msg['project_id']
            data_type = msg['type']
            volume = msg['volume']
            metric_name = msg['name']
            metadata = msg['resource_metadata']

            instance_match = re.match('instance', metric_name)
            network_match = re.match('network', metric_name)
            disk_match = re.match('disk', metric_name)

            hypervisors = self.nc.hypervisors.list()

            data0 = []

            for hv in hypervisors:
               for key, value in hv.to_dict().iteritems():
                   if key in METRIC_KEYS and hv.hypervisor_hostname == self.hostnode:
                          data0.append({ "index" : { } })
                          data0.append({"region" : region, "host" : self.hostnode, "project_id": project_id, key:value, "stats_time":stats_time})
                          #LOG.debug(data0)
                          self.ESPush(data0)

            if disk_match:
                ram = metadata['memory_mb']
                vcpus = metadata['vcpus']
                disk_gb = metadata['disk_gb']
                vmid = metadata.get('instance_id')

            if network_match:
                vmid = metadata.get('instance_id')
                vm = vmid

            else:
                vm = resource_id

            if disk_match:

               data1 = [
                   { "index" : { } },
                   { "region" : region, "host" : self.hostnode, "project_id": project_id, "instance_id": vm, "ram": ram, "stats_time":stats_time },
                   { "index" : { } },
                   { "region" : region, "host" : self.hostnode, "project_id": project_id, "instance_id": vm, "cpu_count": vcpus, "stats_time":stats_time },
                   { "index" : { } },
                   { "region" : region, "host" : self.hostnode, "project_id": project_id, "instance_id": vm, "disk_space": disk_gb, "stats_time":stats_time },
               ]

               self.ESPush(data1)

            if data_type == 'gauge' and instance_match is None:

               data2 = [
                   { "index" : { } },
                   { "region" : region, "host" : self.hostnode, "project_id": project_id, "instance_id": vm, "metric_name": metric_name, "volume": volume, "stats_time": stats_time },
               ]
               self.ESPush(data2)

            else:
                LOG.debug(_("[-]"))

            try:
                LOG.debug(_("OK"))

            except Exception as e:
                LOG.warn(_("Unable to send to ElasticSearch"))
                LOG.exception(e)

    def publish_events(self, context, events):
        """Send an event message for publishing

        :param context: Execution context from the service or RPC call
        :param events: events from pipeline after transformation
        """
        raise ceilometer.NotImplementedError

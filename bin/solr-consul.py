#! /usr/bin/python

import os
import json
import time
import socket
import urllib2
import argparse
import subprocess

from consulfailover import start_handler, log


class Solr(object):
    """Manage Solr"""

    def __init__(self, port, base_uri, base_dir, restart_timeout=300, restart_flag_file='/var/tmp/solr_restart.txt'):

        self.port = port
        self.base_uri = base_uri
        self.base_dir = base_dir
        self.restart_timeout = restart_timeout
        self.restart_flag_file = restart_flag_file

    def flag_restart(self):
        """Flag the time of a master restart"""

        with open(self.restart_flag_file, 'w') as f:
            f.write(str(time.time()))

    def is_restarting(self):
        """Check the timestamp of a restart to see if this host is still allowed
           to be restarting
        """

        if not os.path.exists(self.restart_flag_file):
            return False

        with open(self.restart_flag_file, 'r') as f:
            restart_time = f.readline()

        try:
            restart_time = float(restart_time)
        except ValueError:
            log('Invalid timestamp in {}: {}'.format(self.restart_flag_file, restart_time))
            os.remove(self.restart_flag_file)
            return False

        if time.time() - restart_time < self.restart_timeout:
            return True

        os.remove(self.restart_flag_file)
        return False

    def health(self):
        """Wrapper for returning Solr health in order to return a false positive
           while a Solr master is restarting. health() is called by the Consul
           health check, and get_health is called directly by this class while
           waiting for Solr to restart."""

        # Give a false positive while restarting so we don't lose the consul lock
        if self.is_master() and self.is_restarting():
            return True, 'Master service is restarting'

        return self.get_health()

    def get_health(self):
        """Check Solr health"""

        url = 'http://localhost:{}/{}/admin/cores?action=STATUS&wt=json'.format(self.port, self.base_uri)

        try:
            req = urllib2.urlopen(url, timeout=5)
        except Exception as e:
            return False, 'Unable to connect to Solr API: {}'.format(e)

        status = json.loads(req.read())

        if not status:
            return False, 'Solr API returned empty status'

        if not status.get('status'):
            return False, 'Unable to get core status from Solr API'

        cores = status['status'].keys()

        if not cores:
            return False, 'No cores found'

        for core in cores:

            if not status['status'][core].get('name') or not status['status'][core].get('uptime'):
                return False, 'Health check failed for core {}'.format(core)

        return True, 'Solr operating with {} cores'.format(len(cores))

    def control_solr(self, want_state):
        """Control tomcat using the init.d script"""

        init_arg = {'up': 'start', 'down': 'stop'}.get(want_state)
        args = ['/etc/init.d/tomcat7-solr', init_arg]
        log('Bringing solr {0}'.format(want_state))

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = p.communicate()

        if p.returncode != 0:
            raise Exception('Error running {0}: {1}'.format(' '.join(args), res))

        return self.wait_solr(want_state)

    def wait_solr(self, want_state):
        """Wait for Solr to reach a given state (up or down)"""

        max_time = time.time() + self.restart_timeout
        last_notify = time.time()

        while time.time() < max_time:
            health_ok, health_text = self.get_health()

            if health_ok and want_state == 'up':
                return True
            elif not health_ok and want_state == 'down':
                return True

            if (time.time() - last_notify) > 30:
                last_notify = time.time()
                time_left = int(5 * round(max_time - time.time()) / 5)
                log('Will wait up to {} more seconds for Solr to restart'.format(time_left))

            time.sleep(2)

        log('Solr did not come {} within {} seconds'.format(want_state, self.restart_timeout))
        return False

    def _get_properties_config(self, config_type):
        """Returns the core properties config for either master or slave"""

        if config_type == 'master':
            return 'enable.master=true\nenable.slave=false\n'
        elif config_type == 'slave':
            return 'enable.master=false\nenable.slave=true\n'
        else:
            raise Exception('set_properties: Invalid properties type: {0}'.format(config_type), tag=hostname)

    def _get_properties_files(self):
        """Returns the list of core.properties files found under self.base_dir"""

        core_dirs = [os.path.join(self.base_dir, x) for x in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, x))]
        properties_files = [os.path.join(x, 'core.properties') for x in core_dirs if os.path.isfile(os.path.join(x, 'core.properties'))]

        if not properties_files:
            raise Exception('No core.properites files found under {}'.format(self.base_dir))

        return properties_files

    def set_properties(self, config_type):
        """Set core.properties to enable either master or slave"""

        properties_line = self._get_properties_config(config_type)
        properties_files = self._get_properties_files()

        for properties_file in properties_files:

            with open(properties_file, 'w') as f:
                f.write(properties_line)

        return True

    def _check_core_config(self, config_type):
        """Read the core.properties files to determine whether or not this host is master or slave"""

        properties_line = self._get_properties_config(config_type)
        properties_files = self._get_properties_files()

        for properties_file in properties_files:

            with open(properties_file, 'r') as f:
                current_config = f.read()

                if current_config != properties_line:
                    return False

        return True

    def is_master(self):
        """Check whether this host is master"""

        return self._check_core_config('master')

    def is_slave(self):
        """Check whether this host is a slave"""

        return self._check_core_config('slave')

    def ensure_master(self):
        """Ensure this host is configured as the master"""

        if self.is_master():
            return False

        log('Becoming master')
        self.flag_restart()

        if self.control_solr('down') and self.set_properties('master') and self.control_solr('up'):
            log('Master restarted successfully')
            return True

        log('Master failed to restart!')
        return False

    def ensure_slave(self, master_host=None):
        """Ensure this host is configured as a slave"""

        if self.is_slave():
            return

        log('Becoming a slave')

        if self.control_solr('down') and self.set_properties('slave') and self.control_solr('up'):
            log('Slave restarted successfully')
            return True

        log('Slave failed to restart!')
        return False


def parse():
    """Parse command line"""

    parser = argparse.ArgumentParser()

    parser.add_argument('-a', '--api-port', type=int, default=8000, help='HTTP port for API server (default: %(default)s)')
    parser.add_argument('-c', '--cluster-name', default=socket.gethostname().rstrip('0123456789'), help='Name of this cluster (default: %(default)s)')
    parser.add_argument('-p', '--port', type=int, default=8080, help='Solr API port (default: %(default)s)')
    parser.add_argument('-u', '--base-uri', default='/solr', help='Solr API path prefix (default: %(default)s)')
    parser.add_argument('-b', '--base-dir', default='/var/lib/tomcat7multi/solr/solr', help='Base directory for Solr cores (default: %(default)s)')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse()
    solr_args = [args.port, args.base_uri, args.base_dir]
    start_handler(Solr, solr_args, args.port, args.api_port, args.cluster_name)

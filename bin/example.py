#! /usr/bin/python

# Example of a handler for a fake application

import os

from consulfailover import start_handler


class Exampleapp(object):
    """Handle a fake application"""

    def __init__(self, service_flag):
        self.service_flag = service_flag
        self.master_host = None

    def health(self):
        """Report health to consul-failover API"""

        if os.path.exists(self.service_flag):
            return True, 'Service flag {} exists'.format(self.service_flag)

        return False, 'Service flag {} does not exist'.format(self.service_flag)

    def ensure_master(self):
        """Make sure this host is the master"""

        if self.master_host:
            self.master_host = None
            return True

    def ensure_slave(self, master_host):
        """Make sure this host is a slave to master_host"""

        if not self.master_host:
            self.master_host = None
            return True


if __name__ == '__main__':
    args = ['/var/tmp/in_service']
    start_handler(apphandler_class=Exampleapp, apphandler_args=args, application_port=8080, api_port=8000)

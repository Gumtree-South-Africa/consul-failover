import os
import sys
import json
import time
import consul
import signal
import socket
import logging
import threading
import socketserver as SocketServer
from http import server as SimpleHTTPServer

from consul.base import ConsulException


class TCPServer(SocketServer.TCPServer):
    """Allow port to be reused if it's still in TIME_WAIT"""

    allow_reuse_address = True

    def finish_request(self, request, client_address):
        """Override finish_request in order to ignore client disconnects"""

        try:
            self.RequestHandlerClass(request, client_address, self)
        except IOError:
            pass


class HTTPHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    """API server"""

    def log_message(self, *args):
        """Disable access logging"""

        pass

    def do_GET(self):
        """Handle GET requests"""

        args = self.path.split('/')[1:]

        if not args:
            self.send_error('No command specified')
            return False

        if args[0] == 'health':
            state_ok, state_text = self.server.apphandler.health()
        else:
            state_ok, state_text = False, 'Unsupported endpoint'

        status_code = {True: 200}.get(state_ok, 500)

        try:
            response_text = json.dumps(state_text) + '\n'
        except:
            response_text = str(state_text) + '\n'

        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(response_text.encode())


class ConsulHandler(object):

    def __init__(self, apphandler_class, apphandler_args, cluster_name, api_port, application_port, check_interval='30s', disable_flag_file='/var/tmp/consul_failover_disable'):

        self.apphandler = apphandler_class(*apphandler_args)
        self.cluster_name = cluster_name
        self.api_port = api_port
        self.application_port = application_port
        self.logger = logging.getLogger('ConsulFailover')
        self.consul = consul.Consul()
        self.health_check = consul.Check.http('http://127.0.0.1:{}/health'.format(self.api_port), check_interval)
        self.disable_flag_file = disable_flag_file

    def get_existing_session(self):
        """Return an existing Consul session if there is one"""

        existing_sessions = self.consul.session.node(socket.gethostname(), consistency='consistent')[1]
        leader_sessions = [x for x in existing_sessions if x['Name'] == self.cluster_name]

        if leader_sessions and len(leader_sessions) != 1:
            raise Exception('Multiple {} leader sessions found'.format(self.cluster_name))

        if leader_sessions:
            return leader_sessions[0]['ID']

    def get_session(self):
        """Get an existing session, or create one if one does not exist"""

        # Check for an existing session
        session = self.get_existing_session()

        if session:
            return session

        session_checks = ['serfHealth', 'service:{}'.format(self.cluster_name)]

        while True:

            try:
                return self.consul.session.create(name=self.cluster_name, checks=session_checks, lock_delay=1)
            except ConsulException as e:
                self.logger.info('Error creating session: {}'.format(e))
                time.sleep(2)

    def register(self):
        """Register service in consul"""

        services = self.consul.agent.services()

        # Take no action the service is already registered and has the correct tag
        if self.cluster_name in services:
            return

        self.logger.info('Registering service in Consul')
        self.consul.agent.service.register(self.cluster_name, port=self.application_port, check=self.health_check)
        # Give the Consul agent time to find our registration
        time.sleep(1)

    def deregister(self):
        """Deregister a service in Consul"""

        services = self.consul.agent.services()

        # Take no action if the service is not currently registered
        if not self.cluster_name in services:
            return

        session = self.get_existing_session()

        if session:
            self.logger.info('Destroying leader session')
            self.consul.session.destroy(session)

        self.logger.info('Deregistering service in Consul')
        return self.consul.agent.service.deregister(self.cluster_name)

    def set_tag(self, tag):
        """Register a service in Consul with a tag"""

        services = self.consul.agent.services()

        # Take no action if the service is already registered and has the correct tag
        if self.cluster_name in services and services[self.cluster_name]['Tags'] == [tag]:
            return

        self.logger.info('Updating tag to {}'.format(tag))
        self.consul.agent.service.register(self.cluster_name, port=self.application_port, check=self.health_check, tags=[tag])

        return True

    def get_tag(self):
        """Get the tag that is currently set for this service in Consul"""

        services = self.consul.catalog.service(self.cluster_name)[1]
        my_services = [x for x in services if x['Node'] == socket.gethostname()]

        if my_services and my_services[0]['ServiceTags']:
            return my_services[0]['ServiceTags'][0]

    def get_leader(self):
        """Determine the cluster leader"""

        leader_lock = self.consul.kv.get('lock/{}/leader'.format(self.cluster_name))
        leader_session = leader_lock[1].get('Session')

        if not leader_session:
            return

        try:
            return self.consul.session.info(leader_session)[1]['Node']
        except:
            raise Exception('{} is leader-locked by invalid session ID: {}'.format(self.cluster_name, leader_session))

    def is_healthy(self):
        """Get the health of this service as reported by Consul"""

        checks = self.consul.agent.checks()

        if not checks:
            self.logger.info('Consul agent does not have any health checks')
            return False

        check = checks.get('service:{}'.format(self.cluster_name))

        if not check:
            self.logger.info('Consul agent does not have a health check for service "{}"'.format(self.cluster_name))
            return False

        # A check state of 'passing' is True, anything else is False
        consul_status = check.get('Status')
        check_state = {'passing': True}.get(consul_status, False)
        return check_state

    def monitor(self):
        """Monitor service and report status to Consul"""

        signal.signal(signal.SIGINT, self.graceful_exit)
        signal.signal(signal.SIGTERM, self.graceful_exit)
        self.register()
        last_health = None

        while True:
            time.sleep(2)

            is_healthy = self.is_healthy()

            # Log health state changes for clarity
            if is_healthy != last_health:
                last_health = is_healthy
                health_text = {True: 'healthy'}.get(is_healthy, 'not healthy')
                self.logger.info('Service is {}'.format(health_text))

            if not is_healthy:
                self.set_tag('unhealthy')
                continue

            if os.path.exists(self.disable_flag_file):

                if not self.get_tag() == 'disabled':
                    self.logger.info('Disabling service because {} exists'.format(self.disable_flag_file))

                self.set_tag('disabled')
                continue

            # Attempt to lock, and become the master if it works
            if self.consul.kv.put('lock/{}/leader'.format(self.cluster_name), value='', acquire=self.get_session()):
                self.set_tag('master')
                self.apphandler.ensure_master()
            # Otherwise become a slave to the master
            else:
                leader = self.get_leader()

                if leader:
                    self.set_tag('slave')
                    self.apphandler.ensure_slave(leader)
                else:
                    self.logger.info('Unable to lock and unable to determine leader, retrying...')

    def graceful_exit(self, signum=None, frame=None):
        self.deregister()
        sys.exit(0)


def start_handler(apphandler_class, apphandler_args, application_port, api_port, cluster_name=socket.gethostname().rstrip('0123456789'), log_level='INFO', check_interval='30s'):
    """Set up an application for Consul failover"""

    logger = logging.getLogger('ConsulFailover')
    logger.setLevel(log_level.upper())
    loghandler = logging.StreamHandler()
    logformatter = logging.Formatter(fmt='%(asctime)s [{}] %(message)s'.format(cluster_name), datefmt='%Y-%m-%d %H:%M:%S')
    loghandler.setFormatter(logformatter)
    logger.addHandler(loghandler)

    consulhandler = ConsulHandler(apphandler_class, apphandler_args, cluster_name, api_port, application_port, check_interval)

    # Start API server
    apiserver = TCPServer(('', api_port), HTTPHandler)
    apiserver.apphandler = apphandler_class(*apphandler_args)
    thread = threading.Thread(target=apiserver.serve_forever)
    thread.daemon = True
    thread.start()
    logger.info('API server listening on port {0}'.format(api_port))

    # Start monitoring the service
    consulhandler.monitor()

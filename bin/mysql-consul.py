#! /usr/bin/python

import time
import consul
import socket
import MySQLdb
import MySQLdb.cursors
import logging
import argparse
import warnings
import threading

from consulfailover import start_handler


class Mysql(object):
    """Manage MySQL"""

    def __init__(self, port, defaults_file, replication_user, replication_password, require_databases):
        self.port = port
        self.defaults_file = defaults_file
        self.replication_user = replication_user
        self.replication_password = replication_password
        self.require_databases = require_databases
        self.consul = consul.Consul()
        self.logger = logging.getLogger('ConsulFailover')
        self.connect_lock = threading.Lock()
        self.query_lock = threading.Lock()
        self.conn = None
        self.connect()

    def connect(self):
        """Connect or get existing connection to MySQL"""

        if not self.connect_lock.acquire(False):
            self.logger.info('Failed to get connect lock!')
            return False

        try:
            self.conn = MySQLdb.connect(read_default_file=self.defaults_file, cursorclass=MySQLdb.cursors.DictCursor)
        except Exception as e:
            self.logger.info('Error connecting to MySQL: {}'.format(e))
            self.conn = None
            return False
        finally:
            self.connect_lock.release()

        self.logger.info('Connected to MySQL on port {}'.format(self.port))
        return True

    def query(self, query):
        """Perform a MySQL query"""

        # Attempt to reconnect to MySQL if there is no connection
        if not self.conn and not self.connect():
            return {}

        if not self.query_lock.acquire():
            self.logger.info('Failed to get query lock!')
            return {}

        if query.startswith('CHANGE MASTER TO') or query.startswith('STOP SLAVE'):
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
        # Attempt to reconnect to MySQL if the connection has been lost
        except MySQLdb.OperationalError as e:
            self.logger.info('Connection failed during query: {}'.format(e))
            self.connect()
            return {}
        except Exception as e:
            self.logger.info('Query failed: "{}": {}'.format(query, e))
            return {}
        # Make sure to release the connection if we got one
        finally:
            self.query_lock.release()

        res = cursor.fetchall()

        if res and len(res) == 1:
            return res[0]
        elif res:
            return res

        return {}

    def get_variable(self, variable):
        """Get a MySQL variable and return the value"""

        res = self.query('SELECT @@{}'.format(variable))

        return res.get('@@{}'.format(variable))

    def post_transactions(self):
        """Post the list of executed transactions to Consul"""

        res = self.query('SELECT @@GLOBAL.GTID_EXECUTED AS transactions')

        if not res.get('transactions'):
            self.logger.debug('No transactions listed in GTID_EXECUTED')
            return

        transactions = res['transactions'].replace('\n', '')
        self.logger.debug('Posting transactions to Consul: {}'.format(transactions))
        self.consul.kv.put('mysql/{}/transactions'.format(socket.gethostname()), value=transactions)

    def get_leader_transactions(self, master_host):
        """Get the transactions that have been executed on the master"""

        res = self.consul.kv.get('mysql/{}/transactions'.format(master_host))

        if not res or not res[1]:
            return

        return res[1].get('Value')

    def master_is_ahead(self, master_host):
        """Test whether the master has transactions that this slave hasn't executed yet"""

        # Gossip pause
        time.sleep(1)
        master_transactions = self.get_leader_transactions(master_host)

        if not master_transactions:
            return False

        res = self.query('SELECT GTID_SUBTRACT("{}", @@GLOBAL.GTID_EXECUTED) as leftovers'.format(master_transactions))

        if res and res.get('leftovers'):
            return True

        return False

    def health(self):
        """Return MySQL server health status"""

        res = self.query('SHOW DATABASES')

        try:
            databases = [x['Database'] for x in res]
        except:
            return False, 'Error running SHOW DATABASES: {}'.format(res)

        if not databases:
            return False, 'SHOW DATABASES query failed'

        missing_databases = set(self.require_databases) - set(databases)

        if missing_databases:
            return False, 'The following databases are missing on this server: {}'.format(', '.join(missing_databases))

        return True, 'MySQL serving required databases: {}'.format(', '.join(self.require_databases))

    def ensure_master(self):
        """Make sure this host is configured as the master"""

        self.post_transactions()
        slave_status = self.query('SHOW SLAVE STATUS')

        # Make sure the master is read-write
        if self.get_variable('read_only') != 0:
            self.logger.info('Setting read_only to off')
            self.query('SET GLOBAL read_only = 0')

        # Stop slave threads once we are caught up to the old master
        if slave_status:

            # Don't stop slave if the old master is ahead and still alive
            if slave_status.get('Master_Host') and slave_status['Master_Host'] != socket.gethostname() and self.master_is_ahead(slave_status['Master_Host']):
                self.logger.info('{} is still ahead, waiting to catch up...'.format(slave_status['Master_Host']))
                return

            self.logger.info('Stopping slave threads')
            self.query('STOP SLAVE')
            self.query('RESET SLAVE ALL')

    def ensure_slave(self, master_host):
        """Make sure this host is configured as a slave"""

        slave_status = self.query('SHOW SLAVE STATUS')

        if not type(slave_status) == dict:
            self.logger.info('Error getting slave status: {}'.format(slave_status))
            return False

        # Do a master switch if we are not yet slaving from master_host
        if not slave_status or slave_status.get('Master_Host') != master_host:
            self.logger.info('Becoming a slave to {}'.format(master_host))
            self.query('FLUSH LOCAL TABLES WITH READ LOCK')
            self.query('SET GLOBAL read_only=1')
            self.query('UNLOCK TABLES')
            self.query('STOP SLAVE')
            self.query('RESET SLAVE ALL')
            self.query('CHANGE MASTER TO MASTER_HOST="{}", MASTER_PORT={}, MASTER_USER="{}", MASTER_PASSWORD="{}", MASTER_AUTO_POSITION=1'.format(master_host, self.port, self.replication_user, self.replication_password))
            self.query('START SLAVE')
            return True

        # Try restarting slave threads if they are not running
        if slave_status.get('Slave_IO_Running') != 'Yes' or slave_status.get('Slave_SQL_Running') != 'Yes':
            self.logger.info('Slave threads are not running, trying to restart them')
            self.query('STOP SLAVE')
            self.query('START SLAVE')

        # Make sure the slave is read-only
        if self.get_variable('read_only') != 1:
            self.logger.info('Setting host read-only')
            self.query('SET GLOBAL read_only = 1')


def parse():
    """Parse command line"""

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--api-port', type=int, default=8000, help='HTTP port for API server (default: %(default)s)')
    parser.add_argument('-c', '--cluster-name', default=socket.gethostname().rstrip('0123456789'), help='Name of this cluster (default: %(default)s)')
    parser.add_argument('-P', '--port', type=int, default=3306, help='MySQL port (default: %(default)s)')
    parser.add_argument('-d', '--require-databases', nargs='*', default=['mysql'], help='Health check requires these databases to be available (default: %(default)s)')
    parser.add_argument('-f', '--defaults-file', default='/etc/mysql/consul.cnf', help='Auth file for MySQL connections')
    parser.add_argument('-e', '--replication-user', default='replication', help='Username for replication')
    parser.add_argument('-r', '--replication-password', required=True, help='Password for replication')
    parser.add_argument('-l', '--log-level', default='INFO', help='Output level (default: %(default)s)')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse()
    mysql_args = [args.port, args.defaults_file, args.replication_user, args.replication_password, args.require_databases]
    start_handler(Mysql, mysql_args, args.port, args.api_port, args.cluster_name, args.log_level)

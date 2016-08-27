# Consul Failover
## Synopsis
Automatic failover for old-school master/slave applications.
## Motivation
Provide simple and automated failover for master/slave applications in the cloud.
##Design
1. An application handler class manages the application's state in the cluster by providing three functions:
  * Verify the application's health.
  * Make the application become the cluster master.
  * Make the application become a slave to another host.
2. The application handler is passed to a Consul handler.
3.  The Consul handler makes the application handler's health check available via an HTTP interface.
4. The Consul handler registers the application in Consul as a service.
5. The Consul handler enters a loop, taking the following actions every two seconds:
  * Attempt to [acquire a lock in Consul](https://www.consul.io/docs/guides/leader-election.html).
  * If the lock is acquired, the application handler's ensure_master() method is called.
  * If the lock cannot be acquired, the host that holds the lock is determined and passed to the application handler's ensure_slave() method.
## Usage example
Start the agent:
```
root@solr001:~# solr-consul.py
2016-09-02 19:13:09 [solr] API server listening on port 8000
2016-09-02 19:13:09 [solr] Registering service in Consul
2016-09-02 19:13:12 [solr] Updating tag to master
```
Test the health check:
```
pmcconnell@solr001:~$ curl http://localhost:8000/health
"Solr operating with 3 cores"
```
Check service state in Consul:
```
pmcconnell@solr001:~$ curl -s http://consul001:8500/v1/catalog/service/solr | python -m json.tool
[
    {
        "Address": "10.41.178.59",
        "CreateIndex": 2027783,
        "ModifyIndex": 2027791,
        "Node": "solr001",
        "ServiceAddress": "",
        "ServiceEnableTagOverride": false,
        "ServiceID": "solr",
        "ServiceName": "solr",
        "ServicePort": 8080,
        "ServiceTags": [
            "master"
        ]
    }
]
```
Test Consul's DNS interface:
```
pmcconnell@shellserver001:~$ ping master.solr.service.consul
PING master.solr.service.consul (10.41.178.59) 56(84) bytes of data.
64 bytes from solr001.ams1.ops.pp.bt.ecg.so (10.41.178.59): icmp_seq=1 ttl=62 time=2.64 ms
[...]
```
Access the application running on the cluster master:
```
pmcconnell@shellserver001:~$ curl http://master.solr.service.consul:8080/solr/admin/cores
<?xml version="1.0" encoding="UTF-8"?>
<response>
[...]
```
## Maintenance or controlled failovers
* Stopping the application on a node will cause the health check to fail, which will cause Consul to remove that node from the application service.
* Stopping the consul-failover agent on a node will cause the node to be immediately deregistered from the service in Consul.

If the node was tagged as the master for its service in Consul, either of the above actions will cause it to give up the lock in Consul, and the remaining nodes will elect a new leader.
## Adding support for other applications
Create a Python class to manage your application and pass it to ConsulHandler. The new class needs to provide three methods which will be called repeatedly as ConsulHandler monitors both the application and its state in Consul:
* health(): Determine whether the application is healthy and return a tuple consisting of:
  * A boolean that states whether or not the application is healthy.
  * A string whose contents will be displayed in the output of the HTTP check.
* ensure_master(): Determine whether or not this host is currently the master. If so, take no action; if not, make it the master.
* ensure_slave(master_host): Determine whether or not this host is currently a slave to master_host. If so, take no action; if not, make it a slave to master_host.

See [bin/example.py](bin/example.py) for an example.
## API reference
The health check API has only one endpoint: /health. This endpoint calls the health() method of the application handler class and expects a tuple containing a boolean and a string. If the boolean is True, the HTTP response will be 200; if the boolean is False, the HTTP response will be 500. The contents of the string will be shown in the body of the response.
## Credits
This is a direct descendent of Yorick's [mysql-leader](https://github.corp.ebay.com/ecg-marktplaats/so-mysql-leader).

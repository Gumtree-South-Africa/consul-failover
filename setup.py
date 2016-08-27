from setuptools import setup


setup(
    name='consul-failover',
    version=0.1,
    author='Patrick McConnell',
    author_email='pmcconnell@ebay.com',
    maintainer='Patrick McConnell',
    maintainer_email='pmcconnell@ebay.com',
    description=('Register services in Consul and handle master/slave failover via Consul leader election'),
    license='GPL-3',
    keywords='python consul mysql solr',
    url='https://github.corp.ebay.com/pmcconnell/consul-failover',
    packages=[
        'consulfailover',
    ],
    install_requires=[
        'python-consul',
        'python-mysqldb',
    ],
    scripts=[
        'bin/mysql-consul.py',
        'bin/solr-consul.py',
    ]
)

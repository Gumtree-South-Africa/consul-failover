from setuptools import setup


setup(
    name='consul-failover',
    version=0.4,
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
        'python3-consul',
        'python3-mysqldb',
    ],
    scripts=[
        'bin/mysql-consul.py',
        'bin/solr-consul.py',
    ]
)

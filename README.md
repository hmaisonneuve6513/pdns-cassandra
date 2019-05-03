pdns-cassandra
==============

Remote backend backend for PowerDNS that talks to Cassandra

Getting started
--------------

Create Cassandra schema with cqlsh version 5.0.1 used and Cassandra 3.11.4:

    CREATE KEYSPACE [your own keyspace name] WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': '3'
    };

    USE [your own keyspace];

    CREATE TABLE domain_metadata (
        name ascii,
        kind ascii,
        content ascii,
        PRIMARY KEY (name, kind)
    );

    CREATE TABLE domains (
        zone ascii,
        kind ascii,
        last_check int,
        masters list<ascii>,
        notified_serial int,
        PRIMARY KEY (zone)
    );

    CREATE TABLE records (
        doamin_id ascii,
        qname ascii,
        auth int,
        content ascii,
        disabled int,
        ordername ascii,
        priority int,
        qtype ascii,
        ttl int,
        PRIMARY KEY (domain_id, qname)
    );

    CREATE TABLE supermasters (
        nameserver ascii,
        ip ascii,
        account ascii,
        PRIMARY KEY (nameserver, ip)
    );

Make sure you have the Cassandra driver from Datastax installed:
<https://github.com/datastax/python-driver>

The recommended way is to just:

    $ pip install cassandra-driver
    
Configure the PowerDNS remote backend:

    launch=remote
    remote-connection-string=http:url=http://localhost:5000
    
Now start the application:

    DEBUG=True CASSANDRA_NODES=10.0.0.1,10.0.0.2 python pdns_cassandra.py

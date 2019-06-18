pdns-cassandra
==============

Remote backend backend for PowerDNS that talks to Cassandra

Getting started
--------------

Create Cassandra schema with cqlsh version 5.0.1 used and Cassandra 3.11.4:

Make sure you have the Cassandra driver from Datastax installed:
<https://github.com/datastax/python-driver>

The recommended way is to just:

    $ pip install cassandra-driver
    

    CREATE KEYSPACE <your own keyspace> WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;
    
    CREATE TABLE <your own keyspace>.cryptokeys (
        domain_id ascii,
        content ascii,
        active int,
        flags int,
        PRIMARY KEY (domain_id, content)
    ) WITH CLUSTERING ORDER BY (content ASC);
    
    CREATE TABLE <your own keyspace>.tsigkeys (
        name ascii PRIMARY KEY,
        algorithm ascii,
        secret ascii
    ) ;
    
    CREATE TABLE <your own keyspace>.records (
        domain_id ascii,
        qname ascii,
        content ascii,
        auth int,
        disabled int,
        ordername ascii,
        priority int,
        qtype ascii,
        ttl int,
        PRIMARY KEY (domain_id, qname, content)
    ) WITH CLUSTERING ORDER BY (qname ASC, content ASC)';
    
    CREATE INDEX record_qname ON osnworld_pdns_backend.records (qname);
    
    CREATE TABLE <your own keyspace>.supermasters (
        nameserver ascii,
        ip ascii,
        account ascii,
        PRIMARY KEY (nameserver, ip)
    ) WITH CLUSTERING ORDER BY (ip ASC);
    
    CREATE TABLE <your own keyspace>.domains (
        zone ascii PRIMARY KEY,
        account ascii,
        kind ascii,
        last_check int,
        masters list<ascii>,
        notified_serial int,
        serial int
    ) ;
    
    CREATE TABLE <your own keyspace>.domain_metadata (
        domain_id ascii,
        kind ascii,
        content list<ascii>,
        PRIMARY KEY (domain_id, kind)
    ) WITH CLUSTERING ORDER BY (kind ASC);
    
    CREATE TABLE <your own keyspace>.transactions_data (
        domain_id ascii,
        id ascii,
        sent ascii,
        state ascii,
        time timestamp,
        PRIMARY KEY (domain_id, id)
    ) WITH CLUSTERING ORDER BY (id ASC);

Configure the PowerDNS remote backend:

    launch=remote
    remote-connection-string=http:url=http://localhost:5000

Now start the application:

    DEBUG=True CASSANDRA_NODES=10.0.0.1,10.0.0.2 python pdns_cassandra.py

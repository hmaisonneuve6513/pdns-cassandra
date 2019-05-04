'''
Cassandra remote backend for PowerDNS
'''

'''
First start of project
__author__ = 'Ruben Kerkhof <ruben@tilaa.com>'
'''
__author__ = 'Herve Maisonneuve <hmaisonneuve6513@gmail.com>'
__license__ = 'MIT'
__version__ = '0.0.1'

import os

import cassandra.cluster
import cassandra.query

from flask import Flask, jsonify, abort, request

app = Flask(__name__)


@app.errorhandler(404)
def return_404(error):
    return jsonify(result=False), 404


def get_or_404(query, *args):
    result = db_session.execute(query, *args)
    if not result:
       abort(404)
    return result


@app.route('/lookup/<qname>/<qtype>')
def lookup(qname, qtype):
    ''' do a basic query '''

    rrset = []
    if qtype == 'ANY':
        rrset = get_or_404(
            'SELECT * FROM records WHERE qname = %s', (qname,)
        )
    else:
        rrset = get_or_404(
            'SELECT * FROM records WHERE qname = %s AND qtype = %s',
            (qname, qtype)
        )
    return jsonify(result=rrset)

@app.route('/getAllDomains')
def get_all_domains():
    ''' get all zones for master server included disabled and slave zones'''
    result = []
    inter = []
    zones = get_or_404(
        'SELECT * FROM domains'
    )
    count = 1
    '''{"result":[{"id":1,"zone":"unit.test.","masters":["10.0.0.1"],"notified_serial":2,"serial":2,"last_check":1464693331,"kind":"native"}]}'''
    for zone in zones:
        inter = dict(
            id = count,
            zone=zone['zone'],
            kind=zone['kind'],
            masters=zone['masters'],
            serial=zone['serial'],
            notified_serial=zone['notified_serial'],
            last_check=zone['last_check'],
        )
        result.append(inter)
        count += count

    return jsonify(result=result)


@app.route('/getDomainMetadata/<name>/<kind>')
def get_domain_metadata(name, kind):
    ''' get metadata for a domain '''
    result = []
    rrset = get_or_404(
        'SELECT content FROM domain_metadata WHERE name = %s and kind = %s',
        (name, kind)
    )
    for rr in rrset:
        result.append(rr['content'])
    return jsonify(result=result)


@app.route('/list/<id>/<qname>')
def list(id,qname):
    ''' retrieve all records frome zone=qname'''

    zone_id = id
    rrset = get_or_404(
        'SELECT * FROM records WHERE qname = %s ALLOW FILTERING', (qname,)
    )
    return jsonify(result=rrset)


@app.route('/getDomainInfo/<zone>')
def get_domain_info(zone):
    ''' get info for a domain '''
    rows = get_or_404(
        'SELECT * FROM domains WHERE zone = %s LIMIT 1', (zone,)
    )
    r = rows[0]
    result = dict(
        zone=r['zone'],
        kind=r['kind'],
        masters=r['masters'],
        id=1,
        serial=1,
        notified_serial=1,
        last_check=0,
    )
    return jsonify(result=result)


@app.route('/superMasterBackend/<ip>/<domain>', methods=['POST'])
def super_master_backend(ip, domain):
    ''' check if we can be a slave for a domain '''
    for key, value in request.form.items(multi=True):
        if 'content' in key:
            rows = db_session.execute(
                '''
                SELECT account from supermasters
                WHERE ip = %s AND nameserver = %s
                ''',
                (ip, value)
            )
            if not rows:
                continue
            #if rows[0]['account'] is None:
                # remotebackend doesn't like json null
            #    return jsonify(result=True)
            return jsonify(result={'account': rows[0]['account']})
    abort(404)


@app.route('/createSlaveDomain/<ip>/<domain>', methods=['PUT'])
def create_slave_domain(ip, domain):
    ''' create a new slave domain '''
    db_session.execute(
        """
        INSERT INTO domains (zone, kind, masters)
        VALUES (%s, 'SLAVE', %s)
        """, (domain, [ip]))
    return jsonify(result=True)


if __name__ == '__main__':
    app.config['HOST'] = os.getenv('HOST', '::1')
    app.config['PORT'] = os.getenv('HOST', 5000)
    app.config['DEBUG'] = os.getenv('DEBUG', False)
    ''' use your own keyspace in place of osnworld_pdns_backend'''
    app.config['KEYSPACE'] = os.getenv('KEYSPACE', 'osnworld_pdns_backend')

    cassandra_nodes = os.getenv('CASSANDRA_NODES')
    if not cassandra_nodes:
    	raise SystemExit("CASSANDRA_NODES is not set")
    app.config['cassandra_nodes'] = cassandra_nodes.split(',')

    cluster = cassandra.cluster.Cluster(app.config['cassandra_nodes'])
    db_session = cluster.connect(app.config['KEYSPACE'])
    db_session.row_factory = cassandra.query.dict_factory
    app.run(host=app.config['HOST'], port=app.config['PORT'])

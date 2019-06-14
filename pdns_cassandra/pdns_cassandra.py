'''
Cassandra HTTP remote backend for PowerDNS use python cassandra driver
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

class rrset(object):

    def ___init__(self,content,sclass,name,type,ttl):
        self.content = content
        self.sclass  = sclass
        self.name = name
        self.type = type
        self.ttl = ttl

    def to_dict(self):
        data = {}
        data['content'] = self.content
        data['sclass'] = self.sclass
        data['name'] = self.name
        data['type'] = self.type
        data['ttl'] = self.ttl
        return data

@app.errorhandler(404)
def return_404(error):
    return jsonify(result=False), 404


def get_or_404(query, *args):
    result = db_session.execute(query, *args)
    print result
    if not result:
       abort(404)
    return result

def get_even_null(query, *args):
    result = db_session.execute(query, *args)
    return result


def command(query,*args):
    result = db_session.execute(query, *args)
    return result

def suppress_rrset_form_header(rrset_tocut):
    result = rrset_tocut[2:]
    return result



def parse_to_rrset(stringtoparse):
    '''rrset_values = []'''
    rrset_inter = []
    rrsets = []

    '''additional = '&2&rrset[1][content]=192.168.123.112&rrset[1][qclass]=1&rrset[1][qname]=ftp.osnworld.net.&rrset[1][qtype]=A&rrset[1][ttl]=3600' '''

    parameters = stringtoparse
    parameters = parameters.split("&")


    rrset_key_values = {}

    for out_rrsets in parameters:
        out_rrsets = out_rrsets.replace("][", "]&&[")
        out_rrsets_m = out_rrsets.split('&&')

        print 'out_rrsets_m:'
        print out_rrsets_m

        index = 0
        stocked_rrset = ''
        current_rrset = ''

        for rrsets_m in out_rrsets_m:

            print 'Split to rrset: ' + rrsets_m
            current_rrset = rrsets_m

            if 'rrset' in rrsets_m:
                print 'We are on rrset object: '+str(rrsets_m)

                if current_rrset == 'rrset['+str(index)+']':
                    print 'continue to parse rrset index: '+ str(index)

                else:
                    print 'add index rrset index: '+ str(index)+''
            else:
                working_str = ''
                working_str = str(rrsets_m)
                print 'We are on key value data: '+working_str
                working_str = working_str.replace('[','')
                working_str = working_str.replace(']','')
                print working_str
                key_value = working_str.split('=')
                print key_value
                rrset_key_values[key_value[0]] = key_value[1]
    return rrset_key_values





@app.route('/initialize')
def initialize():

    ''' do a basic query '''
    result = []

    zones = get_or_404( 'SELECT * FROM  domains ')

    if zones:
        return jsonify(result=True)
    else:
        return jsonify(result=False)






@app.route('/lookup/<qname>/<qtype>')
def lookup(qname, qtype):
    ''' do a basic query '''

    result = []
    rrset = []
    record = []
    if qtype == 'ANY':
        rrset = get_or_404('SELECT qtype, qname, content, ttl FROM records WHERE qname = %s ALLOW FILTERING', (qname,) )
    else:
        rrset = get_or_404('SELECT qtype, qname, content, ttl FROM records WHERE  qname = %s AND qtype = %s ALLOW FILTERING', (qname, qtype,) )

    for record in rrset:
        inter = dict (
            qtype = record['qtype'],
            qname = record['qname'],
            content = record['content'],
            ttl = record['ttl'],
        )
        result.append(inter)

    return jsonify(result=result)





@app.route('/list/<id>/<domain_id>')
def list(id,domain_id):
    ''' retrieve all records from zone=domain_id '''

    zone_id = id
    result = []
    rrset = get_or_404('SELECT qtype , qname , content , ttl FROM records WHERE domain_id = %s ALLOW FILTERING', (domain_id,) )

    for record in rrset:
        result.append(record)

    return jsonify(result=result)





@app.route('/getBeforeAndAfterNamesAbsolute/<id>/<qname>')
def get_before_and_after_names_absolute(id, qname):

    in_parameters = request.get_data()
    print 'in parameters :' + in_parameters

    print 'ID : ' + id
    print 'qname: ' + qname

    if not qname[-1] == '.':
        qname += '.'

    result = {"before":"","after":""}
    reached = False
    passed = False
    domain_id = ''

    fqdn = qname.split('.')
    len = len(fqdn)
    if len >= 2:
        domain_id = fqdn[len-3] + '.'  + fqdn[len-2] + '.'
    else:
        result = {'before':'','after':''}

    print domain_id

    out_qnames = get_or_404('SELECT qname FROM records WHERE domain_id = %s ALLOW FILTERING', (domain_id,) )

    for out_qname in out_qnames:

        if passed:
            result['after'] = out_qname['name']
            passed = False
        else:
            current = out_qname
            if out_qname['name'] == qname:
                reached = True
                passed = True
            elif not reached:
                result['before'] = out_qname['name']



    return jsonify(result)



@app.route('/getAllDomainMetadata/<name>')
def get_all_domain_metadata(name):

    result = []
    metadatas = get_or_404('SELECT content FROM domain_metadata WHERE name = %s ALLOW FILTERING', (name, ) )

    for metadata in metadatas:
        inter = dict(
            content=metadata['content'],
        )
        result.append(inter)
    return jsonify(result=result)



@app.route('/getDomainMetadata/<name>/<kind>')
def get_domain_metadata(name, kind):

    ''' get metadata of this kind for this domain name '''

    result = []
    metadatas = get_or_404('SELECT content FROM domain_metadata WHERE name = %s and kind = %s ALLOW FILTERING', (name, kind, ) )

    for metadata in metadatas:
        inter = dict(
            content = metadata['content'],
        )
        result.append(inter)

    return jsonify(result=result)



@app.route('/setDomainMetadata/<name>/<kind>', methods=['PATCH'])
def set_domain_metadata(name, kind):

    print 'URL information'
    print name
    print kind
    print 'Parameter recuperation'
    in_metadatas = request.get_data()
    print in_metadatas




@app.route('/getAllDomains')
def get_all_domains():

    ''' get all zones for master server included disabled and slave zones'''

    result = []
    zones = get_or_404('SELECT * FROM domains' )

    for zone in zones:
        inter = dict(
            zone=zone['zone'],
            kind=zone['kind'],
            masters=zone['masters'],
            serial=zone['serial'],
            notified_serial=zone['notified_serial'],
            last_check=zone['last_check'],
        )
        result.append(inter)

    return jsonify(result=result)




@app.route('/getDomainInfo/<zone>')
def get_domain_info(zone):
    ''' get info for a domain '''
    rows = get_or_404(
        'SELECT * FROM domains WHERE zone = %s LIMIT 1', (zone,)
    )
    if rows:
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
    else:
        result = 'false'
    return jsonify(result=result)






@app.route('/replaceRRSet/<p_id>/<p_qname>/<p_qtype>', methods=['PATCH'])
def replace_rrset(p_id,p_qname,p_qtype):

    print 'URL information'
    print p_id
    print p_qname
    print p_qtype
    print 'Parameter recuperation'
    '''rrsets = request.args.get()'''
    '''rrsets = request.query_params()'''
    in_rrsets = request.get_data()
    print in_rrsets

    in_rrsets = suppress_rrset_form_header(in_rrsets)
    print 'After header suppression'
    print in_rrsets

    out_rrsets = parse_to_rrset(in_rrsets)
    print 'After parsing parameters'
    print out_rrsets

    rrsets = [out_rrsets]


    for rrset in rrsets:

        print 'List rrset content'
        print rrset['content']
        print rrset['qname']
        print rrset['qtype']
        print rrset['ttl']

        print 'select item to destroy:'
        rows = get_or_404(
            'SELECT * FROM records WHERE  qname = %s ALLOW FILTERING', (rrset['qname'], )
        )
        if rows:
            count = 0
            r = rows[0]
            result = dict(
                domain_id=r['domain_id'],
                qname=r['qname'],
                content=r['content'],
                auth=r['auth'],
                disabled=r['disabled'],
                ordername=r['ordername'],
                priority=r['priority'],
                qtype=r['qtype'],
                ttl=r['ttl'],
            )
            print 'Item found:'
            print r['domain_id']
            print r['qname']
            print r['content']
            print r['auth']
            print r['disabled']
            print r['ordername']
            print r['priority']
            print r['qtype']
            print r['ttl']

            print "New content: " + rrset['content']

            print 'Deleting item:'
            delete = command(
                'DELETE FROM records WHERE domain_id = %s and qname = %s and content = %s', ( r['domain_id'], r['qname'], r['content'], )
            )
            print 'Deleted'

            print 'Inserting new Item:'

            insert = command(
                'INSERT INTO records (domain_id, qname, content, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s )', ( r['domain_id'], r['qname'],rrset['content'], r['qtype'], r['ttl'] )
            )

            count += count
            print count
            return jsonify(result=True), 200
        else:
            return jsonify(result=False), 404
    return 'true'






@app.route('/searchRecords')
def searchRecords():

    print 'parameters data recuperation'
    param_max = request.args.get('maxResults',type=int)
    param_qname = request.args.get('pattern')
    print param_max
    print param_qname
    print ''

    rcname_tab = param_qname.split('.')
    rcname_len = len(rcname_tab)
    print rcname_len

    result = []

    if param_qname.endswith('.',0,1):
        if rcname_len == 3:
            rrset = get_or_404(
                'SELECT domain_id, qname, content, disabled, qtype, ttl FROM records WHERE domain_id = %s LIMIT %s ALLOW FILTERING', (param_qname,param_max,)
            )
        elif rcname_len >3:
            rrset = get_or_404(
                'SELECT domain_id, qname, content, disabled, qtype, ttl FROM records WHERE qname = %s LIMIT %s ALLOW FILTERING', (param_qname,param_max,)
            )

    else:
        rrset = get_or_404(
            'SELECT domain_id, qname, content, disabled, qtype, ttl FROM records WHERE qname = %s LIMIT %s ALLOW FILTERING', (param_qname,param_max,)
        )


    for record in rrset:
        inter = dict (
            content = record['content'],
            disabled = record['disabled'],
            qname = record['qname'],
            zone = record['domain_id'],
            qtype = record['qtype'],
            ttl = record['ttl'],
        )
        result.append(inter)

    return jsonify(result=result)





@app.route('/setnotified', methods=['PATCH'])
def setnotified():

    print 'form data recuperation'
    param_serial = request.form.get('serial')
    print param_serial

    '''
    result = command(
        'INSERT INTO records (domain_id, qname, content, disabled, qtype, ttl ) VALUES ( %s, %s, %s, 0, %s, %s)', (domain_id,param_qname,param_content,param_qtype,param_ttl)
    )
    
    '''

    return 'true'




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





@app.route('/startTransaction/<id>/<domain_id>/<number>', methods=['POST'])
def start_transaction(id, domain_id, number):

    print id
    print domain_id
    print number

    insert = command('INSERT INTO  transactions_data( domain_id, id, state ) VALUES ( %s, %s, %s ) ', (domain_id, number, 'STARTED') )

    print 'start transaction insert result:' + str(insert)

    return jsonify(result=True)



@app.route('/commitTransaction/<number>', methods=['POST'])
def commit_transaction( number ):

    trs = command('SELECT * FROM transactions_data WHERE id = %s ALLOW FILTERING' , (number) )

    founds = []

    for tr in trs:
        inter = dict(
            domain_id=tr['domaine_id'],
            id=tr['id'],
        )
        founds.append(inter)

    for found in founds:
        delete = command('DELETE FROM transactions_data WHERE domain_id = %s and id = %s ', (found['domain_id'], found['id'] ) )

    insert = command('INSERT INTO  transactions_data( domain_id, id, state ) VALUES ( %s, %s, %s ) ', (domain_id, number, 'COMMITED') )

    return jsonify(result=True)


if __name__ == '__main__':
    app.config['HOST'] = os.getenv('HOST', '192.168.123.91')
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

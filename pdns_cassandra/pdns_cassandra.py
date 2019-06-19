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




def parse_to_rrsets(nb_rrsets, in_str):

    rrsets_array = []
    rrsets = []
    count = nb_rrsets
    while count >=1:
        sep_str = '&rrset['+str(count-1)+']'
        if not in_str == '':
            rrsets_array = in_str.split(sep_str)
            in_str = rrsets_array[0]

            rrsets_array_len = len(rrsets_array)
            first_index = rrsets_array_len-5

            if rrsets_array_len >= 5:

                n_rrset = dict(
                    content = (rrsets_array[first_index].split('='))[1],
                    qclass = (rrsets_array[first_index+1].split('='))[1],
                    qname = (rrsets_array[first_index+2].split('='))[1],
                    qtype = (rrsets_array[first_index+3].split('='))[1],
                    ttl = int((rrsets_array[first_index+4].split('='))[1]),
                )

                rrsets.append(n_rrset)

            rrsets_array = rrsets_array.pop(0)

            count -= 1
        else:
            break
    return rrsets





def parse_to_nssets(in_str):

    print 'Parsing to nssets :'+in_str
    count = 0
    index = 0

    in_str = in_str.split('&')
    in_str_len = len(in_str)

    nsset_array = []
    nssets = []
    nsset ={}

    for nsset_property_str in in_str:

        index = count+1
        if 'nsset['+str(count+1)+']' in nsset_property_str:
            nsset_property = nsset_property_str.split('=',1)
            nsset_property[0] = nsset_property[0].replace('nsset['+str(count+1)+'][','')
            nsset_property[0] = nsset_property[0].replace(']','')
            prop = nsset_property[0]
            value = nsset_property[1]
            nsset[prop] = value
        else:
            count += 1
            nssets.append(nsset)
            nsset ={}
            nsset_property = nsset_property_str.split('=',1)
            nsset_property[0] = nsset_property[0].replace('nsset['+str(count+1)+'][','')
            nsset_property[0] = nsset_property[0].replace(']','')
            prop = nsset_property[0]
            value = nsset_property[1]
            nsset[prop] = value

    nssets.append(nsset)

    return nssets

def parse_to_rr(in_str):

    print in_str

    in_str = in_str.split('&')
    rrset ={}

    for rrset_property_str in in_str:

        rrset_property = rrset_property_str.split('=',1)
        rrset_property[0] = rrset_property[0].replace('rr[','')
        rrset_property[0] = rrset_property[0].replace(']','')
        prop = rrset_property[0]
        value = rrset_property[1]
        rrset[prop] = value

    return rrset

def extract_domain(in_str):

    print in_str

    if in_str.endswith('.'):
        in_str = in_str[:-1]

    domain_id = ''
    in_str = in_str.split('.')
    array_len = len(in_str)

    if array_len >= 2:
        domain_id = in_str[array_len-2]+'.'+in_str[array_len-1]+'.'

    return domain_id


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




@app.route('/getAllDomainMetadata/<domain_id>')
def get_all_domain_metadata(domain_id):

    result = []
    metadatas = get_or_404('SELECT content FROM domain_metadata WHERE domain_id = %s ALLOW FILTERING', (domain_id, ) )

    for metadata in metadatas:
        inter = dict(
            content=metadata['content'],
        )
        result.append(inter)
    return jsonify(result=result)




@app.route('/getDomainMetadata/<domain_id>/<kind>')
def get_domain_metadata( domain_id, kind):

    ''' get metadata of this kind for this domain name '''

    result = []
    metadatas = get_or_404('SELECT content FROM domain_metadata WHERE domain_id = %s and kind = %s ALLOW FILTERING', ( domain_id, kind, ) )

    for metadata in metadatas:
        inter = dict(
            content = metadata['content'],
        )
        result.append(inter)

    return jsonify(result=result)




@app.route('/setDomainMetadata/<domain_id>/<kind>', methods=['PATCH'])
def set_domain_metadata(domain_id, kind):

    print 'URL information'
    print domain_id
    print kind
    print 'Parameter recuperation'
    in_metadatas = request.get_data()
    print in_metadatas

    inter_array = []
    val_array = []
    val = []

    inter_array = in_metadatas.split('&')
    inter_str = inter_array[0]

    val_array = inter_str.split('=')
    val.append(val_array[1])

    print 'Deleting Item:'
    delete = command( 'DELETE FROM domain_metadata WHERE domain_id = %s and kind = %s', ( domain_id, kind, ) )
    print 'Deleted'

    print 'Inserting new Item:'
    insert = command( 'INSERT INTO domain_metadata (domain_id, kind, content ) VALUES ( %s, %s, %s, )', ( domain_id, kind, val ) )
    print 'Inserted'

    return jsonify(result=result)



@app.route('/getDomainKeys/<domain_id>' )
def get_domain_keys( domain_id ):

    print 'Getting keys for domain : ' + domain_id

    keys = get_or_404('SELECT * FROM cryptokeys WHERE domain_id = %s ALLOW FILTERING' ( domain_id,))

    result=[]
    for key in keys:
        inter = dict(
            domain_id=key['domain_id'],
            active=key['active'],
            content=key['content'],
            flags=key['flags']
        )
        result.append(inter)

    return jsonify(result=result)



app.route('/addDomainKey/<domain_id>', methods=['PUT'] )
def add_domain_key( domain_id ):

    print domain_id

    in_key_data = request.get_data()

    in_key_data = in_key_data.split('&',2)

    in_flags = in_key_data[0]
    in_flags = in_flags.split('=',1)
    in_flags = in_flags[1]

    in_active = in_key_data[1]
    in_active = in_active.split('=',1)
    in_active = in_active[1]

    in_content = in_key_data[2]
    in_content = in_content.split('=',1)
    in_content = in_content[1]


    key_data = dict(
        domain_id=domain_id,
        flags=in_flags,
        active=in_active,
        content=in_content,
    )

    insert = command( 'INSERT INTO cryptokeys ( domain_id, content, active, flags ) VALUES ( %s, %s, %s, %s )', ( key_data['domain_id'], key_data['content'] , key_data['active'] , key_data['flags'] ,) )

    return jsonify(result=True)




@app.route('/removeDomainKey/<domain_id>/<id>', methods=['DELETE'] )
def remove_domain_key( domain_id, id ):

    print domain_id
    print id

    keys = get_or_404('SELECT domain_id, content FROM cryptokeys WHERE domain_id = %s ALLOW FILTERING', ( domain_id, ) )

    count = 0
    for key in keys:
        print 'Deleting Item: '+str(count)
        delete = command( 'DELETE FROM cryptokeys WHERE domain_id = %s and content = %s', ( domain_id, key['content'] , ) )
        print 'Item deleted'
        count += 1

    return jsonify(result=True)




@app.route('/activateDomainKey/<domain_id>/<id>', methods=['POST'] )
def activate_domain_key( domain_id, id ):

    print domain_id
    print id

    keys = get_or_404('SELECT domain_id, content FROM cryptokeys WHERE domain_id = %s ALLOW FILTERING', ( domain_id, ) )

    count = 0
    for key in keys:
        print 'Activating Key: '+str(count)
        insert = command( 'INSERT INTO cryptokeys (domain_id , content, active ) VALUES (%s, %s, %s ) ', ( domain_id, key['content'] , 1) )
        print 'Key activated'
        count += 1

    return jsonify(result=True)




@app.route('/deactivateDomainKey/<domain_id>/<id>', methods=['POST'] )
def deactivate_domain_key( domain_id, id ):

    print domain_id
    print id

    keys = get_or_404('SELECT domain_id, content FROM cryptokeys WHERE domain_id = %s ALLOW FILTERING', ( domain_id, ) )

    count = 0
    for key in keys:
        print 'Deactivating Key: '+str(count)
        delete = command( 'INSERT INTO cryptokeys (domain_id , content, active ) VALUES (%s, %s, %s ) ', ( domain_id, key['content'] , 0) )
        print 'Key deactivated'
        count += 1

    return jsonify(result=True)




@app.route('/getTSIGKey/<domain_id>' )
def get_tsig_key( domain_id ):

    print domain_id

    tsigkey = get_or_404('SELECT algorithm, secret FROM cryptokeys WHERE name = %s ALLOW FILTERING', ( domain_id, ) )

    return jsonify(result=tsigkey)




@app.route('/getDomainInfo/<domain_id>')
def get_domain_info(domain_id):
    ''' get info for a domain '''

    print 'Getting Domain information for domain: ' + domain_id

    result = []

    domains = get_or_404( 'SELECT * FROM domains WHERE zone = %s LIMIT 1', (domain_id,) )
    for domain in domains:
        print domain['zone']
        print domain['kind']
        print domain['serial']
        inter = dict(
            id=1,
            kind=domain['kind'],
            serial=domain['serial'],
            zone=domain['zone'],
        )
        result.append(inter)

    return jsonify(result=inter)





@app.route('/setNotified/<id>', methods=['PATCH'] )
def set_notified( id ):

    print id
    in_serial = request.get_data()
    in_serial = in_serial.split('=', 1)
    serial = in_serial[1]

    domains = get_or_404( 'SELECT * FROM domains ALLOW FILTERING', )

    for domain in domains:
        if not domain['notified_serial'] == 0:
            insert = command( 'INSERT INTO domains ( domain_id, notified_serial, serial ) VALUES ( %s, %s, %s ) ', ( domain_id, 0 , serial) )

    return jsonify(result=True)




@app.route('/isMaster/<domain_id>/<ip>' )
def ismaster( domain_id, ip ):

    print domain_id
    print ip

    domains = get_or_404( 'SELECT * FROM domains WHERE zone = %s LIMIT 1', (domain_id,) )

    masters = []
    for domain in domains:
        masters = domain['masters']
        for master in masters:
            if master == ip:
                return jsonify(result=True)

    return jsonify(result=False)



@app.route('/superMasterBackend/<ip>/<domain_id>', methods=['POST'])
def super_master_backend(ip, domain_id):

    print ip
    print domain_id

    in_nssets = request.get_data()
    print in_nssets

    masters = '['+ip+']'
    print masters

    ''' First insert new domain '''
    print 'Inserting new slave Zone:'

    insert = command( 'INSERT INTO domains (zone, kind, masters ) VALUES ( %s, %s, %s )', ( domain_id, 'Slave', masters ) )

    ''' Second insert rrsets '''
    print 'Inserting new nssets'

    nssets = parse_to_nssets(in_nssets)
    for nsset in nssets:
        insert = command(
            'INSERT INTO records (domain_id, qname, content, auth, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s )', ( domain_id, nsset['qname'], nsset['content'], nsset['auth'], nsset['qtype'], nsset['ttl'] )
        )

    return jsonify(result=True)




@app.route('/createSlaveDomain/<ip>/<domain_id>', methods=['POST'])
def create_slave_domain(ip, domain_id):

    print ip
    print domain_id

    masters = []
    masters.append(ip)
    print masters

    print 'Inserting new slave Zone:'

    insert = command( 'INSERT INTO domains (zone, kind, masters ) VALUES ( %s, %s, %s )', ( domain_id, 'SLAVE', masters ) )

    return jsonify(result=True)




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

    out_params = in_rrsets.split('&',1)
    nb_rrsets = int(out_params[0])
    in_params = out_params[1]
    str_params = '&'+out_params[1]

    rrsets = parse_to_rrsets(nb_rrsets,str_params)

    print 'After parsing parameters'
    print rrsets

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
                'INSERT INTO records (domain_id, qname, content, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s )', ( r['domain_id'], r['qname'],rrset['content'], r['qtype'], r['ttl'], )
            )

            count += count
            print count
            return jsonify(result=True), 200
        else:
            return jsonify(result=False), 404
    return 'true'




@app.route('/feedRecord/<trx>', methods=['PATCH'] )
def feed_record( trx ):

    print trx

    ''' Test if trx is present if not create transaction '''
    transactions = command( 'SELECT * FROM transactions WHERE id = %s ALLOW FILTERING', ( trx, ) )


    ''' Get parameters '''
    in_str = request.get_data()

    rr = parse_to_rr(in_str)
    domain_id = extract_domain(rr['qname'])

    insert = command(
        'INSERT INTO records (domain_id, qname, content, auth, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s, %s )', ( domain_id, rr['qname'],rr['content'],  rr['auth'], rr['qtype'], rr['ttl'], )
    )

    ''' Loop to insert records'''

    return jsonify(result=True)




@app.route('/feedEnts/<id>', methods=['PATCH'] )
def feed_ents( id ):

    print id
    ''' in  parameters example'''
    ''' trxid=1370416133&nonterm[]=_udp&nonterm[]=_sip.udp '''

    ''' Get parameters '''
    in_str = request.get_data()

    in_str = in_str.split('&')
    in_str_len = len(in_str)

    trx = ''
    domain_id= ''
    properties = {}
    index = 0
    prop = ''
    value = ''
    non_term = []

    for property_str in in_str:

        print 'Index = ' + str(index)
        property_str = property_str.split('=')

        if 'trx' in property_str[0]:
            prop = 'trx'
            value = property_str[1]

            transactions = command( 'SELECT * FROM transactions_data WHERE id = %s LIMIT 1', ( property_str[1], ) )

            for transaction in transactions:
                domain_id = transaction['domain_id']

        elif 'nonterm[]' in property_str[0]:
            non_term.append(property_str[1])
            prop = 'nonterm'
            value = non_term

        else:
            prop = property_str[0]
            value = property_str[1]

        properties[prop] = value

    print 'Properties :'
    print properties
    properties_len = len(properties)

    if properties_len > 2:

        ''' Loop to update domains'''
        if domain_id == '':
            records = command( 'SELECT * FROM records ALLOW FILTERING' )
        else:
            records = command( 'SELECT * FROM records WHERE domain_id = %s ALLOW FILTERING' ( domain_id, ) )

        for record in records:
            if properties['nonterm'][0] in record['qname']:
                record['qname'] = properties['nonterm'][1]
                insert = command(
                    'INSERT INTO records (domain_id, qname, content, auth, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s, %s )', ( record['domain_id'], record['qname'], record['content'],  record['auth'], record['qtype'], record['ttl'], )
                    )

        return jsonify(result=True)

    else:
        return jsonify(result=False)




@app.route('/feedEnts3/<id>/<domain_id>', methods=['PATCH'] )
def feed_ents3( id, domain_id ):

    print id
    print domain_id

    ''' in  parameters example'''
    ''' trxid=1370416356&times=1&salt=9642&narrow=0&nonterm[]=_sip._udp&nonterm[]=_udp '''

    ''' Get parameters '''
    in_str = request.get_data()

    in_str = in_str.split('&')
    in_str_len = len(in_str)

    properties = {}
    index = 0
    prop = ''
    value = ''
    non_term = []

    for property_str in in_str:

        print 'Index = ' + str(index)
        property_str = property_str.split('=')

        if 'trx' in property_str[0]:
            prop = 'trx'
            value = property_str[1]

        elif 'nonterm[]' in property_str[0]:
            non_term.append(property_str[1])
            prop = 'nonterm'
            value = non_term

        else:
            prop = property_str[0]
            value = property_str[1]

        properties[prop] = value

    print 'Properties :'
    print properties
    properties_len = len(properties)

    if properties_len > 2:

        records = command( 'SELECT * FROM records WHERE domain_id = %s ALLOW FILTERING' ( domain_id, ) )

        for record in records:
            if properties['nonterm'][0] in record['qname']:
                record['qname'] = properties['nonterm'][1]
                insert = command(
                    'INSERT INTO records (domain_id, qname, content, auth, qtype, ttl ) VALUES ( %s, %s, %s, %s, %s, %s )', ( record['domain_id'], record['qname'], record['content'],  record['auth'], record['qtype'], record['ttl'], )
                )
        return jsonify(result=True)

    else:
        return jsonify(result=False)





@app.route('/startTransaction/<id>/<domain_id>/<number>', methods=['POST'])
def start_transaction(id, domain_id, number):

    print id
    print domain_id
    print number

    insert = command('INSERT INTO  transactions_data( domain_id, id, state, time ) VALUES ( %s, %s, %s , toTimestamp(now()) )', (domain_id, number, 'STARTED', ) )

    print 'start transaction insert result:' + str(insert)

    return jsonify(result=True)



@app.route('/commitTransaction/<number>', methods=['POST'])
def commit_transaction( number ):

    trs = command('SELECT * FROM transactions_data WHERE id = %s ALLOW FILTERING', ( number,) )

    founds = []

    for tr in trs:
        inter = dict(
            domain_id=tr['domain_id'],
            id=tr['id'],
        )
        founds.append(inter)

    for found in founds:
        delete = command('DELETE FROM transactions_data WHERE domain_id = %s and id = %s ', (found['domain_id'], found['id'] ) )
        insert = command('INSERT INTO transactions_data ( domain_id, id, state, time ) VALUES ( %s, %s, %s, toTimestamp(now()) )', ( found['domain_id'], number, 'COMMITED', ) )

    return jsonify(result=True)



@app.route('/abortTransaction/<number>', methods=['POST'])
def abort_transaction( number ):

    trs = command('SELECT * FROM transactions_data WHERE id = %s ALLOW FILTERING', ( number,) )

    founds = []

    for tr in trs:
        inter = dict(
            domain_id=tr['domain_id'],
            id=tr['id'],
        )
        founds.append(inter)

    for found in founds:
        delete = command('DELETE FROM transactions_data WHERE domain_id = %s and id = %s ', (found['domain_id'], found['id'] ) )
        insert = command('INSERT INTO transactions_data ( domain_id, id, state, time ) VALUES ( %s, %s, %s, toTimestamp(now()) )', ( found['domain_id'], number, 'TO_ABORT', ) )

    return jsonify(result=True)





@app.route('/calculateSOASerial/<domain_id>', methods=['POST'] )
def calculate_soa_serial( domain_id ):

    print domain_id

    ''' in  parameters example'''
    ''' sd[qname]=unit.test&sd[nameserver]=ns.unit.test&sd[hostmaster]=hostmaster.unit.test&sd[ttl]=300&sd[serial]=1&sd[refresh]=2&sd[retry]=3&sd[expire]=4&sd[default_ttl]=5&sd[domain_id]=-1&sd[scopemask]=0p '''

    ''' Get parameters '''
    in_str = request.get_data()

    in_str = in_str.split('&')

    sd = {}

    for property_str in in_str:
        if 'sd[' in property_str:
            property_str = property_str.replace('sd[','')
            property_str = property_str.replace(']','')
            property_str = property_str.split('=')
            prop = property_str[0]
            value = property_str[1]

            sd[prop] = value

    serial = sd['serial']

    num_serial = int(serial)
    num_serial += 1

    sd['serial'] = str(num_serial)

    return jsonify(result=sd['serial'])




@app.route('/directBackendCmd', methods=['POST'] )
def direct_backend_cmd():

    in_str = request.get_data()

    print in_str

    ''' TODO '''
    return jsonify(result=True)





@app.route('/getAllDomains')
def get_all_domains():

    ''' get all zones for master server included disabled and slave zones '''

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



@app.route('/getUpdatedMasters')
def get_updated_masters():


    ''' Used to find any updates to master zones and finally used to trigger notifications  '''

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

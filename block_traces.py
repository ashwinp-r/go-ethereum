import httplib
import json
import sys


def trace_block(connection, block_number):
    """ Create a new account with the default password """
    params = [hex(block_number)]
    connection.request(
        method='POST',
        url='',
        body=json.dumps({"jsonrpc": "2.0", "method": "debug_blockWitnessSizeByNumber", "params": params, "id": 0}),
        headers={'Content-Type': 'application/json'})
    response = connection.getresponse()
    if response.status != httplib.OK:
        print 'Could not trace block', response.status, response.reason
        sys.exit(0)
    r = json.load(response)
    if 'result' in r:
        return r['result']
    return r

http = httplib.HTTPConnection('localhost:8545')

with open('wit_sizes.json', 'w') as f:
    for block_number in xrange(1, 3895764):
        print block_number
        print >>f, trace_block(http, block_number)

http.close()

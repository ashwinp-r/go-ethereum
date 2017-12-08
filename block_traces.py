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

for block_number in xrange(3757353, 3757453):
    print trace_block(http, block_number)

#print trace_block(http, 3757353) # Invalid root/receipts

#print trace_block(http, 3757354) # Invalid root/receipts

#print trace_block(http, 3757356) # Works

http.close()

"""
A sample Hello World server.
"""
import os
import json
import gzip

from flask import Flask, request
from google.cloud import bigquery

# pylint: disable=C0103
app = Flask(__name__)

def convert(inKey, outKey, inp, out):
  try:
    out[outKey] = inp[inKey]
  except KeyError:
    pass

def convert_json(inp):
  out = {}
  convert('level', 'log_level', inp, out)
  convert('msg', 'msg', inp, out)
  convert('time', 'log_time', inp, out)

  if 'decision_id' in inp:
    convert('decision_id', 'decision_id', inp, out)

  if 'error' in inp:
    out['rego_error_message'] = inp['error']['message']

  if 'input' in inp:
    out['dest_ip'] = inp['input']['attributes']['destination']['address']['socketAddress']['address']
    out['x_request_id'] = inp['input']['attributes']['request']['http']['headers']['x-request-id']
    out['req_host'] = inp['input']['attributes']['request']['http']['host']
    out['req_path'] = inp['input']['attributes']['request']['http']['path']
    out['req_time'] = inp['input']['attributes']['request']['time']
    out['src_ip'] = inp['input']['attributes']['source']['address']['socketAddress']['address']
    try:
      out['dest_principal'] = inp['input']['attributes']['destination']['principal']
    except KeyError:
      pass
    try:
      out['src_principal'] = inp['input']['attributes']['source']['principal']
    except KeyError:
      pass
    try:
      out['x_forwarded_client_cert'] = inp['input']['attributes']['request']['http']['headers']['x-forwarded-client-cert']
    except KeyError:
      pass

  if 'path' in inp:
    convert('path', 'rego_path', inp, out)

  if 'decision' in inp:
    out['allowed'] = inp['decision']['allowed']
    try:
      out['cant_mutate'] = inp['decision']['headers']['X-CANT-MUTATE']
    except KeyError:
      pass
    out['http_status'] = inp['decision']['http_status']
  elif 'result' in inp:
    out['allowed'] = inp['result']['allowed']
    try:
      out['cant_mutate'] = inp['result']['headers']['X-CANT-MUTATE']
    except KeyError:
      pass
    out['http_status'] = inp['result']['http_status']

  return out

@app.route('/logs', methods=['POST'])
def hello():
  client = bigquery.Client()
  table_id = "monorepotest-323514.authz.decision_log"
  logs = json.loads(gzip.decompress(request.data).decode('utf-8'), strict=False)
  for log in logs:
    print(log)
    converted = convert_json(log)      
    rows_to_insert = [converted]
    errors = client.insert_rows_json(
      table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )  # Make an API request.
    if errors == []:
      continue
    else:
      print("Encountered errors {} while inserting rows: {}".format(errors, converted))
      return "", 500
  return "", 200

if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')

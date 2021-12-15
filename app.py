"""
A sample Hello World server.
"""
import os
import json
import gzip
import uuid
import time

from flask import Flask, request
from google.cloud import bigquery
from google.cloud import monitoring_v3
from google.protobuf import timestamp_pb2
from google.api import label_pb2 as ga_label
from google.api import metric_pb2 as ga_metric
from google.api_core import exceptions

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

  if 'metrics' in inp:
    metrics = inp['metrics']
    convert('timer_rego_external_resolve_ns', 'timer_rego_external_resolve_ns', metrics, out)
    convert('timer_rego_query_compile_ns', 'timer_rego_query_compile_ns', metrics, out)
    convert('timer_rego_query_eval_ns', 'timer_rego_query_eval_ns', metrics, out)
    convert('timer_rego_query_eval_ns', 'timer_rego_query_eval_ns', metrics, out)

  return out

def writeMetrics(log, converted):    
  if 'metrics' in log and 'req_time' in converted:
    client = monitoring_v3.MetricServiceClient()
    project="monorepotest-323514"
    project_name = f"projects/{project}"
    metrics = log['metrics']
    for key in metrics:
        val = metrics[key]
        series = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/opa/" + key
        timestamp = timestamp_pb2.Timestamp()        
        interval = monitoring_v3.TimeInterval()
        timestamp.FromJsonString(converted['req_time'])
        interval.end_time = timestamp
        print('converted[req_time]=', converted['req_time'])
        print('interval.end_time=', interval.end_time)
        point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": val}})
        series.points = [point]   
        try:
            client.create_time_series(name=project_name, time_series=[series]) 
        except exceptions.InvalidArgument as e:
            if "more frequently than the maximum sampling period" not in e.message:
                raise e    
   

@app.route('/logs', methods=['POST'])
def hello():
#   client = bigquery.Client()
#   table_id = "monorepotest-323514.authz.decision_log"
  logs = json.loads(gzip.decompress(request.data).decode('utf-8'), strict=False)
  for log in logs:
    print(log)
    converted = convert_json(log) 
    writeMetrics(log, converted)         
    # rows_to_insert = [converted]
    # errors = client.insert_rows_json(
    #   table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    # )  # Make an API request.
    # if errors == []:
    #   continue
    # else:
    #   print("Encountered errors {} while inserting rows: {}".format(errors, converted))
    #   return "", 500
  return "", 200

def create_metric_descriptor(project_id):
    # [START monitoring_create_metric]
    from google.api import label_pb2 as ga_label
    from google.api import metric_pb2 as ga_metric
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"
    descriptor = ga_metric.MetricDescriptor()
    descriptor.type = "custom.googleapis.com/my_metric"
    descriptor.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
    descriptor.value_type = ga_metric.MetricDescriptor.ValueType.DOUBLE
    descriptor.description = "This is a simple example of a custom metric."

    labels = ga_label.LabelDescriptor()
    labels.key = "TestLabel"
    labels.value_type = ga_label.LabelDescriptor.ValueType.STRING
    labels.description = "This is a test label"
    descriptor.labels.append(labels)

    descriptor = client.create_metric_descriptor(
        name=project_name, metric_descriptor=descriptor
    )
    print("Created {}.".format(descriptor.name))
    # [END monitoring_create_metric]

def delete_metric_descriptor(descriptor_name):
    # [START monitoring_delete_metric]
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    client.delete_metric_descriptor(name=descriptor_name)
    print("Deleted metric descriptor {}.".format(descriptor_name))
    # [END monitoring_delete_metric]

def write_time_series(project_id):
    # [START monitoring_write_timeseries]
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    series = monitoring_v3.TimeSeries()
    series.metric.type = "custom.googleapis.com/my_metric"
    series.resource.type = "gce_instance"
    series.resource.labels["instance_id"] = "1234567890123456789"
    series.resource.labels["zone"] = "us-central1-f"
    series.metric.labels["TestLabel"] = "My Label Data"
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )
    point = monitoring_v3.Point({"interval": interval, "value": {"double_value": 3.14}})
    series.points = [point]
    client.create_time_series(name=project_name, time_series=[series])
    # [END monitoring_write_timeseries]

def list_metric_descriptors(project_id):
    # [START monitoring_list_descriptors]
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"
    for descriptor in client.list_metric_descriptors(name=project_name):
        print(descriptor.type)
    # [END monitoring_list_descriptors]

if __name__ == '__main__':
    PROJECT_ID="monorepotest-323514"
    # create_metric_descriptor(PROJECT_ID)
    # write_time_series(PROJECT_ID)
    # list_metric_descriptors(PROJECT_ID)
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')

import os
import sys
import traceback
import json
import requests
from functools import reduce

api_configs = {
    'enzek-meterpoint-readings-artifacts-uat-555393537168': {
        'type': 'jenkins',
        'auth': ('ci-agent', '11673e1ae064ca46c9a88337e2b91a427f'),
        'url': 'http://internal-igloo-jenkins-elb-14111292.eu-west-1.elb.amazonaws.com/job/deploy-enzek-meterpoint-readings-dags/buildWithParameters',
        'token': 'f8e32a3e-3602-4d0f-aef3-ed1e66881ca2',
        'parameters_from_event_record': [{
            'name': 's3key',
            'path': 's3.object.key'
        }]
    }
}

# Access nested dictionary items by string, e.g.
# tik = { 'foo': { 'bar': 'baz' }}
# assert deep_get(tik, 'foo.bar') == 'baz'
def deep_get(dictionary, keys, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)

def lambda_handler(event, context):
    """
    This lambda function is expected to be invoked when an object is created in
    a specific S3 bucket. It's purpose is to then call an internal API providing
    it the key of the created object.
    """
    try:
        record = event['Records'][0]

        s3_bucket_name = deep_get(record, 's3.bucket.name')
        s3_object_key = record['s3']['object']['key']

        api_config = api_configs[s3_bucket_name]

        if api_config['type'] == 'jenkins':
            parameters = {
                'token': api_config['token']
            }

            for parameter in api_config['parameters_from_event_record']:
                parameters[parameter['name']] = deep_get(record, parameter['path'])

            response = requests.get(api_config['url'], auth=api_config['auth'], params=parameters)

            print('response.status_code: ', response.status_code)
            if response.status_code < 200 or response.status_code > 299:
                print('response.text: ', response.text)
        else:
            print('Unexpected type: ', api_config['type'])
            pass

    except:
        traceback.print_exc()

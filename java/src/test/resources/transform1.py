import json

def init(settings):
    print("transform init() in python")
    print(settings)

def transform(record):
    print("transform entry point in python")
    print(f"received: {record}")

    record['value'] = f"Modified from python --> {record['value']}"
    record['key'] = 999

    return record

def transform_json(record):
    print("transform entry point in python")
    print(f"received: {record}")

    json_obj = json.loads(record['value'])
    print(json_obj)

    json_obj['some_string'] = f"Modified from python --> {json_obj['some_string']}"
    record['value'] = json.dumps(json_obj)
    record['key'] = 999

    return record

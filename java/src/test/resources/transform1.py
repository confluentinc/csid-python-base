import json

def init(settings):
    print("transform init() in python")
    print(settings)

def transform(record):
    try:
        print("transform entry point in python")
        print(f"received: {record}")

        record['value'] = f"Modified from python --> {record['value']}"
        record['key'] = 999
    except Exception as e:
        print("An exception occured:")
        print(e)

    return record

def transform_json(record):
    try:
        print("transform entry point in python")
        print(f"received: {record}")

        json_obj = json.loads(record['value'])
        print(json_obj)

        json_obj['first_name'] = f"Modified from python --> {json_obj['first_name']}"
        record['value'] = json.dumps(json_obj)
        record['key'] = 999
    except Exception as e:
        print("An exception occured:")
        print(e)

    return record

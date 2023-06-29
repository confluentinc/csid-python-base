def init(settings):
    print("transform init() in python")
    print(settings)

def transform(record):
    print("transform entry point in python")
    print(f"received: {record}")

    record['value'] = f"Modified from python --> {record['value']}"
    record['key'] = 999

    return record

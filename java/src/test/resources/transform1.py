def init(settings):
    print("tranform init() in python")
    print(settings)

def transform(record):
    print("transform entry point in python")
    print(f"received: {record}")

    record['value'] = f"Modified from python --> {record['value']}"

    return record

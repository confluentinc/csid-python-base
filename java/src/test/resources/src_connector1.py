import json

def init(settings):
    print("source connector init() in python")
    print(json.loads(settings))

def poll_basic_types():
    return [{
        'key': {'type': 'INT32', 'value': 1234},
        'value': {'type': 'STRING', 'value': 'azerty'}
    }, {
        'key': {'type': 'INT32', 'value': 6789},
        'value': {'type': 'STRING', 'value': 'abcdef'}
    }]

def poll_key_and_value():
    return [{
        'key': {'type': 'INT32', 'value': 1234},
        'value': {'type': 'STRUCT', 'value': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }}
    }, {
        'key': {'type': 'INT32', 'value': 6789},
        'value': {'type': 'STRUCT', 'value': {
            'first_name': 'Jane',
            'last_name': 'Dolittle',
            'age': 37
        }}
    }]

def poll_no_key():
    return [{
        'key': None,
        'value': {'type': 'STRUCT', 'value': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }}
    }]

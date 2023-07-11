import json

def init(settings):
    print("source connector init() in python")
    print(json.loads(settings))

def poll_basic_types():
    return [{
        'key': {'type': 'INT32', 'data': 1234},
        'value': {'type': 'STRING', 'data': 'azerty'}
    }, {
        'key': {'type': 'INT64', 'data': 6789},
        'value': {'type': 'STRING', 'data': 'abcdef'}
    }]

def poll_key_and_value():
    return [{
        'key': {'type': 'INT32', 'data': 1234},
        'value': {'type': 'STRUCT', 'data': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }}
    }, {
        'key': {'type': 'INT32', 'data': 6789},
        'value': {'type': 'STRUCT', 'data': {
            'first_name': 'Jane',
            'last_name': 'Dolittle',
            'age': 37
        }}
    }]

def poll_key_and_value_both_objects():
    return [{
        'key': {'type': 'STRUCT', 'data': {
            'id': 1234,
            'type': 'something'
        }},
        'value': {'type': 'STRUCT', 'data': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }}
    }, {
        'key': {'type': 'STRUCT', 'data': {
            'id': 567,
            'type': 'else'
        }},
        'value': {'type': 'STRUCT', 'data': {
            'first_name': 'Jane',
            'last_name': 'Dolittle',
            'age': 37
        }}
    }]

def poll_no_key():
    return [{
        'key': None,
        'value': {'type': 'STRUCT', 'data': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }}
    }]

def all_default_types():
    return [{
        'key': None,
        'value': {'type': 'STRUCT', 'data': {
            'str': 'Hello',
            'bool': True,
            'int': 25,
            'float': 1.0,
            'bytes': b'\x04\x00'
        }}
    }]

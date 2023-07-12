import json

def init(settings):
    print("source connector init() in python")
    print(json.loads(settings))

def poll_basic_types():
    return [{
        'key': 1234,
        'value': "some string"
    },{
        'key':  1234.5,
        'value':  True
    },{
        'key':  1234.5,
        'value': {
            'str': 'Hello',
            'bool': True,
            'int': 25,
            'float': 1.0,
            'bytes': b'\x04\x00'
        }
    }]


def poll_key_and_value_both_objects():
    return [{
        'key': {
            'id': 1234,
            'type': 'something'
        },
        'value': {
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 25
        }
    }, {
        'key': {
            'id': 567,
            'type': 'else'
        },
        'value': {
            'first_name': 'Jane',
            'last_name': 'Dolittle',
            'age': 37
        }
    }]


def all_default_types():
    return [{
        'key': None,
        'value': {
            'str': 'Hello',
            'bool': True,
            'int': 25,
            'float': 1.0,
            'bytes': b'\x04\x00'
        }
    }]

def single_item():
    return {'key': None, 'value': 'Hello'}

def invalid_1():
    return {'key', 'value', 'Hello'}

def invalid_2():
    return "hello"

import json
import random


def init(settings, offsets):
    print("source connector init() in python")
    print("settings:")
    print(json.loads(settings))
    print("offsets:")
    print(offsets)


def test_offsets(offsets):
    print("offsets:")
    print(offsets)

    offset = offsets.get('latest', 0)

    return [{
        'key': 1234,
        'value': "some string",
        'offset': offset + 1
    },{
        'key': 45634,
        'value': "another string",
        'offset': offset + 2
    }]

def test_offsets_as_string(offsets):
    print("offsets:")
    print(offsets)

    offset = random.randint(0, 99999)

    return [{
        'key': 1234,
        'value': "some string",
        'offset': f"{offset}-1"
    },{
        'key': 45634,
        'value': "another string",
        'offset': f"{offset}-2"
    }]

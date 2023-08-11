import json
import random
import time


def init(settings, offsets):
    print("source connector init() in python")
    print("settings:")
    print(json.loads(settings))
    print("offsets:")
    print(offsets)


def poll(offsets):
    sleep_time = random.randint(1, 1999)
    time.sleep(sleep_time/1000)

    return [{
        'key': sleep_time,
        'value': f"some string - {sleep_time}"
    }]

import json

def simple(a_string, an_integer):
    return f"{a_string} {an_integer}"

def json_str(json_string):
    json_obj = json.loads(json_string)
    return f"{json_obj['a_string']} {json_obj['an_integer']}"

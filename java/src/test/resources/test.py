import arrow

def hello():
    print("inside test.py > hello()")
    return arrow.utcnow().humanize()


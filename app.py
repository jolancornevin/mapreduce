#!/usr/bin/python3
from subprocess import check_output, call
from flask import Flask, make_response, jsonify
from json import loads
from functools import wraps
app = Flask(__name__)


def add_response_headers(headers={}):
    """This decorator adds the headers passed in to the response"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            resp = make_response(f(*args, **kwargs))
            h = resp.headers
            for header, value in headers.items():
                h[header] = value
            return resp
        return decorated_function
    return decorator

def cross_origin(f):
    @wraps(f)
    @add_response_headers({'Access-Control-Allow-Origin': "*"})
    def decorated_function(*args, **kwargs):
        return f(*args, **kwargs)
    return decorated_function

@app.route('/launch')
@cross_origin
def launch():
    return str(call('./run.sh', shell=True))

with open('./visualisation/tronçons_ligne') as f:
    list = loads(f.readline())['features']
    rues = {}
    for thing in list:
        nom = thing['properties']['nom']
        code = thing['properties']['codefuv']
        if not code in rues:
            rues[code] = nom

@app.route('/output')
@cross_origin
def see_output():
    output = check_output('./see_output.sh', shell=True).decode('UTF-8')
    lines = output.split('\n')[:-2]
    liste = []
    for line in lines[-10:]:
        print(line)
        line = line.split('\t')
        liste.append({'nom': rues[line[1]], 'rang': line[0]})
    return jsonify(liste)


if __name__ == '__main__':
    app.run()

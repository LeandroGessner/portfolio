import json
import argparse
import logging as log

from flask import Flask, request, Response
from pipeline import input


def setup():
    """ Setup necessary parameters

    :return  port number
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "port", 
        type=int, 
        help="Provide a port number ",
        nargs='?', 
        default=60000
    )
    args = parser.parse_args()

    log.basicConfig(level=log.INFO, format="%(asctime)s [%(levelname)s] [%(name)8s] %(message)s")
    return args.port


port = setup()
app = Flask(__name__)


def answer(result):
    """ Helper method for creating a JSON answer

    :return HTTP Response
    """
    return Response(json.dumps(result), status=200, mimetype="application/json")


@app.route("/ping", methods=["GET"])
def ping():
    return answer({"status": "OK"})


@app.route("/collect", methods=["POST"])
def collect():
    event = request.get_json()
    return answer(input.collect(event))


app.run(host="0.0.0.0", port=port)

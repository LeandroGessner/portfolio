from uuid import uuid4
import json
import hashlib
# import os
from .kafka import Kafka


OK = {'status': 'OK'}
KAFKA = Kafka()


def get_package_id() -> str:
    return str(uuid4()).split('-')[0]


def get_object_id(obj: str) -> str:
    return hashlib.md5(obj.encode()).hexdigest()


def collect(event: list):
    """ Handle data collection event

    :return confirmation
    """
    package_id = get_package_id()

    for ob in event:
        object_id = get_object_id(obj=json.dumps(ob, sort_keys=True))
        key = f"{package_id}={object_id}"

        KAFKA.send_message(
            data=ob,
            key=key
        )

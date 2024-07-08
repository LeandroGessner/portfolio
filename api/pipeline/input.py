from uuid import uuid4
import json
import hashlib
import os
from .kafka import KafkaKantox


OK = {'status': 'OK'}

KAFKA = KafkaKantox()


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

        # print(f"PACKAGE ID: {package_id}")
        # print(f"OBJECT ID: {object_id}")
        # print(json.dumps(ob, indent=4))

        KAFKA.send_message(
            data=ob,
            key=key
        )


if __name__ == "__main__":
    print(os.path.abspath(os.path.expanduser('~')))

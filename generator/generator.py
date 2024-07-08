import logging as log
import argparse
import random
import requests
import json


def setup():
    """ Setup necessary parameters

    :return  arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "hostname", 
        type=str, 
        help="Provide a hostname ",
        nargs='?', 
        default='http://localhost:60000'
    )
    parser.add_argument(
        "filename", 
        type=str, 
        help="Provide a data filename ",
        nargs='?',
        default='../data/events.json'
    )
    args = parser.parse_args()
    
    # Logging Configuration
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)8s] %(message)s'
    )
    
    return args


def send_events(hostname, events):
    # Sending events to a REST API
    try:
        response = requests.post(f'{hostname}/collect', json=events)
        response.raise_for_status()
        if response.status_code != 200:
            log.error("Failed to send event:", events)
    except Exception as e:
        log.error(e)


def generate_random_json_objects(hostname, filename):
    with open(filename, mode="r", encoding="utf8") as file_obj:
        rnd: int = random.randint(1, 10)
        count: int = 0
        data = []

        for line in file_obj:
            try:
                json_object = json.loads(line)
                data.append(json_object)
                count += 1

                if count == rnd:
                    send_events(hostname, data)
                    data.clear()
                    count = 0
                    rnd = random.randint(1, 10)
            except json.JSONDecodeError as e:
                log.error(f"Error parsing JSON: {e}")


if __name__ == "__main__":
    args = setup()

    # current_os: str = (platform.system()).lower()
    # if current_os == "windows":
    #     args.filename = 'challenge/data/events.json'

    generate_random_json_objects(args.hostname, args.filename)

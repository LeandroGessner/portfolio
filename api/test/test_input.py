from random import randrange
from pipeline import input


def test_collect():
    result = input.collect({
        'id': randrange(0, 999999),
        'type': 'page_view',
        "event": {
            'user-agent': 'Mozilla/5.0 (Linux; Android 2.2) AppleWebKit/536.2 (KHTML, like Gecko) Chrome/46.0.817.0 Safari/536.2',
            'ip': '24.49.160.149',
            'customer-id': None,
            'timestamp': '2022-05-25T05:44:03.648681',
            'page': 'https://xcc-webshop.com/category/13'
        }
    })
    assert result == {'status': 'OK'}


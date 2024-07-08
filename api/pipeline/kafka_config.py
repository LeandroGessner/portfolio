config = {
    'kafka': {
        'bootstrap.servers': 'localhost:9092'
    },
    'schema_registry': {
        'url': 'http://localhost:8085'
    },
    'avro_schema': """{
        "type": "record",
        "name": "EventValidation",
        "namespace": "kantox.leandro.challenge",
        "fields": [
            {
                "name": "id",
                "type": "long"
            },
            {
                "name": "type",
                "type": "string"
            },
            {
                "name": "event",
                "type": {
                    "type": "record",
                    "name": "event",
                    "fields": [
                        {
                            "name": "user_agent",
                            "type": "string"
                        },
                        {
                            "name": "ip",
                            "type": "string"
                        },
                        {
                            "name": "customer_id",
                            "type": [
                                "null",
                                "long"
                            ]
                        },
                        {
                            "name": "timestamp",
                            "type": "string"
                        },
                        {
                            "name": "page",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "query",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "product",
                            "type": [
                                "null",
                                "long"
                            ]
                        },
                        {
                            "name": "referrer",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "position",
                            "type": [
                                "null",
                                "long"
                            ]
                        }
                    ]
                }
            }
        ]
    }"""
}

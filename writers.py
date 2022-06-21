import json
from abc import ABC, abstractmethod
from json import dumps
from os import getenv
from utils import RedisHelper
from azure.eventhub import EventHubProducerClient, EventData


class AbstractWriter(ABC):
    def __init__(self, endpoint):
        self.redis_helper = RedisHelper(endpoint=endpoint)

    @abstractmethod
    def write(self, data):
        raise NotImplementedError


class ConsoleWriter(AbstractWriter):
    def write(self, data):
        print(dumps(data), '\n')


class EventHubWriter(AbstractWriter):
    def __init__(self, endpoint):
        super().__init__(endpoint)
        self.producer = EventHubProducerClient.from_connection_string(conn_str=getenv('EVENT_HUB_CONN_STR'),
                                                                      eventhub_name=getenv('EVENT_HUB_NAME'))

    def write(self, data):
        self.redis_helper.set_in_progress_status()
        batch = self.producer.create_batch()
        for record in data:
            batch.add(EventData(json.dumps(record)))
        self.producer.send_batch(event_data_batch=batch)
        self.redis_helper.set_completed_status()
        print(f'Send data to Event HUB \n DATA: {data}')

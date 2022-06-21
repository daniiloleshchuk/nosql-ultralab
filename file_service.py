from writers import EventHubWriter, ConsoleWriter
from utils import get_endpoint_data_partially
from os import getenv


class FileService:
    def __init__(self, endpoint, target):
        self.endpoint = endpoint
        self.target = target
        self.chunk_size = int(getenv('CHUNK_SIZE', 100))
        self.writer = (EventHubWriter(endpoint=endpoint)
                       if target and str(target).lower().replace(' ', '').replace('_', '') == 'eventhub'
                       else ConsoleWriter(endpoint=endpoint))

    def process(self):
        if not self.writer.redis_helper.processed():
            for chunk in get_endpoint_data_partially(endpoint=self.endpoint):
                self.writer.write(data=chunk.json())
            return f'Written data to {self.target}'
        return f'Data has been already written to {self.target}'

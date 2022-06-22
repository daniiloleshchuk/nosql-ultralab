from os import getenv

from redis import StrictRedis
from requests import get


def get_endpoint_data_partially(endpoint):
    chunk_size = int(getenv('CHUNK_SIZE', 100))
    endpoint = endpoint
    offset = 0
    while True:
        request_url = f'{endpoint}?$limit={chunk_size}&$offset={offset}'
        chunk_data = get(request_url)
        offset += chunk_size
        if chunk_data.json():
            yield chunk_data
        else:
            break


class RedisHelper:
    COMPLETED = 'COMPLETED'
    IN_PROGRESS = 'IN PROGRESS'

    def __init__(self, endpoint):
        self.endpoint_status_key = endpoint + '_status'
        self.redis_client = StrictRedis(host=getenv('REDIS_HOST', 'localhost'),
                                        port=getenv('REDIS_PORT', 6379),
                                        password=getenv('REDIS_PASSWORD', None),
                                        ssl=True)

    def processed(self):
        endpoint_status_value = self.redis_client.get(self.endpoint_status_key)
        return endpoint_status_value and endpoint_status_value.decode() == self.COMPLETED

    def set_in_progress_status(self):
        self.redis_client.set(self.endpoint_status_key, self.IN_PROGRESS)

    def set_completed_status(self):
        self.redis_client.set(self.endpoint_status_key, self.COMPLETED)

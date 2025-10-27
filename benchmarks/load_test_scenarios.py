import time
import random
from locust import HttpUser, task, between, events

class CacheUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    def on_start(self):
        self.client_id = self.environment.runner.client_id
        self.key_index = 0

    def get_next_key(self):
        self.key_index = (self.key_index + 1) % 50 
        return f"cache_key_{self.key_index}"

    @task(20) 
    def read_from_cache(self):
        address = self.get_next_key()
        
        with self.client.get(f"/cache/read?address={address}", catch_response=True, name="/cache/read") as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                response.success() 
            else:
                response.failure(f"Cache read failed with status {response.status_code}")

    @task(1) 
    def write_to_cache(self):
        address = self.get_next_key()
        payload = {
            "address": address,
            "value": f"data_from_{self.client_id}_{time.time()}"
        }
        
        with self.client.post("/cache/write", json=payload, catch_response=True, name="/cache/write") as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Cache write failed with status {response.status_code}")
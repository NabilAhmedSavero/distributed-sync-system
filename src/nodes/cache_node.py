import asyncio
import logging
from typing import Dict, Any, Optional, Tuple
from collections import OrderedDict
from enum import Enum
from aiohttp import web
import time
import json 
from redis.asyncio import Redis 

from .base_node import BaseNode, NodeConfig, NodeState
from ..consensus.raft import RaftNode, RaftState
from ..utils import metrics

logger = logging.getLogger(__name__)


class CacheState(Enum):
    MODIFIED = "modified"
    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    INVALID = "invalid"


class LRUCache:
    def __init__(self, capacity: int = 128):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]['value']

    def put(self, key: str, value: Any, state: CacheState):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = {'value': value, 'state': state}
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def get_state(self, key: str) -> Optional[CacheState]:
        return self.cache[key]['state'] if key in self.cache else None
        
    def set_state(self, key: str, state: CacheState):
        if key in self.cache:
            self.cache[key]['state'] = state
            
    def invalidate(self, key: str):
        if key in self.cache:
            self.cache[key]['state'] = CacheState.INVALID

class DistributedCacheNode(BaseNode):
    
    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self.cache = LRUCache(capacity=256)
        self.main_memory = self.redis_client 
        self.raft: Optional[RaftNode] = None
        
        self.internal_metrics.update({
            'cache_hits': 0,
            'cache_misses': 0,
            'invalidations_sent': 0,
            'invalidations_received': 0
        })
        logger.info(f"DistributedCacheNode {self.node_id} initialized")

    def _register_routes(self, app: web.Application):
        app.add_routes([
            web.post('/raft-vote', self._handle_raft_vote),
            web.post('/raft-append', self._handle_raft_append),
            web.post('/cache/invalidate', self._handle_invalidate),
            web.post('/cache/bus_read', self._handle_bus_read),
            web.get('/cache/read', self._handle_cache_read),
            web.post('/cache/write', self._handle_cache_write),
        ])

    async def _on_start(self):
        if not self.redis_client:
             logger.error("Redis client not initialized before RaftNode creation.")
             raise RuntimeError("Redis client required for RaftNode")

        peer_ids = list(self.peers.keys()) 
        logger.info(f"Initializing RaftNode for CacheNode with peers: {peer_ids}")
        
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=peer_ids,
            send_rpc_callback=self._send_rpc,
            apply_command_callback=self._apply_raft_command,
            persist_callback=self._persist_raft_state, 
            load_callback=self._load_raft_state,     
            redis_client=self.redis_client
        )
        await self.raft.start()
        
        logger.info(f"Cache Node {self.node_id} is running.")
        self._tasks.append(
            asyncio.create_task(self._collect_metrics_loop())
        )

    async def _on_stop(self):
        if self.raft:
            await self.raft.stop()
        logger.info(f"Cache Node {self.node_id} is stopping.")

    async def _on_peer_failed(self, peer_id: str):
        logger.warning(f"Cache peer {peer_id} failed.")

    async def _on_peer_recovered(self, peer_id: str):
        logger.info(f"Cache peer {peer_id} recovered.")

    async def _handle_raft_vote(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        
        if not self.raft: 
            response_data = {"error": "Raft not initialized"}
            status_code = 503
        else:
            try:
                data = await request.json()
                response_data = await self.raft.handle_vote_request(data)
            except Exception as e:
                logger.error(f"Error handling vote request: {e}", exc_info=True)
                metrics.increment_errors(self.node_id, "handle_raft_vote_error")
                response_data = {"error": str(e)}
                status_code = 400
                
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_raft_append(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200

        if not self.raft: 
            response_data = {"error": "Raft not initialized"}
            status_code = 503
        else:
            try:
                data = await request.json()
                response_data = await self.raft.handle_append_entries(data)
            except Exception as e:
                logger.error(f"Error handling append entries request: {e}", exc_info=True)
                metrics.increment_errors(self.node_id, "handle_raft_append_error")
                response_data = {"error": str(e)}
                status_code = 400

        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_invalidate(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        
        try:
            data = await request.json()
            address = data['address']
            
            if self.cache.get_state(address) == CacheState.SHARED:
                self.cache.invalidate(address)
                
            self.internal_metrics['invalidations_received'] += 1
            response_data = {"status": "ack"}
        except Exception as e:
            logger.error(f"Error handling invalidate: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, "handle_invalidate_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
            
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)


    async def _handle_bus_read(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200

        try:
            data = await request.json()
            address = data['address']
            state = self.cache.get_state(address)
            
            if state in {CacheState.MODIFIED, CacheState.EXCLUSIVE, CacheState.SHARED}:
                if state == CacheState.MODIFIED:
                    await self._write_back_to_memory(address)
                
                self.cache.set_state(address, CacheState.SHARED)
                response_data = {"status": "shared", "value": self.cache.get(address)}
            else:
                response_data = {"status": "not_found"}
                
        except Exception as e:
            logger.error(f"Error handling bus read: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, "handle_bus_read_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
            
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_cache_read(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        try:
            address = request.query.get('address')
            if not address:
                 response_data = {"status": "error", "message": "address query parameter is required"}
                 status_code = 400
            else:
                value = await self.read_data(address)
                if value is not None:
                    response_data = {"status": "hit", "address": address, "value": value}
                else:
                    response_data = {"status": "miss", "address": address, "value": None}
                    status_code = 404
        except Exception as e:
            logger.error(f"Error handling cache read: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, "handle_cache_read_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
        
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_cache_write(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        try:
            data = await request.json()
            address = data['address']
            value = data['value']
            await self.write_data(address, value)
            response_data = {"status": "written", "address": address, "value": value}
        except Exception as e:
            logger.error(f"Error handling cache write: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, "handle_cache_write_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400

        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def read_data(self, address: str) -> Optional[Any]:
        state = self.cache.get_state(address)

        if state in {CacheState.MODIFIED, CacheState.EXCLUSIVE, CacheState.SHARED}:
            self.internal_metrics['cache_hits'] += 1
            return self.cache.get(address)
        
        self.internal_metrics['cache_misses'] += 1
        logger.info(f"Cache miss (read) for {address}. Fetching...")
        
        value, state = await self._broadcast_bus_read(address)
        
        if value is None and self.main_memory:
            value_str = await self.main_memory.get(f"cache_mem:{address}")
            try:
                 value = json.loads(value_str) if value_str else None
            except:
                 value = value_str
            state = CacheState.EXCLUSIVE
            
        if value is not None:
             self.cache.put(address, value, state)
        return value

    async def write_data(self, address: str, value: Any):
        state = self.cache.get_state(address)
        
        if state == CacheState.SHARED or state == CacheState.INVALID or state is None:
            if state is None or state == CacheState.INVALID:
                 self.internal_metrics['cache_misses'] += 1
            logger.info(f"Write miss/shared. Broadcasting INVALIDATE for {address}")
            await self._broadcast_invalidate(address)
            
        self.cache.put(address, value, CacheState.MODIFIED)
        await self._write_back_to_memory(address) 

    async def _broadcast_invalidate(self, address: str):
        self.internal_metrics['invalidations_sent'] += 1
        tasks = []
        for peer_id in self.peers:
            tasks.append(
                self._send_rpc(peer_id, '/cache/invalidate', {"address": address})
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_bus_read(self, address: str) -> Tuple[Optional[Any], CacheState]:
        tasks = [
            self._send_rpc(peer_id, '/cache/bus_read', {"address": address})
            for peer_id in self.peers
        ]
        
        if not tasks:
             return None, CacheState.EXCLUSIVE
             
        for future in asyncio.as_completed(tasks):
            response = await future
            if response and response.get('status') == 'shared':
                return response.get('value'), CacheState.SHARED
                
        return None, CacheState.EXCLUSIVE

    async def _write_back_to_memory(self, address: str):
        value = self.cache.get(address)
        if value and self.main_memory:
            logger.info(f"Writing back {address} to main memory")
            value_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            await self.main_memory.set(f"cache_mem:{address}", value_str)

    async def _collect_metrics_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(30)
                logger.info(f"[Metrics] Hits: {self.internal_metrics['cache_hits']}, Misses: {self.internal_metrics['cache_misses']}, InvSent: {self.internal_metrics['invalidations_sent']}, InvRecv: {self.internal_metrics['invalidations_received']}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                 logger.warning(f"Metrics loop error: {e}")

    async def _apply_raft_command(self, command: Dict[str, Any]):
        logger.debug(f"CacheNode received Raft command: {command.get('type')}, but does not act on it.")
        pass

    async def _persist_raft_state(self, data: str) -> None:
        try:
            if not self.redis_client:
                 logger.warning("No redis client to persist raft state")
                 return
            await self.redis_client.set(f"raft:{self.node_id}:state_json", data)
        except Exception as e:
            logger.error(f"Failed to persist raft state to redis: {e}", exc_info=True)

    async def _load_raft_state(self) -> Optional[str]:
        try:
            if not self.redis_client:
                 logger.warning("No redis client to load raft state from")
                 return None
            data = await self.redis_client.get(f"raft:{self.node_id}:state_json")
            if data:
                 return data
        except Exception as e:
            logger.error(f"Failed to load raft state from redis: {e}", exc_info=True)
        return None
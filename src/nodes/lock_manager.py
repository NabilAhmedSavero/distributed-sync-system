import asyncio
import time
import logging
from typing import Dict, Set, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
from aiohttp import web
from redis.asyncio import Redis

from .base_node import BaseNode, NodeConfig, NodeState
from ..consensus.raft import RaftNode, RaftState
from ..utils import metrics

logger = logging.getLogger(__name__)


class LockMode(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class LockRequest:
    resource_id: str
    mode: LockMode
    requester_id: str
    timeout: float = 10.0
    future: Optional[asyncio.Future] = None


class DeadlockDetector:
    def __init__(self):
        self.wait_for_graph: Dict[str, Set[str]] = defaultdict(set)
        
    def add_edge(self, waiter: str, holder: str):
        self.wait_for_graph[waiter].add(holder)
    
    def remove_edge(self, waiter: str, holder: str):
        if waiter in self.wait_for_graph:
            self.wait_for_graph[waiter].discard(holder)
            
    def remove_node(self, node: str):
        self.wait_for_graph.pop(node, None)
        for _, holders in self.wait_for_graph.items():
            holders.discard(node)

    def detect_deadlock(self) -> Optional[list]:
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: list) -> Optional[list]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in list(self.wait_for_graph.get(node, [])):
                if neighbor not in visited:
                    result = dfs(neighbor, path[:])
                    if result: return result
                elif neighbor in rec_stack:
                    try:
                        cycle_start = path.index(neighbor)
                        return path[cycle_start:] + [neighbor]
                    except ValueError: 
                        return path + [neighbor] 
            
            rec_stack.remove(node)
            return None
        
        for node in list(self.wait_for_graph.keys()):
            if node not in visited:
                cycle = dfs(node, [])
                if cycle: return cycle
        
        return None


class DistributedLockManager(BaseNode):
    
    def __init__(self, config: NodeConfig):
        super().__init__(config)
        
        self.raft: Optional[RaftNode] = None 
        
        self.pending_requests: Dict[str, asyncio.Queue[LockRequest]] = defaultdict(asyncio.Queue)
        self.deadlock_detector = DeadlockDetector()
        
        self.internal_metrics.update({
            'locks_acquired_redis': 0,
            'locks_released_redis': 0,
            'deadlocks_detected': 0,
            'lock_timeouts': 0
        })

    def _register_routes(self, app: web.Application):
        app.add_routes([
            web.post('/raft-vote', self._handle_raft_vote),
            web.post('/raft-append', self._handle_raft_append),
            web.post('/lock/acquire', self._handle_acquire_lock_api),
            web.post('/lock/release', self._handle_release_lock_api),
            web.get('/lock/status/{resource_id}', self._handle_get_status_api),
            web.get('/lock/status', self._handle_get_all_status_api),
        ])

    async def _on_start(self):
        if not self.redis_client:
             logger.error("Redis client not initialized before RaftNode creation.")
             raise RuntimeError("Redis client required for RaftNode")

        peer_ids = list(self.peers.keys()) 
        logger.info(f"Initializing RaftNode for LockManager with peers: {peer_ids}")
        
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
        
        self._tasks.append(
            asyncio.create_task(self._deadlock_detection_loop())
        )
    
    async def _on_stop(self):
        if self.raft:
             await self.raft.stop()

    async def _on_peer_failed(self, peer_id: str):
        logger.warning(f"Peer {peer_id} failed. Raft leader will handle releasing its locks.")
        if self.raft and self.raft.state == RaftState.LEADER:
            await self._release_locks_by_owner(peer_id) 
    
    async def _on_peer_recovered(self, peer_id: str):
        logger.info(f"Peer {peer_id} recovered")

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

    async def _handle_acquire_lock_api(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200

        if not self.raft or self.raft.state != RaftState.LEADER:
            response_data = {"status": "error", "message": "Raft not ready or not leader"}
            status_code = 503
        else:
            try:
                data = await request.json()
                lock_request = LockRequest(
                    resource_id=data['resource_id'],
                    mode=LockMode(data['mode']),
                    requester_id=data['requester_id'],
                    timeout=float(data.get('timeout', 10.0))
                )
                
                success = await self.acquire_lock(lock_request)
                
                if success:
                    response_data = {"status": "acquired"}
                    status_code = 200
                else:
                    response_data = {"status": "timeout_or_failed"}
                    status_code = 408
            except Exception as e:
                logger.error(f"Error in acquire_lock_api: {e}", exc_info=True)
                metrics.increment_errors(self.node_id, "handle_acquire_lock_error")
                response_data = {"status": "error", "message": str(e)}
                status_code = 400
        
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_release_lock_api(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200

        if not self.raft or self.raft.state != RaftState.LEADER:
             response_data = {"status": "error", "message": "Raft not ready or not leader"}
             status_code = 503
        else:
            try:
                data = await request.json()
                resource_id = data['resource_id']
                owner_id = data['owner_id']
                
                success = await self.release_lock(resource_id, owner_id)
                
                if success:
                    response_data = {"status": "released"}
                    status_code = 200
                else:
                    response_data = {"status": "failed_not_owner_or_not_locked"}
                    status_code = 400
            except Exception as e:
                logger.error(f"Error in release_lock_api: {e}", exc_info=True)
                metrics.increment_errors(self.node_id, "handle_release_lock_error")
                response_data = {"status": "error", "message": str(e)}
                status_code = 400

        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)

    async def _handle_get_status_api(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        
        try:
            resource_id = request.match_info.get('resource_id')
            if not resource_id:
                response_data = {"error": "Resource ID required"}
                status_code = 400
            else:
                response_data = await self.get_lock_status(resource_id)
        except Exception as e:
             logger.error(f"Error getting lock status: {e}", exc_info=True)
             metrics.increment_errors(self.node_id, "handle_get_status_error")
             response_data = {"status": "error", "message": str(e)}
             status_code = 500
             
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)
        
    async def _handle_get_all_status_api(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        response_data = {}
        status_code = 200
        
        try:
             response_data = await self.get_all_locks()
        except Exception as e:
             logger.error(f"Error getting all locks status: {e}", exc_info=True)
             metrics.increment_errors(self.node_id, "handle_get_all_status_error")
             response_data = {"status": "error", "message": str(e)}
             status_code = 500
             
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return web.json_response(response_data, status=status_code)


    async def _apply_raft_command(self, command: Dict[str, Any]):
        cmd_type = command.get('type')
        
        if cmd_type == 'ACQUIRE_LOCK':
            resource_id = command['resource_id']
            owner_id = command['owner_id']
            mode = command['mode']
            
            lock_key = f"lock:{resource_id}"
            owner_key = f"locks_by_owner:{owner_id}"
            
            async with self.redis_client.pipeline(transaction=True) as pipe:
                pipe.hset(lock_key, mapping={
                    "owner_id": owner_id,
                    "mode": mode,
                    "acquired_at": command.get('acquired_at', time.time())
                })
                pipe.sadd(owner_key, resource_id)
                results = await pipe.execute()
                if all(results):
                     self.internal_metrics['locks_acquired_redis'] += 1
                     logger.info(f"Raft Apply: Lock {resource_id} acquired by {owner_id}")
                else:
                     logger.error(f"Raft Apply FAILED: Lock acquire {resource_id} by {owner_id}")

        elif cmd_type == 'RELEASE_LOCK':
            resource_id = command['resource_id']
            owner_id = command['owner_id']
            
            lock_key = f"lock:{resource_id}"
            owner_key = f"locks_by_owner:{owner_id}"

            async with self.redis_client.pipeline(transaction=True) as pipe:
                pipe.delete(lock_key)
                pipe.srem(owner_key, resource_id)
                results = await pipe.execute()
                if results and results[0] and results[0] > 0: 
                     self.internal_metrics['locks_released_redis'] += 1
                     logger.info(f"Raft Apply: Lock {resource_id} released by {owner_id}")
                     await self._process_pending_requests(resource_id)
                else:
                     logger.warning(f"Raft Apply: Lock release {resource_id} by {owner_id} - lock already gone?")

    async def acquire_lock(self, request: LockRequest) -> bool:
        if not self.raft or self.raft.state != RaftState.LEADER:
            return False
        
        loop = asyncio.get_running_loop()
        request.future = loop.create_future()
        try:
            can_grant_now = await self._can_grant_lock_redis(request.resource_id, request.mode)
            if can_grant_now:
                success = await self._grant_lock_via_raft(request)
                if not request.future.done():
                    request.future.set_result(success)
            else:
                logger.info(f"Lock {request.resource_id} busy, queuing request for {request.requester_id}")
                await self.pending_requests[request.resource_id].put(request)
                lock_data = await self.redis_client.hgetall(f"lock:{request.resource_id}")
                owner_id = None
                if lock_data:
                    owner_id = lock_data.get('owner_id')
                    if isinstance(owner_id, (bytes, bytearray)):
                        owner_id = owner_id.decode()
                if owner_id:
                    self.deadlock_detector.add_edge(request.requester_id, owner_id)

            await asyncio.wait_for(request.future, timeout=request.timeout)
            return request.future.result()
        except asyncio.TimeoutError:
            logger.warning(f"Lock timeout for {request.requester_id} on {request.resource_id}")
            self.internal_metrics['lock_timeouts'] += 1
            if not request.future.done():
                request.future.set_result(False)
            self._remove_from_pending(request)
            return False
        except asyncio.CancelledError:
            logger.warning(f"Lock acquire cancelled for {request.requester_id}")
            if not request.future.done():
                request.future.set_result(False)
            self._remove_from_pending(request)
            return False
        finally:
            lock_data = await self.redis_client.hgetall(f"lock:{request.resource_id}")
            owner_id = None
            if lock_data:
                owner_id = lock_data.get('owner_id')
                if isinstance(owner_id, (bytes, bytearray)):
                    owner_id = owner_id.decode()
            if owner_id:
                self.deadlock_detector.remove_edge(request.requester_id, owner_id)


    async def _can_grant_lock_redis(self, resource_id: str, mode: LockMode) -> bool:
        lock_data = await self.redis_client.hgetall(f"lock:{resource_id}")
        if not lock_data:
            return True

        existing_mode_str = lock_data.get('mode')
        if isinstance(existing_mode_str, (bytes, bytearray)):
             existing_mode_str = existing_mode_str.decode()
             
        if not existing_mode_str:
             existing_mode_enum = LockMode.SHARED
        else:
             try:
                 existing_mode_enum = LockMode(existing_mode_str)
             except ValueError:
                  logger.warning(f"Invalid lock mode '{existing_mode_str}' found in Redis for {resource_id}")
                  existing_mode_enum = LockMode.SHARED

        if mode == LockMode.EXCLUSIVE:
            return False
        if mode == LockMode.SHARED:
            return existing_mode_enum == LockMode.SHARED
        return False

    async def _grant_lock_via_raft(self, request: LockRequest) -> bool:
        if not self.raft:
            return False
        command = {
            'type': 'ACQUIRE_LOCK',
            'resource_id': request.resource_id,
            'mode': request.mode.value,
            'owner_id': request.requester_id,
            'acquired_at': time.time(),
        }
        return await self.raft.submit_command(command)

    async def release_lock(self, resource_id: str, owner_id: str) -> bool:
        if not self.raft or self.raft.state != RaftState.LEADER:
            return False

        lock_data = await self.redis_client.hgetall(f"lock:{resource_id}")
        owner = None
        if lock_data:
            owner = lock_data.get('owner_id')
            if isinstance(owner, (bytes, bytearray)):
                 owner = owner.decode()

        if not lock_data or owner != owner_id:
            logger.warning(f"Release failed: {owner_id} does not own {resource_id} or lock does not exist.")
            return False

        command = {
            'type': 'RELEASE_LOCK',
            'resource_id': resource_id,
            'owner_id': owner_id
        }
        return await self.raft.submit_command(command)

    async def _release_locks_by_owner(self, owner_id: str):
        if not self.raft:
            return
        owner_key = f"locks_by_owner:{owner_id}"
        resource_ids_raw = await self.redis_client.smembers(owner_key)
        resource_ids = [rid.decode() if isinstance(rid, (bytes, bytearray)) else rid for rid in resource_ids_raw]
        
        raft_submissions = []
        for resource_id in resource_ids:
            logger.info(f"Submitting release command for lock {resource_id} held by failed node {owner_id}")
            command = {
                'type': 'RELEASE_LOCK',
                'resource_id': resource_id,
                'owner_id': owner_id
            }
            raft_submissions.append(self.raft.submit_command(command))
        await asyncio.gather(*raft_submissions, return_exceptions=True)
        self.deadlock_detector.remove_node(owner_id)
            
    async def _process_pending_requests(self, resource_id: str):
        if resource_id not in self.pending_requests or not self.raft or self.raft.state != RaftState.LEADER:
            return
        q = self.pending_requests[resource_id]
        
        while not q.empty():
            request = None
            try:
                can_grant_exclusive = await self._can_grant_lock_redis(resource_id, LockMode.EXCLUSIVE)
                if not can_grant_exclusive: break 

                request = q.get_nowait()
                if request.future and not request.future.done():
                    can_grant_now = await self._can_grant_lock_redis(request.resource_id, request.mode)
                    if can_grant_now:
                        logger.info(f"Processing pending request for {request.requester_id} on {resource_id}")
                        success = await self._grant_lock_via_raft(request)
                        request.future.set_result(success)
                    else:
                        logger.warning(f"Could not grant pending lock for {request.requester_id}, re-queuing.")
                        await self._requeue_request(request)
                        break 
                elif request.future and request.future.done():
                    logger.debug(f"Skipping already resolved/cancelled request for {request.requester_id}")

            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"Error processing pending request: {e}", exc_info=True)
                if request and request.future and not request.future.done():
                    request.future.set_result(False) 
                 
    async def _requeue_request(self, request: LockRequest):
        await self.pending_requests[request.resource_id].put(request)

    def _remove_from_pending(self, request_to_remove: LockRequest):
        q: Optional[asyncio.Queue] = self.pending_requests.get(request_to_remove.resource_id)
        if not q: return
        
        temp = []
        while True:
            try:
                item = q.get_nowait()
                temp.append(item)
            except asyncio.QueueEmpty:
                break
        
        new_q = asyncio.Queue()
        for item in temp:
            if item is not request_to_remove and item.requester_id != request_to_remove.requester_id : 
                new_q.put_nowait(item)
        self.pending_requests[request_to_remove.resource_id] = new_q


    async def _deadlock_detection_loop(self):
        while not getattr(self, "_shutdown_event", asyncio.Event()).is_set():
            await asyncio.sleep(5)
            if not self.raft or self.raft.state != RaftState.LEADER:
                continue
            try:
                cycle = self.deadlock_detector.detect_deadlock()
                if cycle:
                    self.internal_metrics['deadlocks_detected'] += 1
                    logger.warning(f"Deadlock detected: {' -> '.join(cycle)}")
                    if len(cycle) > 1:
                        victim = cycle[-2] 
                        logger.warning(f"Resolving deadlock by releasing locks held by {victim}")
                        await self._release_locks_by_owner(victim)
                    else:
                        logger.error(f"Deadlock cycle detected with only one node: {cycle}")
            except Exception as e:
                logger.error(f"Error in deadlock detection: {e}", exc_info=True)

    async def get_lock_status(self, resource_id: str) -> Dict[str, Any]:
        lock_data = await self.redis_client.hgetall(f"lock:{resource_id}")
        owner_holds = []
        owner_id = None
        if lock_data:
            owner_id = lock_data.get('owner_id')
            if isinstance(owner_id, (bytes, bytearray)):
                owner_id = owner_id.decode()
        if owner_id:
            members = await self.redis_client.smembers(f"locks_by_owner:{owner_id}")
            owner_holds = [m.decode() if isinstance(m, (bytes, bytearray)) else m for m in members]

        pending_count = "N/A (Not Leader)"
        if self.raft and self.raft.state == RaftState.LEADER:
            pending_count = self.pending_requests.get(resource_id, asyncio.Queue()).qsize()

        pretty_lock_data = None
        if lock_data:
            pretty_lock_data = {
                (k.decode() if isinstance(k, (bytes, bytearray)) else k):
                (v.decode() if isinstance(v, (bytes, bytearray)) else v)
                for k, v in lock_data.items()
            }

        return {
            'resource_id': resource_id,
            'lock_data': pretty_lock_data,
            'owner_holds': owner_holds,
            'pending_requests': pending_count
        }

    async def get_all_locks(self) -> Dict[str, Any]:
        lock_keys_raw = await self.redis_client.keys("lock:*")
        lock_keys = [k.decode() if isinstance(k, (bytes, bytearray)) else k for k in lock_keys_raw]
        
        all_status = {}
        for key in lock_keys:
            try:
                resource_id = key.split(":", 1)[1]
                all_status[resource_id] = await self.get_lock_status(resource_id)
            except Exception:
                logger.warning(f"Malformed lock key found: {key}")

        owner_keys_raw = await self.redis_client.keys("locks_by_owner:*")
        owner_keys = [k.decode() if isinstance(k, (bytes, bytearray)) else k for k in owner_keys_raw]
        locks_by_owner = {}
        for key in owner_keys:
            try:
                owner_id = key.split(":", 1)[1]
                members = await self.redis_client.smembers(key)
                locks_by_owner[owner_id] = [m.decode() if isinstance(m, (bytes, bytearray)) else m for m in members]
            except Exception:
                logger.warning(f"Malformed owner key found: {key}")

        raft_state_info = None
        if self.raft:
            try:
                raft_state_info = self.raft.get_state()
            except Exception as e:
                logger.error(f"Could not get Raft state: {e}")

        return {
            'total_locks': len(lock_keys),
            'locks_by_resource': all_status,
            'locks_by_owner': locks_by_owner,
            'raft_state': raft_state_info,
            'metrics': self.internal_metrics
        }
    
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
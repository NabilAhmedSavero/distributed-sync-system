import asyncio
import time
import hashlib
import logging


from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
import json
from aiohttp import web
from redis.asyncio import Redis
import redis.exceptions

from .base_node import BaseNode, NodeConfig, NodeState
from ..consensus.raft import RaftNode, RaftState
from ..utils import metrics

logger = logging.getLogger(__name__)


class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node
        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Added node {node} to consistent hash ring")

    def remove_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:{i}"
            hash_value = self._hash(virtual_key)
            if hash_value in self.ring:
                del self.ring[hash_value]
        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Removed node {node} from consistent hash ring")

    def get_node(self, key: str) -> Optional[str]:
        if not self.ring: return None
        hash_value = self._hash(key)
        for ring_key in self.sorted_keys:
            if hash_value <= ring_key: return self.ring[ring_key]
        return self.ring[self.sorted_keys[0]]


class DistributedQueue(BaseNode):
    
    def __init__(self, config: NodeConfig):
        super().__init__(config)
        
        self.hash_ring: Optional[ConsistentHash] = None 
        self.raft: Optional[RaftNode] = None 
        
        self.internal_metrics.update({ 
            'messages_published': 0,
            'messages_consumed': 0,
            'messages_acknowledged': 0,
            'requests_forwarded': 0
        })

    def _register_routes(self, app: web.Application):
        app.add_routes([
            web.post('/raft-vote', self._handle_raft_vote),
            web.post('/raft-append', self._handle_raft_append),
            web.post('/queue/publish', self._handle_publish),
            web.post('/queue/consume', self._handle_consume),
            web.post('/queue/ack', self._handle_ack),
            web.get('/queue/stats', self._handle_get_stats)
        ])

    async def _on_start(self):
        if not self.redis_client:
             logger.error("Redis client not initialized.")
             raise RuntimeError("Redis client required")
             
        all_node_ids = [self.node_id] + list(self.peers.keys()) 
        logger.info(f"Initializing ConsistentHash for QueueNode with nodes: {all_node_ids}")
        self.hash_ring = ConsistentHash(all_node_ids)
        
        logger.info(f"Initializing RaftNode for QueueNode with peers: {all_node_ids[1:]}")
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=list(self.peers.keys()),
            send_rpc_callback=self._send_rpc,
            apply_command_callback=self._apply_raft_command,
            persist_callback=self._persist_raft_state, 
            load_callback=self._load_raft_state,     
            redis_client=self.redis_client
        )
        await self.raft.start() 
        
        logger.info(f"DistributedQueue node {self.node_id} started")
    
    async def _on_stop(self):
        if self.raft:
            await self.raft.stop()
        logger.info(f"DistributedQueue node {self.node_id} stopped")

    async def _on_peer_failed(self, peer_id: str):
        logger.warning(f"Peer {peer_id} failed, removing from hash ring")
        if self.hash_ring:
            self.hash_ring.remove_node(peer_id)
    
    async def _on_peer_recovered(self, peer_id: str):
        logger.info(f"Peer {peer_id} recovered, adding to hash ring")
        if self.hash_ring:
            self.hash_ring.add_node(peer_id)

    async def _handle_raft_vote(self, request: web.Request) -> web.Response:
        if not self.raft: return web.json_response({"error": "Raft not initialized"}, status=503)
        try:
            data = await request.json()
            response = await self.raft.handle_vote_request(data)
            return web.json_response(response)
        except Exception as e:
            logger.error(f"Error handling vote request: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=400)

    async def _handle_raft_append(self, request: web.Request) -> web.Response:
        if not self.raft: return web.json_response({"error": "Raft not initialized"}, status=503)
        try:
            data = await request.json()
            response = await self.raft.handle_append_entries(data)
            return web.json_response(response)
        except Exception as e:
            logger.error(f"Error handling append entries request: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=400)

    def _get_responsible_node(self, topic: str) -> str:
        if not self.hash_ring:
             logger.error("Hash ring not initialized!")
             return self.node_id 
        node = self.hash_ring.get_node(topic)
        if node is None:
             return self.node_id
        return node
        
    def _get_stream_key(self, topic: str) -> str:
        return f"queue_stream:{topic}"

    async def _forward_request(self, responsible_node_id: str, endpoint: str, data: Dict) -> Optional[Dict]:
        if responsible_node_id == self.node_id:
            logger.error("Should not forward request to self")
            return None

        self.internal_metrics['requests_forwarded'] += 1
        logger.debug(f"Forwarding request to {responsible_node_id} at {endpoint}")
        return await self._send_rpc(responsible_node_id, endpoint, data)

    async def _handle_publish(self, request: web.Request) -> web.Response:
        start_req_time = time.monotonic()
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        response_data = {}
        status_code = 200
        try:
            data = await request.json()
            topic = data['topic']
            message_data = data.get('message', {})
            
            responsible_node = self._get_responsible_node(topic)
            if responsible_node != self.node_id:
                response = await self._forward_request(responsible_node, '/queue/publish', data)
                if response:
                     response_data = response
                else:
                     response_data = {"status": "error", "message": "Failed to forward"}
                     status_code = 500
            else:
                msg_id = await self.publish_local(topic, message_data)
                response_data = {"status": "published", "message_id": msg_id}
                
        except Exception as e:
            logger.error(f"Error handling publish: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, error_type=f"handler_{endpoint.replace('/','_')}_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
        finally:
             duration = time.monotonic() - start_req_time
             metrics.record_request_latency(self.node_id, endpoint, method, duration)
             return web.json_response(response_data, status=status_code)


    async def _handle_consume(self, request: web.Request) -> web.Response:
        start_req_time = time.monotonic()
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        response_data = {}
        status_code = 200
        try:
            data = await request.json()
            topic = data['topic']
            consumer_group = data['consumer_group']
            consumer_id = data['consumer_id']
            count = int(data.get('count', 10))

            responsible_node = self._get_responsible_node(topic)
            if responsible_node != self.node_id:
                response = await self._forward_request(responsible_node, '/queue/consume', data)
                if response:
                     response_data = response
                else:
                     response_data = {"status": "error", "message": "Failed to forward"}
                     status_code = 500
            else:
                messages = await self.consume_local(topic, consumer_group, consumer_id, count)
                response_data = {"status": "ok", "messages": messages}

        except Exception as e:
            logger.error(f"Error handling consume: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, error_type=f"handler_{endpoint.replace('/','_')}_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
        finally:
             duration = time.monotonic() - start_req_time
             metrics.record_request_latency(self.node_id, endpoint, method, duration)
             return web.json_response(response_data, status=status_code)

    async def _handle_ack(self, request: web.Request) -> web.Response:
        start_req_time = time.monotonic()
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        response_data = {}
        status_code = 200
        try:
            data = await request.json()
            topic = data['topic']
            consumer_group = data['consumer_group']
            message_id = data['message_id']
            
            responsible_node = self._get_responsible_node(topic)
            if responsible_node != self.node_id:
                response = await self._forward_request(responsible_node, '/queue/ack', data)
                if response:
                     response_data = response
                else:
                     response_data = {"status": "error", "message": "Failed to forward"}
                     status_code = 500
            else:
                success = await self.acknowledge_local(topic, consumer_group, message_id)
                response_data = {"status": "acknowledged" if success else "failed"}
                if not success: status_code = 400 

        except Exception as e:
            logger.error(f"Error handling ack: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, error_type=f"handler_{endpoint.replace('/','_')}_error")
            response_data = {"status": "error", "message": str(e)}
            status_code = 400
        finally:
             duration = time.monotonic() - start_req_time
             metrics.record_request_latency(self.node_id, endpoint, method, duration)
             return web.json_response(response_data, status=status_code)
            
    async def _handle_get_stats(self, request: web.Request) -> web.Response:
         start_req_time = time.monotonic()
         endpoint = request.path
         method = request.method
         metrics.increment_requests_received(self.node_id, endpoint, method)
         response_data = {}
         status_code = 200
         try:
              response_data = await self.get_queue_stats()
         except Exception as e:
              logger.error(f"Error getting queue stats: {e}", exc_info=True)
              metrics.increment_errors(self.node_id, "handler_get_stats_error")
              response_data = {"status": "error", "message": str(e)}
              status_code = 500
         finally:
              duration = time.monotonic() - start_req_time
              metrics.record_request_latency(self.node_id, endpoint, method, duration)
              return web.json_response(response_data, status=status_code)


    async def publish_local(self, topic: str, data: Any) -> str:
        stream_key = self._get_stream_key(topic)
        message_body = {"data": json.dumps(data), "timestamp": str(time.time())}
        
        msg_id = await self.redis_client.xadd(stream_key, message_body)
        self.internal_metrics['messages_published'] += 1
        logger.info(f"Message {msg_id} published locally to topic {topic}")
        return msg_id

    async def consume_local(self, topic: str, consumer_group: str, consumer_id: str, count: int = 10) -> List[Dict]:
        stream_key = self._get_stream_key(topic)
        
        try:
            await self.redis_client.xgroup_create(stream_key, consumer_group, id="0", mkstream=True)
            logger.info(f"Ensured consumer group {consumer_group} exists for topic {topic}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Error creating/checking consumer group: {e}")
                raise
        
        stream_data = await self.redis_client.xreadgroup(
            consumer_group,
            consumer_id,
            {stream_key: '>'}, 
            count=count,
            block=2000 
        )
        
        messages = []
        if stream_data:
            for _, msg_list in stream_data:
                for msg_id, msg_data_bytes in msg_list:
                    msg_data = {k: v for k, v in msg_data_bytes.items()}
                    try:
                        data_content = json.loads(msg_data.get('data', '{}'))
                    except json.JSONDecodeError:
                        data_content = msg_data.get('data') 

                    messages.append({
                        "id": msg_id,
                        "data": data_content,
                        "timestamp": msg_data.get('timestamp')
                    })
        
        if messages:
             self.internal_metrics['messages_consumed'] += len(messages)
             logger.info(f"Consumer {consumer_id} consumed {len(messages)} messages from {topic}")
        return messages

    async def acknowledge_local(self, topic: str, consumer_group: str, message_id: str) -> bool:
        stream_key = self._get_stream_key(topic)
        ack_count = await self.redis_client.xack(stream_key, consumer_group, message_id)
        
        if ack_count > 0:
            self.internal_metrics['messages_acknowledged'] += 1
            logger.debug(f"Message {message_id} acknowledged locally for group {consumer_group}")
            return True
        logger.warning(f"Failed to acknowledge message {message_id} for group {consumer_group}")
        return False

    async def get_queue_stats(self) -> Dict[str, Any]:
         stream_keys_raw = await self.redis_client.keys(f"queue_stream:*")
         stream_keys = [k.decode() if isinstance(k, (bytes, bytearray)) else k for k in stream_keys_raw]
         topics_info = {}
         total_messages = 0
         
         for key in stream_keys:
             try:
                 topic = key.split(":", 1)[1]
                 info = await self.redis_client.xinfo_stream(key)
                 length = info.get('length', 0)
                 groups_raw = await self.redis_client.xinfo_groups(key)
                 groups = [g['name'].decode() if isinstance(g['name'], (bytes, bytearray)) else g['name'] for g in groups_raw]
                 topics_info[topic] = {
                     'length': length,
                     'groups': len(groups),
                     'group_names': groups,
                     'last_generated_id': info.get('last-generated-id')
                 }
                 total_messages += length
             except IndexError:
                 logger.warning(f"Malformed stream key found: {key}")
             except Exception as e:
                 logger.error(f"Failed to get info for stream {key}: {e}")

         responsible_topics = []
         if self.hash_ring:
              responsible_topics = [t for t in topics_info if self._get_responsible_node(t) == self.node_id]

         return {
             'node_id': self.node_id,
             'topics_on_this_node': list(topics_info.keys()),
             'total_messages_on_this_node': total_messages,
             'topics_info': topics_info,
             'metrics': self.internal_metrics, 
             'responsible_for': responsible_topics
         }
    
    async def _apply_raft_command(self, command: Dict[str, Any]):
        logger.debug(f"QueueNode received Raft command: {command.get('type')}, but does not act on it.")
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
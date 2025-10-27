import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

from aiohttp import web
from redis.asyncio import Redis 

from ..utils import metrics
from ..utils.metrics import generate_metrics_output
from ..communication.message_passing import AsyncHttpClient
from ..communication.failure_detector import HeartbeatFailureDetector

logger = logging.getLogger(__name__)


class NodeState(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class NodeConfig:
    node_id: str
    host: str
    port: int
    cluster_nodes: List[str]
    redis_host: str = "localhost"
    redis_port: int = 6379
    heartbeat_interval: float = 2.0
    request_timeout: float = 5.0


class BaseNode(ABC):
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id
        self.state = NodeState.INITIALIZING
        self.peers: Dict[str, Dict[str, Any]] = {}
        self.internal_metrics = { 
            'requests_received': 0, 
            'requests_sent': 0,     
            'errors': 0,          
        }
        self._tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        
        self.http_client: AsyncHttpClient = AsyncHttpClient(timeout_seconds=config.request_timeout)
        self.failure_detector = HeartbeatFailureDetector(heartbeat_timeout=config.heartbeat_interval * 3)
        self.start_time = time.monotonic()
        
        self.web_server: Optional[web.Application] = None
        self.web_runner: Optional[web.AppRunner] = None
        self.redis_client: Optional[Redis] = None 

    async def start(self):
        logger.info(f"Starting node {self.node_id} on {self.config.host}:{self.config.port}")
        try:
            await self._initialize()
            
            self.web_runner = web.AppRunner(self.web_server)
            await self.web_runner.setup()
            site = web.TCPSite(self.web_runner, self.config.host, self.config.port)
            await site.start()
            logger.info(f"HTTP server running on {self.config.host}:{self.config.port}")

            self.state = NodeState.RUNNING
            
            self._tasks.append(
                asyncio.create_task(self._heartbeat_loop())
            )
            self._tasks.append(
                asyncio.create_task(self._health_check_loop())
            )
            self._tasks.append(
                 asyncio.create_task(metrics.update_system_metrics_periodically(self.node_id))
            )
            
            await self._on_start()
            
            logger.info(f"Node {self.node_id} started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start node {self.node_id}: {e}", exc_info=True)
            self.state = NodeState.FAILED
            await self.stop()
            raise

    async def stop(self):
        if self.state == NodeState.STOPPED:
            return
            
        logger.info(f"Stopping node {self.node_id}")
        self.state = NodeState.STOPPED
        self._shutdown_event.set()
        
        if self.web_runner:
            await self.web_runner.cleanup()
        
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        if self.http_client:
             await self.http_client.stop() 
             self.http_client = None
            
        if self.redis_client:
            await self.redis_client.close()
            
        await self._on_stop()
        logger.info(f"Node {self.node_id} stopped")

    async def wait_for_shutdown(self):
        await self._shutdown_event.wait()

    async def _initialize(self):
        await self.http_client.start() 
        
        try:
            self.redis_client = Redis( 
                host=self.config.redis_host,
                port=self.config.redis_port,
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
        
        self.web_server = web.Application()

        async def handle_metrics(request: web.Request) -> web.Response:
            return web.Response(text=generate_metrics_output(), content_type="text/plain")
            
        self.web_server.add_routes([
            web.post('/heartbeat', self._handle_heartbeat),
            web.get('/health', self._handle_health),
            web.get('/metrics', handle_metrics) 
        ])
        self._register_routes(self.web_server)
        
        self.peers = {} 
        for node_addr in self.config.cluster_nodes:
            if ':' in node_addr:
                parts = node_addr.split(':')
                node_id, host, port_str = None, "localhost", None
                
                if len(parts) == 3: 
                    node_id, host, port_str = parts
                elif len(parts) == 2: 
                    node_id, port_str = parts
                    host = "localhost" 
                else:
                    logger.warning(f"Invalid cluster_node format: {node_addr}")
                    continue
                
                try:
                    port = int(port_str)
                    if node_id != self.node_id:
                        comm_host = host if host != "0.0.0.0" else node_id 
                        
                        self.peers[node_id] = {
                            'id': node_id,
                            'host': comm_host, 
                            'port': port,
                            'url': f"http://{comm_host}:{port}",
                            'last_heartbeat': 0, 
                            'alive': False
                        }
                        logger.debug(f"Parsed peer: {node_id} at http://{comm_host}:{port}") 
                except ValueError:
                     logger.warning(f"Invalid port in cluster_node format: {node_addr}")
                     continue
                     
        logger.info(f"Peers initialized: {list(self.peers.keys())}")
        
        metrics.initialize_metrics(self.node_id, self.start_time, len(self.peers))


    async def _send_rpc(self, peer_id: str, endpoint: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.state != NodeState.RUNNING or not self.http_client:
            logger.warning(f"Cannot send RPC while not running or http_client missing. State: {self.state}")
            return None
            
        peer_info = self.peers.get(peer_id)
        if not peer_info:
            logger.warning(f"Attempted to send RPC to unknown peer: {peer_id}")
            return None

        url = f"{peer_info['url']}{endpoint}"
        self.internal_metrics['requests_sent'] += 1 
        
        metrics.increment_requests_sent(self.node_id, peer_id, endpoint)
        logger.debug(f"Sending RPC POST to {url} - Peer: {peer_id}, Endpoint: {endpoint}") 
        
        response_data = await self.http_client.post(url, data) 
        
        if response_data is None:
             metrics.increment_errors(self.node_id, error_type=f"rpc_fail_{peer_id}_{endpoint.replace('/','_')}") 
             self.internal_metrics['errors'] += 1
             logger.debug(f"RPC POST to {url} failed or timed out.") 
        else:
             logger.debug(f"Received RPC response from {url}: {str(response_data)[:100]}...") 
        
        return response_data

    async def _heartbeat_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}", exc_info=True)

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self._check_peer_health()
                now = time.monotonic() 
                for pid, pinfo in self.peers.items():
                     if pinfo['alive']:
                          logger.debug(f"Peer {pid} is alive. Last heartbeat: {now - pinfo.get('last_heartbeat', now):.2f}s ago.")
                     else:
                          logger.debug(f"Peer {pid} is dead. Last heartbeat: {now - pinfo.get('last_heartbeat', 0):.2f}s ago.")

                await asyncio.sleep(self.config.heartbeat_interval * 2) 
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}", exc_info=True)

    async def _send_heartbeats(self):
        tasks = [
            self._send_heartbeat(peer_id) for peer_id in self.peers.keys()
        ]
        await asyncio.gather(*tasks, return_exceptions=True) 

    async def _check_peer_health(self):
        newly_failed, newly_recovered = self.failure_detector.update_peer_liveness(self.peers)

        for peer_id in newly_failed:
            logger.warning(f"Peer {peer_id} detected as FAILED")
            await self._on_peer_failed(peer_id)

        for peer_id in newly_recovered:
            logger.info(f"Peer {peer_id} detected as RECOVERED")
            await self._on_peer_recovered(peer_id)
            
        alive_count = len([p for p in self.peers.values() if p.get('alive', False)])
        metrics.update_alive_peers(self.node_id, alive_count)

    async def _handle_health(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        
        response = web.json_response({
                "status": "ok",
                "node_id": self.node_id,
                "state": self.state.value
            })
            
        duration = time.monotonic() - start_req_time
        metrics.record_request_latency(self.node_id, endpoint, method, duration)
        return response

    async def _handle_heartbeat(self, request: web.Request) -> web.Response:
        endpoint = request.path
        method = request.method
        metrics.increment_requests_received(self.node_id, endpoint, method)
        start_req_time = time.monotonic()
        
        response = None
        status_code = 200
        
        try:
            data = await request.json()
            peer_id = data.get('node_id')
            logger.debug(f"Received heartbeat from {peer_id}") 
            if peer_id and peer_id in self.peers:
                self.peers[peer_id]['last_heartbeat'] = time.monotonic() 
                logger.debug(f"Updated last_heartbeat for {peer_id}")
                
            response = {"status": "ack", "node_id": self.node_id}
            
        except Exception as e:
            logger.error(f"Error processing heartbeat: {e}", exc_info=True)
            metrics.increment_errors(self.node_id, error_type=f"handler_{endpoint.replace('/','_')}_error")
            response = {"status": "error", "message": str(e)}
            status_code = 400
            
        finally:
             duration = time.monotonic() - start_req_time
             metrics.record_request_latency(self.node_id, endpoint, method, duration) 
             if response is None: 
                  response = {"status": "error", "message": "Internal server error"}
                  status_code = 500
             logger.debug(f"Responding to heartbeat from {peer_id}: {response}") 
             return web.json_response(response, status=status_code)

    @abstractmethod
    def _register_routes(self, app: web.Application):
        pass

    @abstractmethod
    async def _on_start(self):
        pass

    @abstractmethod
    async def _on_stop(self):
        pass

    async def _send_heartbeat(self, peer_id: str, peer_info: Dict[str, Any] = None):
        logger.debug(f"Sending heartbeat to {peer_id}") 
        response_data = await self._send_rpc(
            peer_id,
            '/heartbeat',
            {'node_id': self.node_id}
        )
        if response_data is None:
             logger.debug(f"Failed to send heartbeat or get response from {peer_id}")

    @abstractmethod
    async def _on_peer_failed(self, peer_id: str):
        pass

    @abstractmethod
    async def _on_peer_recovered(self, peer_id: str):
        pass
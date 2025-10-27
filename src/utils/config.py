import os
import logging
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

@dataclass
class AppConfig:
    node_id: str
    node_type: str
    host: str
    port: int
    cluster_nodes: List[str]
    redis_host: str
    redis_port: int
    log_level: str
    heartbeat_interval: float
    request_timeout: float
    election_timeout_min: int
    election_timeout_max: int
    raft_heartbeat_interval: int

DEFAULT_CLUSTER_NODES="node1:localhost:8001,node2:localhost:8002,node3:localhost:8003"

def load_config() -> AppConfig:
    load_dotenv()

    raw_cluster_nodes = os.getenv("CLUSTER_NODES", DEFAULT_CLUSTER_NODES)
    cluster_nodes_list = [node.strip() for node in raw_cluster_nodes.split(',') if node.strip()]

    config = AppConfig(
        node_id=os.getenv("NODE_ID", "node1"),
        node_type=os.getenv("NODE_TYPE", "lock_manager"),
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8001)),
        cluster_nodes=cluster_nodes_list,
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", 6379)),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", 2.0)),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", 5.0)),
        election_timeout_min=int(os.getenv("ELECTION_TIMEOUT_MIN", 150)),
        election_timeout_max=int(os.getenv("ELECTION_TIMEOUT_MAX", 300)),
        raft_heartbeat_interval=int(os.getenv("RAFT_HEARTBEAT_INTERVAL", 50)),
    )
    logger.debug(f"Configuration loaded: {config}")
    return config

app_config = load_config()
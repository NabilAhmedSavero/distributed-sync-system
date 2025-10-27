import time
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest
import psutil
import asyncio
import logging

logger = logging.getLogger(__name__)

registry = CollectorRegistry()

REQUESTS_RECEIVED = Counter(
    'node_requests_received_total',
    'Total number of requests received by the node',
    ['node_id', 'endpoint', 'method'],
    registry=registry
)

REQUESTS_SENT = Counter(
    'node_requests_sent_total',
    'Total number of RPC requests sent by the node',
    ['node_id', 'peer_id', 'endpoint'],
    registry=registry
)

ERRORS_TOTAL = Counter(
    'node_errors_total',
    'Total number of errors encountered',
    ['node_id', 'error_type'],
    registry=registry
)

UPTIME_SECONDS = Gauge(
    'node_uptime_seconds',
    'Node uptime in seconds',
    ['node_id'],
    registry=registry
)

ALIVE_PEERS = Gauge(
    'node_alive_peers_count',
    'Number of currently alive peers',
    ['node_id'],
    registry=registry
)

TOTAL_PEERS = Gauge(
    'node_total_peers_count',
    'Total number of configured peers',
    ['node_id'],
    registry=registry
)

CPU_USAGE_PERCENT = Gauge(
    'node_cpu_usage_percent',
    'Current CPU usage percentage',
    ['node_id'],
    registry=registry
)

MEMORY_USAGE_BYTES = Gauge(
    'node_memory_usage_bytes',
    'Current memory usage in bytes',
    ['node_id'],
    registry=registry
)

REQUEST_LATENCY = Histogram(
    'node_request_latency_seconds',
    'Histogram of request processing latency',
    ['node_id', 'endpoint', 'method'],
    registry=registry
)


def initialize_metrics(node_id: str, start_time: float, total_peers: int):
    UPTIME_SECONDS.labels(node_id=node_id).set_function(lambda: time.monotonic() - start_time)
    TOTAL_PEERS.labels(node_id=node_id).set(total_peers)
    ALIVE_PEERS.labels(node_id=node_id).set(0)
    try:
        CPU_USAGE_PERCENT.labels(node_id=node_id).set(psutil.cpu_percent())
        MEMORY_USAGE_BYTES.labels(node_id=node_id).set(psutil.Process().memory_info().rss)
    except Exception as e:
        logger.warning(f"Could not initialize system metrics: {e}")


def update_alive_peers(node_id: str, count: int):
    ALIVE_PEERS.labels(node_id=node_id).set(count)

def increment_requests_received(node_id: str, endpoint: str, method: str):
    REQUESTS_RECEIVED.labels(node_id=node_id, endpoint=endpoint, method=method).inc()

def increment_requests_sent(node_id: str, peer_id: str, endpoint: str):
    REQUESTS_SENT.labels(node_id=node_id, peer_id=peer_id, endpoint=endpoint).inc()

def increment_errors(node_id: str, error_type: str = "general"):
    ERRORS_TOTAL.labels(node_id=node_id, error_type=error_type).inc()

def record_request_latency(node_id: str, endpoint: str, method: str, duration_seconds: float):
    REQUEST_LATENCY.labels(node_id=node_id, endpoint=endpoint, method=method).observe(duration_seconds)

async def update_system_metrics_periodically(node_id: str, interval_seconds: int = 15):
    process = psutil.Process()
    while True:
        try:
            cpu = psutil.cpu_percent()
            mem = process.memory_info().rss
            CPU_USAGE_PERCENT.labels(node_id=node_id).set(cpu)
            MEMORY_USAGE_BYTES.labels(node_id=node_id).set(mem)
        except Exception as e:
            logger.warning(f"Failed to update system metrics: {e}")
        
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            logger.info("System metrics update task cancelled.")
            break

def generate_metrics_output() -> str:
    return generate_latest(registry).decode('utf-8')
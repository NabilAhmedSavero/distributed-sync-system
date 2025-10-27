import asyncio
import logging
import os
import signal
import time
from typing import Optional
from dotenv import load_dotenv

from src.utils.config import app_config
from src.nodes.base_node import BaseNode, NodeState, NodeConfig
from src.nodes.lock_manager import DistributedLockManager
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import DistributedCacheNode

logging.basicConfig(
    level=app_config.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
current_node: Optional[BaseNode] = None


async def shutdown(sig, loop):
    """Handle graceful shutdown on signal (SIGINT, SIGTERM)."""
    logger.info(f"Received exit signal {sig.name}...")
    global current_node

    if current_node and current_node.state != NodeState.STOPPED:
        logger.info(f"Shutting down node {current_node.node_id}...")
        await current_node.stop()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    logger.info(f"Waiting for {len(tasks)} tasks to cancel...")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def main():
    """Main async entry point for node initialization."""
    global current_node

    node_config = NodeConfig(
        node_id=app_config.node_id,
        host=app_config.host,
        port=app_config.port,
        cluster_nodes=app_config.cluster_nodes,
        redis_host=app_config.redis_host,
        redis_port=app_config.redis_port,
        heartbeat_interval=app_config.heartbeat_interval,
        request_timeout=app_config.request_timeout,
    )

    node_type = app_config.node_type

    # Initialize correct node type
    if node_type == "lock_manager":
        logger.info(f"Initializing DistributedLockManager: {app_config.node_id}")
        current_node = DistributedLockManager(node_config)
    elif node_type == "queue":
        logger.info(f"Initializing DistributedQueue: {app_config.node_id}")
        current_node = DistributedQueue(node_config)
    elif node_type == "cache":
        logger.info(f"Initializing DistributedCacheNode: {app_config.node_id}")
        current_node = DistributedCacheNode(node_config)
    else:
        logger.error(f"Unknown NODE_TYPE: {node_type}")
        return

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s, loop)))

    try:
        await current_node.start()
        await current_node.wait_for_shutdown()
        # Keep the node alive until a signal stops it
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        logger.info("Main task cancelled during startup or run.")
    except Exception as e:
        logger.error(f"Node {app_config.node_id} failed: {e}", exc_info=True)
    finally:
        logger.info("Main loop finished.")
        if current_node and current_node.state != NodeState.STOPPED:
            logger.info("Ensuring node stop is called in finally block...")
            await asyncio.sleep(0.1)
            if current_node.state != NodeState.STOPPED:
                await current_node.stop()


if __name__ == "__main__":
    logger.info("Starting application...")
    try:
        asyncio.run(main())
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user (KeyboardInterrupt).")
    except asyncio.CancelledError:
        logger.info("Main execution cancelled.")
    finally:
        logger.info("Application finished.")

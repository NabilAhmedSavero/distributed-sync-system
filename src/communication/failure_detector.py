import time
from typing import Dict, Any, List, Tuple

class HeartbeatFailureDetector:
    def __init__(self, heartbeat_timeout: float):
        self.heartbeat_timeout = heartbeat_timeout

    def update_peer_liveness(self, peers: Dict[str, Dict[str, Any]]) -> Tuple[List[str], List[str]]:
        current_time = time.monotonic()
        newly_failed = []
        newly_recovered = []

        for peer_id, peer_info in peers.items():
            last_heartbeat = peer_info.get('last_heartbeat', 0)
            time_since_heartbeat = current_time - last_heartbeat
            was_alive = peer_info.get('alive', False)
            is_now_alive = time_since_heartbeat < self.heartbeat_timeout

            if was_alive and not is_now_alive:
                newly_failed.append(peer_id)
            elif not was_alive and is_now_alive:
                newly_recovered.append(peer_id)

            peer_info['alive'] = is_now_alive 

        return newly_failed, newly_recovered
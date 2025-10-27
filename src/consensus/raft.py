import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Awaitable, Callable
from redis.asyncio import Redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {"term": self.term, "index": self.index, "command": self.command, "timestamp": self.timestamp}


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: Optional[int] = None


class RaftNode:

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        send_rpc_callback: Callable[[str, str, Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]],
        apply_command_callback: Callable[[Dict[str, Any]], Awaitable[None]],
        persist_callback: Callable[[str], Awaitable[None]],
        load_callback: Callable[[], Awaitable[Optional[str]]],
        redis_client: Optional[Redis] = None, 
        election_timeout_min: int = 1500,
        election_timeout_max: int = 3000,
        heartbeat_interval: int = 150,
    ):
        self.node_id = node_id
        self.peers = peers[:] 
        self.send_rpc_callback = send_rpc_callback
        self._apply_command = apply_command_callback
        self._persist_callback = persist_callback
        self._load_callback = load_callback
        self.redis = redis_client

        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = -1  
        self.last_applied = -1

        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval

        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = time.monotonic()

        self._tasks: List[asyncio.Task] = []
        self._stop = False
        self._vote_lock = asyncio.Lock()
        self._election_lock = asyncio.Lock()
        self._apply_event = asyncio.Event()
        self._leader_id: Optional[str] = None

        if self.redis:
            logger.info(f"[{self.node_id}] Redis client attached to RaftNode")

        logger.info(f"[{self.node_id}] RaftNode init peers={self.peers}")

    def _random_election_timeout(self) -> float:
        return random.uniform(self.election_timeout_min / 1000.0, self.election_timeout_max / 1000.0)

    async def persist_state(self):
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [entry.to_dict() for entry in self.log],
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }
        try:
            if self.redis:
                await self.redis.set(f"raft:{self.node_id}:state", json.dumps(state))
            await self._persist_callback(json.dumps(state))
        except Exception as e:
            logger.warning(f"[{self.node_id}] persist_state failed: {e}")

    async def load_persistent_state(self):
        try:
            data = await self._load_callback()
            if (not data) and self.redis:
                raw = await self.redis.get(f"raft:{self.node_id}:state")
                if raw:
                    data = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw

            if not data:
                return

            state = json.loads(data)
            self.current_term = int(state.get("current_term", 0))
            self.voted_for = state.get("voted_for")
            self.log = [LogEntry(**entry) for entry in state.get("log", [])]
            self.commit_index = int(state.get("commit_index", -1))
            self.last_applied = int(state.get("last_applied", -1))
            logger.info(f"[{self.node_id}] Loaded persistent state: term={self.current_term}, voted_for={self.voted_for}, log_len={len(self.log)}")
        except Exception as e:
            logger.warning(f"[{self.node_id}] load_persistent_state failed: {e}")

    async def start(self):
        await self.load_persistent_state()
        startup_jitter = random.uniform(0.0, min(1.0, self.election_timeout_max / 1000.0))
        logger.info(f"[{self.node_id}] startup jitter {startup_jitter:.3f}s")
        await asyncio.sleep(startup_jitter)
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = time.monotonic()
        self._tasks.append(asyncio.create_task(self._main_loop()))
        self._tasks.append(asyncio.create_task(self._apply_committed_entries_loop()))
        logger.info(f"[{self.node_id}] started as {self.state.value} (election_timeout={self.election_timeout:.3f}s)")

    async def stop(self):
        self._stop = True
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info(f"[{self.node_id}] stopped")

    async def _main_loop(self):
        while not self._stop:
            try:
                if self.state == RaftState.FOLLOWER:
                    await self._follower_loop()
                elif self.state == RaftState.CANDIDATE:
                    await self._candidate_loop()
                elif self.state == RaftState.LEADER:
                    await self._leader_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[{self.node_id}] main loop error: {e}")
                await asyncio.sleep(1)

    async def _follower_loop(self):
        while self.state == RaftState.FOLLOWER and not self._stop:
            if time.monotonic() - self.last_heartbeat > self.election_timeout:
                logger.info(f"[{self.node_id}] follower -> candidate (timeout)")
                self.state = RaftState.CANDIDATE
                return
            await asyncio.sleep(0.05)

    async def _candidate_loop(self):
        async with self._election_lock:
            self.current_term += 1
            self.voted_for = self.node_id
            await self.persist_state()

            votes_received = 1
            total_nodes = len(self.peers) + 1
            votes_needed = total_nodes // 2 + 1
            logger.info(f"[{self.node_id}] starting election term={self.current_term} votes_needed={votes_needed}")

            vote_tasks = {p: asyncio.create_task(self._send_vote_request(p)) for p in self.peers if p != self.node_id}
            
            if not vote_tasks and votes_needed <= 1:
                logger.info(f"[{self.node_id}] single node cluster, becoming leader")
                await self._become_leader()
                return

            try:
                done, pending = await asyncio.wait(list(vote_tasks.values()), timeout=self.election_timeout, return_when=asyncio.ALL_COMPLETED)
            except ValueError as e:
                logger.warning(f"[{self.node_id}] election wait failed (no peers?): {e}")
                done, pending = set(), set()
            
            for t in pending:
                t.cancel()

            for peer, task in list(vote_tasks.items()):
                if task in done:
                    try:
                        resp = task.result()
                        if resp and resp.vote_granted:
                            votes_received += 1
                        elif resp and resp.term > self.current_term:
                            logger.info(f"[{self.node_id}] discovered higher term {resp.term}, stepping down")
                            await self._become_follower(resp.term)
                            return
                    except Exception as e:
                        logger.debug(f"[{self.node_id}] vote task failed for {peer}: {e}")

            if self.state != RaftState.CANDIDATE:
                return

            if votes_received >= votes_needed:
                logger.info(f"[{self.node_id}] won election term={self.current_term} votes={votes_received}")
                await self._become_leader()
            else:
                logger.info(f"[{self.node_id}] election failed (got {votes_received}/{votes_needed}), backoff and retry")
                await asyncio.sleep(random.uniform(0.05, min(0.5, self.election_timeout)))

    async def _leader_loop(self):
        while self.state == RaftState.LEADER and not self._stop:
            await self._send_heartbeats()
            await asyncio.sleep(self.heartbeat_interval / 1000.0)

    async def _send_vote_request(self, peer_id: str) -> Optional[VoteResponse]:
        req = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": len(self.log) - 1,
            "last_log_term": self.log[-1].term if self.log else 0,
        }
        try:
            resp = await self.send_rpc_callback(peer_id, "/raft-vote", req)
        except Exception as e:
            logger.debug(f"[{self.node_id}] vote request to {peer_id} failed: {e}")
            return None
        if not resp:
            return None
        try:
            return VoteResponse(**resp)
        except Exception:
            return VoteResponse(term=resp.get("term", self.current_term), vote_granted=resp.get("vote_granted", False))

    async def _send_append_entries(self, peer_id: str, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        req = {
            "term": request.term,
            "leader_id": request.leader_id,
            "prev_log_index": request.prev_log_index,
            "prev_log_term": request.prev_log_term,
            "entries": request.entries,
            "leader_commit": request.leader_commit,
        }
        try:
            resp = await self.send_rpc_callback(peer_id, "/raft-append", req)
        except Exception as e:
            logger.debug(f"[{self.node_id}] append entries to {peer_id} failed: {e}")
            return None
        if not resp:
            return None
        try:
            return AppendEntriesResponse(**resp)
        except Exception:
            return AppendEntriesResponse(term=resp.get("term", self.current_term), success=resp.get("success", False), match_index=resp.get("match_index"))

    async def _send_heartbeats(self):
        tasks = []
        for peer in self.peers:
            if peer == self.node_id:
                continue
            
            next_idx = self.next_index.get(peer, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
            
            entries_to_send = self.log[next_idx:]
            
            req = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[e.to_dict() for e in entries_to_send],
                leader_commit=self.commit_index,
            )
            tasks.append((peer, asyncio.create_task(self._send_append_entries(peer, req))))
            
        if not tasks:
            await self._update_commit_index() 
            return

        results = []
        for peer, task in tasks:
            try:
                res = await task
                results.append((peer, res))
            except Exception as e:
                logger.debug(f"[{self.node_id}] heartbeat to {peer} error: {e}")
                results.append((peer, None))

        for peer, res in results:
            if isinstance(res, AppendEntriesResponse):
                if res.success:
                    new_match_index = len(self.log) - 1 
                    self.match_index[peer] = new_match_index
                    self.next_index[peer] = new_match_index + 1
                elif res.term > self.current_term:
                    await self._become_follower(res.term)
                    return 
                else: 
                    self.next_index[peer] = max(0, self.next_index[peer] - 1) 
                    
        await self._update_commit_index()


    async def handle_vote_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            req = VoteRequest(**request_data)
        except Exception as e:
            logger.error(f"[{self.node_id}] Invalid VoteRequest data: {e}")
            return {"term": self.current_term, "vote_granted": False}

        resp = await self._on_vote_request(req)
        return {"term": resp.term, "vote_granted": resp.vote_granted}

    async def handle_append_entries(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            req = AppendEntriesRequest(
                term=int(request_data.get("term", 0)),
                leader_id=request_data.get("leader_id", ""),
                prev_log_index=int(request_data.get("prev_log_index", -1)),
                prev_log_term=int(request_data.get("prev_log_term", 0)),
                entries=request_data.get("entries", []),
                leader_commit=int(request_data.get("leader_commit", -1)),
            )
        except Exception as e:
            logger.error(f"[{self.node_id}] Invalid AppendEntriesRequest data: {e}")
            return {"term": self.current_term, "success": False}

        resp = await self._on_append_entries(req)
        return {"term": resp.term, "success": resp.success, "match_index": resp.match_index}

    async def _on_vote_request(self, req: VoteRequest) -> VoteResponse:
        async with self._vote_lock:
            if req.term < self.current_term:
                logger.debug(f"[{self.node_id}] denied vote to {req.candidate_id} (term {req.term} < {self.current_term})")
                return VoteResponse(term=self.current_term, vote_granted=False)

            if req.term > self.current_term:
                await self._become_follower(req.term)

            up_to_date = self._is_up_to_date(req.last_log_index, req.last_log_term)
            if (self.voted_for is None or self.voted_for == req.candidate_id) and up_to_date:
                self.voted_for = req.candidate_id
                self.last_heartbeat = time.monotonic()
                await self.persist_state()
                logger.info(f"[{self.node_id}] granted vote to {req.candidate_id} for term {req.term}")
                return VoteResponse(term=self.current_term, vote_granted=True)

            logger.debug(f"[{self.node_id}] denied vote to {req.candidate_id} (voted_for={self.voted_for}, up_to_date={up_to_date})")
            return VoteResponse(term=self.current_term, vote_granted=False)

    async def _on_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        if req.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False)

        if req.term > self.current_term or self.state == RaftState.CANDIDATE:
            await self._become_follower(req.term)

        self.last_heartbeat = time.monotonic()
        self._leader_id = req.leader_id

        log_len = len(self.log)
        if req.prev_log_index >= 0:
            if req.prev_log_index >= log_len:
                logger.warning(f"[{self.node_id}] consistency check fail: prev_log_index ({req.prev_log_index}) >= log length ({log_len})")
                return AppendEntriesResponse(term=self.current_term, success=False, match_index=log_len -1)
            if self.log[req.prev_log_index].term != req.prev_log_term:
                logger.warning(f"[{self.node_id}] consistency check fail: term mismatch at index {req.prev_log_index} (local={self.log[req.prev_log_index].term}, remote={req.prev_log_term})")
                return AppendEntriesResponse(term=self.current_term, success=False, match_index=self.commit_index)

        insert_at = req.prev_log_index + 1
        new_entries_idx_start = -1
        
        for i, entry_dict in enumerate(req.entries):
            current_log_idx = insert_at + i
            if current_log_idx < log_len:
                if self.log[current_log_idx].term != entry_dict.get("term", 0):
                    logger.info(f"[{self.node_id}] truncating log from index {current_log_idx}")
                    self.log = self.log[:current_log_idx]
                    new_entries_idx_start = i
                    break
            else:
                 new_entries_idx_start = i
                 break
        
        if new_entries_idx_start != -1:
             for entry_dict in req.entries[new_entries_idx_start:]:
                  cmd = entry_dict.get("command", {})
                  entry = LogEntry(term=int(entry_dict.get("term", self.current_term)), index=len(self.log), command=cmd)
                  self.log.append(entry)
        
        elif not req.entries and req.prev_log_index == log_len - 1:
             pass 
        
        elif req.entries: 
             logger.debug(f"[{self.node_id}] appending {len(req.entries)} new entries from index {insert_at}")
             for entry_dict in req.entries:
                  cmd = entry_dict.get("command", {})
                  entry = LogEntry(term=int(entry_dict.get("term", self.current_term)), index=len(self.log), command=cmd)
                  self.log.append(entry)


        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, len(self.log) - 1)
            self._apply_event.set()
        
        await self.persist_state() 
        
        match_idx = len(self.log) - 1
        return AppendEntriesResponse(term=self.current_term, success=True, match_index=match_idx)

    async def _become_follower(self, term: int):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.state = RaftState.FOLLOWER
        self._leader_id = None
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._random_election_timeout()
        await self.persist_state()
        logger.info(f"[{self.node_id}] became follower term={self.current_term}")

    async def _become_leader(self):
        self.state = RaftState.LEADER
        self._leader_id = self.node_id
        last_index = len(self.log) - 1
        for p in self.peers:
            self.next_index[p] = last_index + 1
            self.match_index[p] = -1
        self.match_index[self.node_id] = last_index
        self.next_index[self.node_id] = last_index + 1
        
        logger.info(f"[{self.node_id}] became leader term={self.current_term}")
        await self._send_heartbeats() 

    async def submit_command(self, command: Dict[str, Any]) -> bool:
        if self.state != RaftState.LEADER:
            logger.warning(f"[{self.node_id}] submit_command rejected (not leader, state={self.state.value})")
            return False
            
        entry_index = len(self.log)
        entry = LogEntry(term=self.current_term, index=entry_index, command=command)
        self.log.append(entry)
        await self.persist_state() 
        
        self.match_index[self.node_id] = entry_index
        self.next_index[self.node_id] = entry_index + 1
        
        asyncio.create_task(self._replicate_log_entry(entry))
        return True

    async def _replicate_log_entry(self, entry: LogEntry):
        tasks = {}
        for peer in self.peers:
            if peer == self.node_id:
                continue
            
            next_idx = self.next_index.get(peer, entry.index)
            if entry.index < next_idx:
                 logger.debug(f"[{self.node_id}] skipping replication of index {entry.index} to {peer} (next_index={next_idx})")
                 continue 

            prev_index = next_idx - 1
            prev_term = self.log[prev_index].term if prev_index >= 0 and prev_index < len(self.log) else 0
            
            entries_to_send = self.log[next_idx : entry.index + 1] 
            
            if not entries_to_send and entry.index < next_idx:
                 logger.debug(f"[{self.node_id}] No entries to send to {peer} for index {entry.index} (next_index={next_idx})")
                 continue
                 
            logger.debug(f"[{self.node_id}] Replicating {len(entries_to_send)} entries (starting {next_idx}) to {peer}")

            req = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=[e.to_dict() for e in entries_to_send],
                leader_commit=self.commit_index,
            )
            tasks[peer] = asyncio.create_task(self._send_append_entries(peer, req))

        if not tasks:
             await self._update_commit_index()
             return

        results = {}
        for peer, task in tasks.items():
            try:
                res = await task
                results[peer] = res
            except Exception as e:
                logger.debug(f"[{self.node_id}] replicate to {peer} error: {e}")
                results[peer] = None

        if self.state != RaftState.LEADER: return 

        for peer, res in results.items():
            if isinstance(res, AppendEntriesResponse) and res.success:
                idx = res.match_index if res.match_index is not None else entry.index
                self.match_index[peer] = idx
                self.next_index[peer] = idx + 1
            elif isinstance(res, AppendEntriesResponse) and not res.success:
                 if res.term > self.current_term:
                      await self._become_follower(res.term)
                      return
                 else:
                      self.next_index[peer] = max(0, self.next_index[peer] - 1)

        await self._update_commit_index()

    async def _update_commit_index(self):
        if self.state != RaftState.LEADER:
            return
            
        majority_needed = (len(self.peers) + 1) // 2 + 1 
        
        start_index = self.commit_index + 1
        for N in range(len(self.log) - 1, self.commit_index, -1): 
            if self.log[N].term == self.current_term:
                match_count = 1 + sum(1 for p in self.peers if self.match_index.get(p, -1) >= N)
                if match_count >= majority_needed:
                    if N > self.commit_index:
                         logger.info(f"[{self.node_id}] commit_index advanced to {N}")
                         self.commit_index = N
                         self._apply_event.set()
                    break 
                     
        if self.commit_index > self.last_applied:
             self._apply_event.set()


    async def _apply_committed_entries_loop(self):
        while not self._stop:
            try:
                await self._apply_event.wait()
                self._apply_event.clear()
                
                apply_batch = []
                while self.last_applied < self.commit_index:
                    apply_idx = self.last_applied + 1
                    try:
                         entry = self.log[apply_idx]
                         apply_batch.append(entry)
                         self.last_applied = apply_idx 
                    except IndexError:
                         logger.error(f"[{self.node_id}] apply index {apply_idx} out of bounds (log len: {len(self.log)})!")
                         self.last_applied = self.commit_index 
                         apply_batch = [] 
                         break
                
                if apply_batch:
                    logger.debug(f"[{self.node_id}] applying {len(apply_batch)} entries up to index {self.last_applied}")
                    for entry in apply_batch:
                        try:
                            await self._apply_command(entry.command)
                        except Exception as e:
                            logger.exception(f"[{self.node_id}] apply command failed for index {entry.index}: {e}")
                            self.last_applied = entry.index - 1 
                            break 
                    
                    await self.persist_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[{self.node_id}] apply loop error: {e}")
                await asyncio.sleep(1)

    def _is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        if not self.log:
            return True
        my_last_log_term = self.log[-1].term
        my_last_log_index = self.log[-1].index 
        
        if last_log_term != my_last_log_term:
            return last_log_term > my_last_log_term
        return last_log_index >= my_last_log_index

    def get_state(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "leader_id": self._leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": list(self.peers),
            "next_index": self.next_index if self.state == RaftState.LEADER else {},
            "match_index": self.match_index if self.state == RaftState.LEADER else {},
        }
# node.py
import argparse
import hashlib
import json
import socket
import threading
import time
import traceback
import uuid
import random
import sys
from dataclasses import dataclass

from networkx.classes import neighbors
# Add this near the top of your imports
import logging

# Configure a shared log file with explicit delay/flush settings
logging.basicConfig(
    filename='simulation.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Force the internal buffer to flush immediately (helps prevent interleaving)
for handler in logging.root.handlers:
    handler.flush()

# --- PHASE 1: Data Structures ---
fullyRandomPeers = False
TOTALNODES = 20 # TODO set with conf
clusterCount = int(TOTALNODES // 5)

@dataclass
class NodeConfig:
    fanout: int
    ttl: int
    peer_limit: int
    ping_interval: float
    peer_timeout: float
    seed: int
    bootstrapNode: None
    pull_interval: float
    ihave_max_ids: int
    pow_k: int

@dataclass
class PeerInfo:
    node_id: str
    addr: str
    last_seen: float

class GossipNode:
    def __init__(self, ip: str, port: int, config: NodeConfig):
        self.node_id: str = str(uuid.uuid4())
        self.self_addr: str = f"{ip}:{port}"
        self.config: NodeConfig = config
        self.peers: dict[str, PeerInfo] = {}
        self.seen_set: set[str] = set()

        # Phase 2: UDP Socket Setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))

        # Set random seed for reproducibility
        random.seed(self.config.seed)
        print(f"[*] Node started. ID: {self.self_addr} | Addr: {self.self_addr}")

        # Phase 4-1:
        self.pull_timer = threading.Thread(target=self.pull_loop, daemon=True)
        self.message_store: dict[str, dict] = {}  # msg_id -> full_message_dict

    def solve_pow(self) -> dict:
        """Finds a nonce such that SHA256(node_id + nonce) starts with '0' * k."""
        print(f"[*] Solving PoW (k={self.config.pow_k})...")
        start_time = time.time()
        nonce = 0
        prefix = '0' * self.config.pow_k

        while True:
            # Concatenate ID and nonce
            data = f"{self.node_id}{nonce}".encode()
            digest = hashlib.sha256(data).hexdigest()

            if digest.startswith(prefix):
                duration = time.time() - start_time
                print(f"[*] PoW solved in {duration:.2f}s! Nonce: {nonce}")
                return {
                    "nonce": nonce,
                    "k": self.config.pow_k,
                    "hash": digest
                }
            nonce += 1

    def verify_pow(self, peer_id: str, pow_data: dict) -> bool:
        """Verifies that the provided nonce satisfies the PoW requirement."""
        try:
            nonce = pow_data.get("nonce")
            k = pow_data.get("k")
            received_hash = pow_data.get("hash")

            # 1. Check if k meets our local difficulty requirement
            if k < self.config.pow_k:
                return False

            # 2. Recompute hash
            data = f"{peer_id}{nonce}".encode()
            actual_hash = hashlib.sha256(data).hexdigest()

            # 3. Verify prefix and integrity
            prefix = '0' * k
            return actual_hash.startswith(prefix) and actual_hash == received_hash
        except Exception:
            return False

    def pull_loop(self):
        while True:
            time.sleep(self.config.pull_interval)
            if not self.peers: continue

            # Select target neighbors
            targets = random.sample(list(self.peers.values()), min(self.config.fanout, len(self.peers)))

            # Prepare list of known msg_ids (limited by ihave_max_ids)
            known_ids = list(self.seen_set)[-self.config.ihave_max_ids:]
            ihave_msg = self.build_message("IHAVE", {"ids": known_ids})

            for target in targets:
                self.send_udp(ihave_msg, target.addr)

    def build_message(self, msg_type: str, payload: dict, msg_id: str = None, ttl: int = None) -> dict:
        if msg_id is None:
            msg_id = str(uuid.uuid4())
        if ttl is None:
            ttl = self.config.ttl

        return {
            "version": 1,
            "msg_id": msg_id,
            "msg_type": msg_type,
            "sender_id": self.node_id,
            "sender_addr": self.self_addr,
            "timestamp_ms": int(time.time() * 1000),
            "ttl": ttl,
            "payload": payload
        }

    def add_or_update_peer(self, peer_id: str, peer_addr: str):
        if peer_id == self.node_id:
            return

        # Check if this is a new connection before adding/updating
        is_new_peer = peer_id not in self.peers

        self.peers[peer_id] = PeerInfo(node_id=peer_id, addr=peer_addr, last_seen=time.time())

        # Log the link for the graph drawer
        if is_new_peer:
            logging.info(f"LINK {self.self_addr.split(':')[1]} {peer_addr.split(':')[1]}")

        # Enforce peer limit      # TODO will we ever need to drop ?
        if len(self.peers) > self.config.peer_limit:
            print("peer list is full, gonna have to remove a peer")
            oldest_peer_id = min(self.peers, key=lambda k: self.peers[k].last_seen)
            print(f"[-] Peer limit reached. Dropping oldest peer: {oldest_peer_id}")
            del self.peers[oldest_peer_id]

    def send_udp(self, msg_dict: dict, target_addr: str):
        """Helper to send JSON over UDP."""
        # Add a tiny jitter to prevent all nodes from hitting the log file at the exact same microsecond
        time.sleep(random.uniform(0, 0.01))
        try:
            ip, port_str = target_addr.split(":")
            port = int(port_str)
            data = json.dumps(msg_dict).encode('utf-8')
            if msg_dict['msg_type'] == 'GOSSIP':
                logging.info(f"SEND {self.self_addr.split(':')[1]} {msg_dict['msg_type']} {target_addr.split(':')[1]}")
            else:
                logging.info(f"SEND {self.self_addr.split(':')[1]} {msg_dict['msg_type']}")
            self.sock.sendto(data, (ip, port))
        except Exception as e:
            print(f"[!] Error sending to {target_addr}: {e}")

    # --- PHASE 2: Thread 1 - Listening Loop ---
    def listen_loop(self):
        """Listens for incoming UDP messages and routes them."""
        while True:
            try:
                data, addr = self.sock.recvfrom(4096) # 4096 is the maximum number of bytes the socket will read from one incoming UDP packet.
                msg = json.loads(data.decode('utf-8'))
                self.process_message(msg, f"{addr[0]}:{addr[1]}")
            except json.JSONDecodeError:
                print("[!] Received malformed JSON. Ignoring.")  # Don't crash on bad JSON
            except Exception as e:
                print(f"[!] Listen error: {e}")
                traceback.print_exc()

    def process_message(self, msg: dict, sender_ip_port: str):
        msg_type = msg.get("msg_type")
        sender_id = msg.get("sender_id")
        sender_addr = msg.get("sender_addr")
        msg_id = msg.get("msg_id")
        payload = msg.get("payload", {})

        if not all([msg_type, sender_id, sender_addr, msg_id]):
            return  # Invalid message format

        self.add_or_update_peer(sender_id, sender_addr)

        if msg_type == "HELLO":
            pow_data = payload.get("pow")
            if pow_data and self.verify_pow(sender_id, pow_data):
                print(f"[+] Valid PoW received. Adding peer {sender_addr}")
                self.add_or_update_peer(sender_id, sender_addr)
            else:
                print(f"[!] Invalid or missing PoW from {sender_addr}. Dropping request.")
                return

        elif msg_type == "GET_PEERS": # TODO node bazri
            if self.config.bootstrapNode is None: # TODO, reuse max_peers
                max_peers = min(msg.get("payload").get("max_peers"), len(self.peers))

                sender_port = int(str(self.peers.get(sender_id).addr).split(":")[1])
                same_mode, diff_mode = [], []
                all_mode = []
                for nid, peer in self.peers.items():
                    port = int(peer.addr.split(":")[1])
                    if port % clusterCount == sender_port % clusterCount:
                        same_mode.append((nid, peer))
                    else:
                        diff_mode.append((nid, peer))

                    all_mode.append((nid, peer))

                same_count = min(len(same_mode), max_peers * 3 // 4)
                diff_count = max_peers - same_count
                if diff_count > len(diff_mode):
                    same_count += diff_count - len(diff_mode)
                    diff_count = len(diff_mode)

                response = (random.sample(same_mode, same_count) +
                            random.sample(diff_mode, diff_count))
                if fullyRandomPeers:
                    response = random.sample(all_mode, max_peers)

                # Respond with chosen peers
                peers_list = [{"node_id": pid, "addr": p.addr} for (pid, p) in response]
                reply = self.build_message("PEERS_LIST", {"peers": peers_list})
                self.send_udp(reply, sender_addr)

        elif msg_type == "PEERS_LIST":
            for p in msg.get("payload", {}).get("peers", []):
                self.add_or_update_peer(p["node_id"], p["addr"])

        elif msg_type == "PING":
            reply = self.build_message("PONG", {"ping_id": msg.get("payload", {}).get("ping_id")})
            self.send_udp(reply, sender_addr)

        elif msg_type == "PONG":
            pass

        elif msg_type == "GOSSIP":
            if msg_id in self.seen_set:
                return  # Ignore duplicates
            self.message_store[msg_id] = msg
            self.seen_set.add(msg_id)
            logging.info(f"RECEIVE_GOSSIP {sender_addr.split(':')[1]} {self.self_addr.split(':')[1]}")
            print(f"\n[GOSSIP RECEIVED] From {sender_addr}: {msg.get('payload', {}).get('data')}")

            # Forwarding logic
            ttl = msg.get("ttl", 0) - 1
            if ttl > 0:
                self.forward_gossip(msg, ttl)

        elif msg_type == "IHAVE":
            remote_ids = msg.get("payload", {}).get("ids", [])
            missing_ids = [mid for mid in remote_ids if mid not in self.seen_set]

            if missing_ids:
                iwant_msg = self.build_message("IWANT", {"ids": missing_ids})
                self.send_udp(iwant_msg, sender_addr)

        elif msg_type == "IWANT":
            requested_ids = msg.get("payload", {}).get("ids", [])
            for mid in requested_ids:
                if mid in self.message_store:
                    response_msg = self.message_store[mid].copy()
                    response_msg["ttl"] = 1  # Direct response, no need for further gossip
                    self.send_udp(response_msg, sender_addr)
                    self.forward_gossip()

    def forward_gossip(self, original_msg: dict, new_ttl: int):
        """Forwards a GOSSIP message to 'fanout' random peers."""
        sender_id = original_msg.get("sender_id")
        msg_to_forward = original_msg.copy()
        msg_to_forward["ttl"] = new_ttl
        # Update sender info to self so peers can add us to their lists
        msg_to_forward["sender_id"] = self.node_id
        msg_to_forward["sender_addr"] = self.self_addr
        # Exclude the sender_id and the bazri node from the list of potential targets
        available_peers = [
            peer for pid, peer in self.peers.items()
            if pid != sender_id and peer.addr != "127.0.0.1:8000"
        ]
        # Choose random peers
        targets = random.sample(available_peers, min(self.config.fanout, len(available_peers)))

        for target in targets:
            self.send_udp(msg_to_forward, target.addr)

    # --- PHASE 2: Thread 2 - Maintenance Loop (PINGs & Timeouts) ---
    def maintenance_loop(self):
        """Periodically sends PINGs and removes dead peers."""
        while True:
            time.sleep(self.config.ping_interval)
            current_time = time.time()
            dead_peers = []

            for pid, peer in self.peers.items():
                if current_time - peer.last_seen > self.config.peer_timeout:
                    dead_peers.append(pid)
                else:
                    # Send PING
                    ping_msg = self.build_message("PING", {"ping_id": str(uuid.uuid4())})
                    self.send_udp(ping_msg, peer.addr)

            for pid in dead_peers:
                print(f"[-] Peer {self.peers[pid].addr} timed out. Removing.")
                del self.peers[pid]

    # --- PHASE 2: Bootstrap Logic ---
    def bootstrap(self, bootstrap_addr: str):
        if bootstrap_addr:
            pow_solution = self.solve_pow()
            print(f"[*] Bootstrapping to {bootstrap_addr}...")
            hello_msg = self.build_message("HELLO", {
                "capabilities": ["udp", "json"],
                "pow": pow_solution
            })
            self.send_udp(hello_msg, bootstrap_addr)

            get_peers_msg = self.build_message("GET_PEERS", {"max_peers": clusterCount})
            self.send_udp(get_peers_msg, bootstrap_addr)


# --- PHASE 2: Thread 3 - User Input & CLI ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gossip Node")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--bootstrap", type=str, default=None, help="Bootstrap node IP:Port")
    parser.add_argument("--fanout", type=int, default=3, help="Gossip fanout")
    parser.add_argument("--ttl", type=int, default=8, help="Message TTL")
    parser.add_argument("--peer-limit", type=int, default=20, help="Max peers to keep")
    parser.add_argument("--ping-interval", type=float, default=2.0, help="Seconds between PINGs")
    parser.add_argument("--peer-timeout", type=float, default=6.0, help="Seconds before dropping peer")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--pull-interval", type=float, default=2.0, help="Seconds between Pull cycles")
    parser.add_argument("--ihave-limit", type=int, default=32, help="Max IDs to send in IHAVE")
    parser.add_argument("--pow-k", type=int, default=4, help="PoW difficulty (number of leading zeros)")
    args = parser.parse_args()

    config = NodeConfig(
        fanout=args.fanout,
        ttl=args.ttl,
        peer_limit=args.peer_limit,
        ping_interval=args.ping_interval,
        peer_timeout=args.peer_timeout,
        seed=args.seed,
        bootstrapNode=args.bootstrap,
        pull_interval=args.pull_interval,
        ihave_max_ids=args.ihave_limit,
        pow_k=args.pow_k
    )

    # Initialize node
    node = GossipNode(ip="127.0.0.1", port=args.port, config=config)

    # Start threads
    threading.Thread(target=node.listen_loop, daemon=True).start()
    threading.Thread(target=node.maintenance_loop, daemon=True).start()
    node.pull_timer.start()

    # Bootstrap
    if args.bootstrap:
        node.bootstrap(args.bootstrap)

    # Main thread handles user input for creating new GOSSIP messages
    print("Type a message and press Enter to start a GOSSIP. Type 'exit' to quit.")
    while True:
        try:
            user_input = input()
            if user_input.lower() == 'exit':
                sys.exit(0)
            if user_input.strip():
                gossip_msg = node.build_message("GOSSIP", {"topic": "user_input", "data": user_input})
                logging.info(f"INJECT {node.self_addr.split(':')[1]} {gossip_msg['msg_id']}")
                node.seen_set.add(gossip_msg["msg_id"])  # Add our own msg to seen set
                node.forward_gossip(gossip_msg, node.config.ttl)
                print(f"[*] Started Gossip: {user_input}")
        except KeyboardInterrupt:
            sys.exit(0)
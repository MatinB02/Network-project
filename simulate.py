import subprocess
import time
import os
import uuid
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime

# Experiment parameters
# NETWORK_SIZES = [10, 20, 50]
# RUNS_PER_SIZE = 5

NETWORK_SIZES = [10]
RUNS_PER_SIZE = 1


BASE_PORT = 8000
RATIO = 0.95

def clear_log():
    if os.path.exists('simulation.log'):
        open('simulation.log', 'w').close()


def parse_and_analyze(n_nodes):
    """
    Analyzes simulation.log post-run.
    Calculates time and overhead exactly at the 95% convergence mark.
    """
    events = []
    if not os.path.exists('simulation.log'):
        return 0, 0

    with open('simulation.log', 'r') as f:
        for line in f:
            try:
                parts = line.strip().split(" - ")
                if len(parts) != 2: continue
                time_str, action_data = parts
                dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
                timestamp = dt.timestamp()
                action_parts = action_data.split(" ")
                events.append({
                    "time": timestamp,
                    "action": action_parts[0],
                    "node_id": action_parts[1],
                    "msg_id": action_parts[2] if len(action_parts) > 2 else None,
                    "msg_type": action_parts[2] if action_parts[0] == "SEND" else None
                })
            except Exception:
                continue

    events.sort(key=lambda x: x["time"])

    # 1. Find the injection time
    inject_time = None
    for e in events:
        if e["action"] == "INJECT":
            inject_time = e["time"]
            break

    if not inject_time:
        return 0, 0

    # 2. Track convergence and count overhead simultaneously
    threshold = int(n_nodes * RATIO)
    receivers = set()
    msgs_count = 0
    convergence_time_ms = 0

    # We iterate through events that happened AFTER injection
    for e in events:
        if e["time"] < inject_time:
            continue

        # Count all UDP sends as overhead until we reach convergence
        if e["action"] == "SEND":
            msgs_count += 1

        # Check for message receipt
        if e["action"] == "RECEIVE_GOSSIP" or e["action"] == "INJECT":
            receivers.add(e["node_id"])

            # Check if we just hit the 95% threshold
            if len(receivers) >= threshold:
                convergence_time_ms = (e["time"] - inject_time) * 1000
                # We stop counting overhead and time here
                break

    return convergence_time_ms, msgs_count


def run_simulation(n_nodes, seed_base):
    print(f"\n--- Starting simulation for N={n_nodes}, Seed={seed_base} ---")
    clear_log()
    processes = []

    # Start Nodes
    for i in range(n_nodes):
        port = BASE_PORT + i
        cmd = ["python", "node.py", "--port", str(port), "--seed", str(seed_base + i)]
        if i > 0:
            cmd += ["--bootstrap", f"127.0.0.1:{BASE_PORT}"]

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             text=True)
        processes.append(p)

    print(f"Waiting 5s for {n_nodes} nodes to stabilize...")
    time.sleep(5)

    print("Injecting Gossip message...")
    processes[9].stdin.write("Simulation Test Message\n")
    processes[9].stdin.flush()

    # Give a tiny buffer to let the INJECT log hit the disk
    time.sleep(0.5)

    # Reverted to static wait period: N seconds for N nodes
    wait_period = n_nodes/4
    print(f"Waiting {wait_period} seconds for propagation...")
    time.sleep(wait_period)

    # Terminate all
    for p in processes:
        p.terminate()
        p.wait()

    # Post-run Analysis
    c_time, overhead = parse_and_analyze(n_nodes)

    print(f"Results -> Convergence: {c_time:.2f} ms | Overhead: {overhead} messages")
    return c_time, overhead


def main():
    results_convergence = defaultdict(list)
    results_overhead = defaultdict(list)

    for n in NETWORK_SIZES:
        for run in range(RUNS_PER_SIZE):
            c_time, overhead = run_simulation(n, seed_base=100 * n + run)
            results_convergence[n].append(c_time)
            results_overhead[n].append(overhead)

    # Calculate Averages for plotting
    avg_convergence = [sum(results_convergence[n]) / RUNS_PER_SIZE for n in NETWORK_SIZES]
    avg_overhead = [sum(results_overhead[n]) / RUNS_PER_SIZE for n in NETWORK_SIZES]

    # Plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    ax1.plot(NETWORK_SIZES, avg_convergence, marker='o', linestyle='-', color='royalblue', linewidth=2)
    ax1.set_title('95% Convergence Time vs Network Size')
    ax1.set_xlabel('Number of Nodes (N)')
    ax1.set_ylabel('Time (ms)')
    ax1.grid(True, alpha=0.3)

    ax2.plot(NETWORK_SIZES, avg_overhead, marker='s', linestyle='-', color='crimson', linewidth=2)
    ax2.set_title('Message Overhead vs Network Size')
    ax2.set_xlabel('Number of Nodes (N)')
    ax2.set_ylabel('Total Messages Sent')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('phase3_results.png')
    print("\n[!] Simulation complete. Graph saved as 'phase3_results.png'.")
    plt.show()


if __name__ == "__main__":
    main()

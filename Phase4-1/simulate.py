# simulate.py
import subprocess
import time
import os
import math
from math import nan
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime

# Experiment parameters
NETWORK_SIZES = [10, 20, 50]
RUNS_PER_SIZE = 5

# simplified for now:
# NETWORK_SIZES = [50]
# RUNS_PER_SIZE = 5


BASE_PORT = 8000
RATIO = 0.95



def draw_network_graph(n_nodes, seed_base):
    """Parses the simulation.log to build and save a visual network graph."""
    if not os.path.exists('simulation.log'):
        print("[!] No simulation.log found to draw graph.")
        return

    G = nx.DiGraph()
    gossip_edges = set()  # To store edges where GOSSIP was sent

    with open('simulation.log', 'r') as f:
        for line in f:
            line = line.strip()
            if " - LINK " in line:
                try:
                    action_data = line.split(" - ")[1]
                    parts = action_data.split(" ")
                    node_a = parts[1]
                    node_b = parts[2]
                    G.add_edge(node_a, node_b)
                except Exception:
                    continue

            # New Logic: Detect GOSSIP transmissions
            elif " - SEND " in line and " GOSSIP " in line:
                try:
                    action_data = line.split(" - ")[1]
                    parts = action_data.split(" ")
                    # Format: SEND {src} GOSSIP {dest}
                    src = parts[1]
                    dest = parts[3]
                    gossip_edges.add((src, dest))
                    # Ensure nodes exist in graph even if LINK wasn't logged
                    if not G.has_edge(src, dest):
                        G.add_edge(src, dest)
                except Exception:
                    continue

    if G.number_of_nodes() == 0:
        print("[!] No links found in log. Graph is empty.")
        return

    # Node Coloring Logic
    node_colors = ['skyblue' if node == "8000" else 'lightgreen' for node in G.nodes()]

    # Edge Coloring Logic: Red if GOSSIP was sent, otherwise gray
    edge_colors = []
    for u, v in G.edges():
        if (u, v) in gossip_edges:
            edge_colors.append('red')
        else:
            edge_colors.append('gray')

    # Edge Width Logic: Make GOSSIP lines thicker
    edge_widths = [2.0 if (u, v) in gossip_edges else 1.0 for u, v in G.edges()]

    plt.figure(figsize=(12, 10))
    pos = nx.spring_layout(G, seed=seed_base)

    nx.draw(
        G, pos,
        with_labels=True,
        node_color=node_colors,
        edge_color=edge_colors,  # Applied our list of colors
        width=edge_widths,  # Applied our list of widths
        node_size=2000,
        font_size=10,
        font_weight='bold',
        arrows=True,
        arrowstyle='-|>',
        connectionstyle="arc3,rad=0.1"
    )

    plt.title(f"Gossip Topology (Red = Active Gossip Path)")
    plt.savefig(f'logs/{RATIO}/network_topology_{n_nodes}_{seed_base}.png', bbox_inches='tight')
    plt.close()
    print(f"[*] Network graph saved for N={n_nodes}.")

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
            receivers.add(e["msg_id"])   # the node that received the gossip

            # Check if we just hit the 95% threshold
            if len(receivers) >= threshold:
                convergence_time_ms = (e["time"] - inject_time) * 1000
                # We stop counting overhead and time here
                break

    return convergence_time_ms, msgs_count


def run_simulation(n_nodes, seed_base):
    print(f"\n--- Starting simulation for N={n_nodes}, Seed={seed_base} ---")
    clear_log()
    time.sleep(0.1)
    processes = []

    # Start Nodes
    for i in range(n_nodes+1): # +1 for the bazri node
        port = BASE_PORT + i
        cmd = ["python", "node.py", "--port", str(port), "--seed", str(seed_base + i)]
        if i > 0:
            cmd += ["--bootstrap", f"127.0.0.1:{BASE_PORT}"]

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             text=True)
        time.sleep(0.1)
        processes.append(p)

    wait_period = n_nodes//5
    print(f"Waiting {wait_period}s for nodes to stabilize...")
    time.sleep(wait_period)

    print("Injecting Gossip message...")
    processes[n_nodes].stdin.write("Simulation Test Message\n")
    processes[n_nodes].stdin.flush()

    # Reverted to static wait period: N seconds for N nodes
    wait_period = n_nodes//2
    print(f"Waiting {wait_period}s for propagation...")
    time.sleep(wait_period)

    # Terminate all
    for p in processes:
        p.terminate()
        p.wait()

    # DRAW THE GRAPH OF THE NETWORK (NODES AND THEIR CONNECTIONS)
    draw_network_graph(n_nodes, seed_base)

    # Post-run Analysis
    c_time, overhead = parse_and_analyze(n_nodes)
    # didn't converge
    if c_time == 0.0:
        c_time = nan
        overhead = nan
    print(f"Results -> Convergence: {c_time:.2f} ms | Overhead: {overhead} messages")
    return c_time, overhead


def main():
    results_file = open(f'logs/{RATIO}/simulation_results.txt', 'w')
    results_file.write(f"Simulation started at {datetime.now()}\n\n")
    results_convergence = defaultdict(list)
    results_overhead = defaultdict(list)

    for n in NETWORK_SIZES:
        for run in range(RUNS_PER_SIZE):
            c_time, overhead = run_simulation(n, seed_base= 100 * n + run)
            results_convergence[n].append(c_time)
            results_overhead[n].append(overhead)
            results_file.write(f"N={n}, Run={run}, Convergence={c_time:.2f} ms, Overhead={overhead} messages\n")

    # Calculate Averages for plotting
    avg_convergence = []
    avg_overhead = []
    for n in NETWORK_SIZES:
        # Filter out zeros
        nonzero_convergence = [x for x in results_convergence[n] if not math.isnan(x)]
        nonzero_overhead = [x for x in results_overhead[n] if not math.isnan(x)]
        # Compute average only if there are non-zero entries, else nan
        avg_c = sum(nonzero_convergence) / len(nonzero_convergence) if nonzero_convergence else nan
        avg_o = sum(nonzero_overhead) / len(nonzero_overhead) if nonzero_overhead else nan
        avg_convergence.append(avg_c)
        avg_overhead.append(avg_o)

    results_file.write("\nAverage Results:\n")
    for i, n in enumerate(NETWORK_SIZES):
        results_file.write(
            f"N={n}: Avg Convergence={avg_convergence[i]:.2f} ms, Avg Overhead={avg_overhead[i]:.2f} messages\n")

    results_file.write("\nSimulation completed.\n")
    results_file.close()

    # Plotting
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    ax1.plot(NETWORK_SIZES, avg_convergence, marker='o', linestyle='-', color='royalblue', linewidth=2)
    ax1.set_title(f'{RATIO*100}% Convergence Time vs Network Size')
    ax1.set_xlabel('Number of Nodes (N)')
    ax1.set_ylabel('Time (ms)')
    ax1.grid(True, alpha=0.3)

    ax2.plot(NETWORK_SIZES, avg_overhead, marker='s', linestyle='-', color='crimson', linewidth=2)
    ax2.set_title('Message Overhead vs Network Size')
    ax2.set_xlabel('Number of Nodes (N)')
    ax2.set_ylabel('Total Messages Sent')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'logs/{RATIO}/phase3_results.png')
    print("\n[!] Simulation complete. Graph saved as 'phase3_results.png'.")
    plt.show()


if __name__ == "__main__":
    main()

# Distributed Key-Value Store (Java)

This is a highly-available, distributed key-value store built from scratch in Java. It features a custom **Write-Ahead Log (WAL)** for persistence and implements the **Raft Consensus Algorithm** for distributed master-slave replication via gRPC. It also exposes live cluster health metrics via **Prometheus**.

---

### Project Phases
1.  **Phase 1**: Core gRPC communication and RAM storage.
2.  **Phase 2**: Adding an append-only **Write-Ahead Log (WAL)** for persistence and crash recovery.
3.  **Phase 3**: Scaling to a distributed cluster using the **Raft Consensus Algorithm** (Leader Elections & Heartbeats).
4.  **Phase 4**: Hardening the system with **Leader Hinting** and **Smart Client** routing.
5.  **Phase 5**: Enabling cluster self-healing with the **Log Matching Property** (Fast Catch-up & `nextIndex` trackers).
6.  **Phase 6**: Production Hardening with **Pre-Vote Phase** and **Two-Phase Commits** (`commitIndex`).

### Deep Dives
For comprehensive explanations of the inner workings of this distributed system, check out the following documents:
- [ARCHITECTURE.md](ARCHITECTURE.md): A step-by-step breakdown of how the architecture evolved across the 6 phases.
- [raft node.md](raft%20node.md): A technical deep dive into `RaftNode.java`, covering the Pre-Vote phase, Two-Phase Commits, and Log Matching.

---

## Prerequisites
- **Java 17+**
- **Maven**
- **Docker** (Optional, for running the Prometheus Dashboard)

---

## 🚀 How to Start the Cluster

### Step 1: Compile the Project
Before running the servers, you must compile the gRPC protobuf files and Java source code. Run this command in the root project directory:
```bash
mvn clean compile
```

### Step 2: Start the 3-Node Raft Cluster
To simulate a distributed system, you will need to open **three separate terminal windows**. Run one command in each terminal. 

The servers will automatically discover each other, hold a Raft election, and elect a Leader.

**Terminal 1 (Node A):**
```bash
mvn exec:java -Dexec.mainClass=com.cloudwick.kvstore.ServerMain -Dexec.args="50051 50052,50053"
```

**Terminal 2 (Node B):**
```bash
mvn exec:java -Dexec.mainClass=com.cloudwick.kvstore.ServerMain -Dexec.args="50052 50051,50053"
```

**Terminal 3 (Node C):**
```bash
mvn exec:java -Dexec.mainClass=com.cloudwick.kvstore.ServerMain -Dexec.args="50053 50051,50052"
```

*Note: You will see the nodes outputting their Raft state (e.g., "starting election", "stepping down", "BECAME LEADER").*

---

## 📊 Viewing Live Metrics

Each node spins up a lightweight Prometheus metrics server on its port + 1000.
Open your web browser and navigate to any of these URLs to see the live data:
- [http://localhost:51051/metrics](http://localhost:51051/metrics)
- [http://localhost:51052/metrics](http://localhost:51052/metrics)
- [http://localhost:51053/metrics](http://localhost:51053/metrics)

You will see counters for `kvstore_put_requests_total`, `raft_current_term`, and `raft_node_role`.

---

## 📈 Dashboarding with Prometheus (Docker)
If you have Docker installed, you can automatically scrape all three nodes and graph their traffic in a beautiful dashboard. 

Run this command in a new terminal in the project root:
```bash
docker run -p 9090:9090 -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
```

Then, open your browser and navigate to: [http://localhost:9090](http://localhost:9090). 
Search for `raft_node_role` or `kvstore_put_requests_total` in the search bar and click the "Graph" tab to see real-time distributed cluster behavior!

---

## 🧠 Architecture Choices: Why Protobuf & gRPC?
At the core of this project is the `kvstore.proto` file. Instead of using standard REST APIs with JSON, we chose **gRPC** and **Protocol Buffers (Protobuf)** for our cluster communication. Here is why:

1. **Auto-Generated Networking:** Writing raw TCP socket code by hand is error-prone. By simply defining our service in `kvstore.proto`, the gRPC compiler automatically generates thousands of lines of bulletproof Java networking code for us. We just call `stub.appendEntries(request)` and gRPC handles the rest.
2. **Lightning Fast Binary:** Unlike JSON (which is heavy, plain-text data), Protobuf compresses our requests into tiny, raw binary (1s and 0s). This makes network communication infinitely faster and uses significantly less bandwidth.
3. **Strict API Contracts:** In a distributed system, a missing field can crash the whole cluster. The `.proto` file acts as a strict, strongly-typed legal contract. Every server on the network *must* follow the exact data schema defined in the file, preventing bugs before the code even compiles.
4. **Language Agnostic:** Because the `.proto` file is universally understood, a Python or Go developer could easily use this exact same file to write a Node in their preferred language, and it would seamlessly join our Java Raft cluster!

---

## 🛠️ Common Issues & Troubleshooting (Developer Log)

During the development and testing of this cluster, we encountered and fixed several complex distributed systems issues:

### 1. Maven Protoc Plugin Caching Bug
**Issue**: Running `mvn compile` multiple times quickly sometimes failed with `Unable to clean up temporary proto file directory`. 
**Fix**: This is a known caching bug with the `protobuf-maven-plugin`. We resolved it by forcefully clearing the target directory before compiling: `rm -rf target/ && mvn compile`.

### 2. Smart Routing Missing for GET Requests
**Issue**: The Interactive Client (`ClientMain`) handled `PUT` requests dynamically, failing over to other nodes if the Leader crashed. However, `GET` requests did not have a try-catch loop, so attempting to `GET` from a dead Leader threw a fatal `Connection refused` exception, crashing the client.
**Fix**: We wrapped the `GET` request in the exact same `try-catch` exponential fallback loop as the `PUT` request, allowing it to seamlessly hunt for surviving nodes without crashing.

### 3. Raft Election Storms (Section 6 Disruptions)
**Issue**: When a Follower node was forcefully killed and restarted, its `electionTimer` would expire before the Java gRPC server fully initialized. It would panic and send a `Pre-Vote` to the healthy Leader. Because the restarted node's log was perfectly up to date, the Leader would wrongly grant it the vote, causing the restarted node to successfully (and incorrectly) overthrow the healthy Leader!
**Fix**: We implemented the official **"Leader Lease"** fix from Section 6 of the Raft Thesis. Nodes now track `lastMessageFromLeaderTime`. If any node (including the Leader) receives a vote request but has heard from the healthy Leader in the last 150ms, it aggressively rejects the vote, completely preventing disruptive election storms.

### 4. Nodes Getting Stuck in Pre-Vote Spam
**Issue**: A restarted node would endlessly spam `Node 50051 starting PRE-VOTE! Timer expired.` in the terminal without ever running a real election or becoming a Follower.
**Fix**: This wasn't a bug, but a side effect of **gRPC's Exponential Reconnection Backoff**. When the Follower died, the Leader's gRPC channel went into a sleeping penalty state for up to 2 minutes. The Follower spammed safe `Pre-Votes` (rejected by the healthy cluster) until the Leader's network penalty expired and it finally sent a heartbeat, instantly healing the cluster.
# Distributed_Key_value_store

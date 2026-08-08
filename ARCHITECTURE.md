# How We Built a Distributed Database (Like a Mini-Redis)

Imagine you have a piece of paper where you write down important information for your friends. If you lose that piece of paper, all the information is gone! This project is about building a system that makes sure that "piece of paper" is never lost, even if your computer crashes or your house loses power.

We built this in **Phases**. Here is how it works, explained simply!

## Phase 1: The Memory Vault

In Phase 1, we built the core "brain" of the database. 

1. **The Hash Map**: In Java, we used a `ConcurrentHashMap`. Think of this as a super-fast dictionary that lives in your computer's RAM (Random Access Memory). If a user says "Save my username as Cloudwick", we put that in the dictionary. It is extremely fast to read and write to this dictionary.
2. **Talking over the Network (gRPC)**: We can't just have this dictionary sitting isolated; other computers need to talk to it! We used a technology called **gRPC** (created by Google). It acts as the telephone line. We defined exactly three rules for talking on this telephone:
   * `Put`: Put some data in the dictionary.
   * `Get`: Ask for data from the dictionary.
   * `Delete`: Remove data from the dictionary.

**The Problem with Phase 1:** RAM is volatile. If the Java program crashes, or you turn off the computer, the dictionary is wiped completely clean!

---

## Phase 2: The Indestructible Log (Persistence)

To fix the problem from Phase 1, we needed a way to survive crashes. 

We created the **Write-Ahead Log (WAL)**. 
Think of the WAL as an accountant's ledger book. Before the accountant (our server) is allowed to change the fast dictionary in memory, they *must* first write down what they are about to do in the ledger book, using a pen (saving it to the hard drive).

1. A user says "Save my username as Cloudwick".
2. The server opens a file on the hard drive called `kvstore.wal` and writes: `ACTION: PUT, KEY: username, VALUE: Cloudwick`.
3. Only *after* the hard drive confirms it is saved, the server updates the super-fast dictionary in RAM.

**How does this save us?**
If the server crashes, the RAM dictionary is destroyed. But the ledger book (`kvstore.wal`) is safe on the hard drive. When we turn the server back on, the very first thing it does is read the ledger book line by line, re-doing every single action. By the time it finishes reading the book, the dictionary in RAM is perfectly restored to exactly how it was right before the crash!

---

## Phase 3: The Clones (Distributed Raft Consensus)

What if the hard drive itself blows up? Or what if the server catches fire? The ledger book won't save us then!

To solve this, we don't just run one server; we run **three** servers (nodes). They all have their own dictionary and their own ledger book. But how do they agree on what data goes in? If one server is slow, how do we prevent data from getting mixed up?

We are using an algorithm called **Raft**. 
1. **The Election:** The three servers hold an election. One of them is elected the **Leader**. The other two become **Followers**.
2. **The Boss:** If a user wants to save data, they *must* tell the Leader. The Followers are not allowed to accept data directly.
3. **The Replication:** The Leader writes the data to its own ledger book, and then shouts to the Followers: "Hey! Write this down!". The Leader waits until at least one Follower says "Got it!" before telling the user the data is saved.
4. **Crash Recovery:** If the Leader catches fire, the two remaining Followers notice the Leader is gone, hold a new election, and pick a new Leader within milliseconds. The database never goes down!

Because followers have no idea who won the election (they just know someone else won), they will return an error to the client saying `"I am not the leader!"`. But they are smart: because they receive heartbeats every 50ms, they know exactly who the Leader is. They return a **Leader Hint** in their error message, so the client can instantly try again on the correct port.

---

## Phase 4: Seeing into the Matrix (Observability)

When you run three servers on a network, it's very hard to know what they are actually doing. Are they crashing? Are they holding elections every two seconds? How much traffic are they handling?

To solve this, we added **Observability** using **Prometheus**.

1. **The Counters**: We added tiny counters inside the Java code that tick upwards every time an event happens. We track:
   * Total `PUT` requests.
   * Total `GET` requests.
   * The current `Term` number (so we know if an election happened).
   * The current `Role` (to easily see who the Leader is).
2. **The Metrics Server**: Every time we start a database node, it also spins up a tiny, invisible web server. If you go to that web server in your browser, it spits out all of those counters in a plain-text format.
3. **The Scraper**: We use a tool called Prometheus that automatically visits those web servers every 5 seconds, scrapes the numbers, and allows us to draw beautiful graphs of our database's health in real-time!

---

## Phase 5: Log Matching & Fast Catch-Up (Self-Healing)

What happens if Node B's power cord gets kicked out, and it stays offline for 20 minutes? In those 20 minutes, Node A (Leader) and Node C (Follower) will continue to accept thousands of `PUT` requests. Node B has completely missed them!

To solve this, we implemented the strict **Log Matching Property**:

1. **The Teacher's Checkbook (`nextIndex`)**: The Leader keeps a tracker in RAM of exactly which log index each follower is on. 
2. **The Test**: When the Leader sends new data, it also sends the `prevLogIndex` (e.g. "I am giving you line 51, the previous line was 50").
3. **The Rejection**: When Node B boots back up, it receives this message. But Node B's hard drive only goes up to line 10! It rejects the message, and sends back a hint: `"False! My last log is actually line 10."`
4. **Fast Catch-Up**: The Leader instantly realizes Node B is massively behind. It drops its `nextIndex` for Node B straight down to 11, bundles up lines 11 through 51 into one massive payload, and sends it all at once to perfectly heal Node B's hard drive.

This guarantees that every hard drive in the cluster is identical, no matter how many times the servers crash and reboot.

---

## Phase 6: Production Hardening (Pre-Vote & Two-Phase Commits)

To elevate this educational implementation to a true production-grade standard (like Etcd or CockroachDB), two final features were added:

1. **Pre-Vote Phase (Election Storm Prevention)**: If a node is disconnected from the network, its election timer will constantly expire, causing it to inflate its `term` into the thousands. When it reconnects, its artificially high term would depose the legitimate leader and cause chaos. To prevent this, nodes now enter a `PRE_CANDIDATE` role when they timeout. They ask the cluster *"Would you vote for me?"* **without** incrementing their term. If they don't get a majority of YES votes, they stay silent.

2. **Two-Phase Commits (`commitIndex`)**: Followers no longer apply data to their RAM HashMaps the moment they receive it. Instead, they write it to the WAL and wait. The Leader tracks a `commitIndex` (the highest log index safely replicated to a majority of servers). In the next heartbeat, the Leader broadcasts this `commitIndex` to the Followers. Only then do the Followers officially apply the data to their RAM. This guarantees a follower will never serve uncommitted "dirty" reads to a client.

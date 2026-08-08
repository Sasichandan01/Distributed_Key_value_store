# Deep Dive: The Raft Node (`RaftNode.java`)

This document is a deep dive into the inner workings of our `RaftNode.java` implementation. It explains the mechanics of how our nodes achieve distributed consensus, handle crashes, and ensure strict data consistency matching production systems like Etcd.

## 1. Core State Tracking
Every Raft node, regardless of its role, maintains these fundamental variables:
- `currentTerm`: A logical clock that increments on every election. If a node ever sees a message with a higher term, it instantly knows it is outdated and steps down.
- `votedFor`: Tracks who this node voted for in the current term to prevent voting twice.
- `commitIndex`: The "Safe High-Water Mark". The highest log index that is guaranteed to have been saved to the hard drives of a **majority** of the cluster.
- `lastApplied`: The pointer for our RAM HashMap. A background loop constantly reads the WAL and applies logs to the HashMap, but is strictly forbidden from ever surpassing the `commitIndex`.

## 2. Leader Specific Tracking
When a node wins an election, it initializes two tracking maps for every other node in the cluster:
- `nextIndex`: Optimistic. The index of the next log entry the Leader intends to send to a follower. Initialized to `logList.size()`.
- `matchIndex`: Pessimistic. The highest log index the Leader is *certain* the follower has successfully replicated. Initialized to `-1`.

## 3. The Log Matching Property & Fast Catch-up (Phase 5)
When a Follower's power cord is pulled, it misses data. When it reconnects, it must be healed. 

**The Check:**
When the Leader sends `AppendEntries`, it includes `prevLogIndex` and `prevLogTerm`. The Follower checks its own WAL. If its log doesn't go up to `prevLogIndex`, or if the term at that index mismatches, it rejects the payload.

**Fast Catch-Up:**
Instead of the Leader decrementing `nextIndex` by 1 and retrying (which is slow), the Follower provides a hint: `lastLogIndex`. The Leader immediately drops its `nextIndex` for that follower to `hint + 1`, bundles all missing logs into a single array, and transmits them to instantly synchronize the Follower's hard drive.

## 4. Two-Phase Commits (Phase 6)
To prevent Followers from serving dirty, uncommitted reads to clients, we implement a two-phase commit:
1. The Leader broadcasts a new log entry.
2. Followers receive it, write it to their `kvstore.wal` (Hard Drive), but do **not** apply it to their RAM HashMap.
3. Followers reply "Success" to the Leader.
4. Once the Leader receives a majority of "Success" replies, it updates its own `commitIndex` and applies it to its own RAM.
5. In the next 50ms heartbeat, the Leader includes `leaderCommit = commitIndex`.
6. The Followers receive the heartbeat, update their local `commitIndex`, and finally copy the data from the WAL into their RAM.

## 5. Pre-Vote Phase & Election Storm Prevention (Phase 6)
If a Follower gets disconnected from the network, its election timer will constantly expire. In standard Raft, it would increment its `term`, transition to `CANDIDATE`, and request votes. Over an hour, its `term` could reach 10,000. When the network heals, it broadcasts `term=10000` to the cluster, causing the legitimate Leader (who might only be on `term=5`) to immediately step down, causing unnecessary cluster outages.

**The Solution:**
When a node's timer expires, it transitions to `PRE_CANDIDATE` and sends an `isPreVote = true` request. **It does not increment its term.**
If the rest of the cluster is happily talking to a legitimate Leader, they will reject the Pre-Vote. The disconnected node realizes it has no chance of winning, stays quiet, and waits for the network to heal, preserving the stability of the cluster!

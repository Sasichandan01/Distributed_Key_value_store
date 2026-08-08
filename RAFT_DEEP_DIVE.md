# Raft Consensus: The Micro-Level Deep Dive

This document explains exactly what happens inside a Raft cluster, leaving no "magic" unexplained. This is the level of detail you will need for a senior systems design interview.

## 1. The Core Variables
Every single server (node) in the cluster maintains these variables in memory (and some on disk):

*   `currentTerm`: An integer (e.g., `42`) representing the current election term. (Saved to disk).
*   `votedFor`: Which node it voted for in the current term. (Saved to disk).
*   `log[]`: The actual array of commands (e.g., `PUT x=1`, `PUT y=2`). (Saved to disk via WAL).
*   `commitIndex`: The highest log index that is guaranteed to be saved on a *majority* of servers. Once a log is "committed", it is safe to apply it to the database (Redis).

## 2. The Normal Flow (Client saves data)
Assume **Node A** is the Leader. **Node B** and **Node C** are Followers.

1.  **Client Request:** A user sends `PUT name=Cloudwick` to Node A.
2.  **Leader Local Write:** Node A does *not* immediately reply to the client! First, Node A appends this command to its own `log[]` at the next available index (let's say Index 10).
3.  **The Broadcast (AppendEntries):** Node A sends an `AppendEntries` gRPC message to Node B and Node C. This message contains:
    *   `entries`: `[PUT name=Cloudwick]`
    *   `prevLogIndex`: `9` (The index right before this new one)
    *   `prevLogTerm`: `42` (The term of the log at index 9)
4.  **Follower Validation:** Node B receives the message. Before accepting the new data, Node B checks its own `log[]`. It looks at Index `9`. Does the term at Index `9` match the `prevLogTerm` (42) the Leader sent? 
    *   *If YES:* Node B knows it is perfectly in sync. It appends Index 10 to its log and replies `success = true`.
5.  **The Commit:** Node A waits until it gets `success = true` from a *majority* of the cluster (in this case, just 1 follower is enough, making 2/3 total). 
6.  **Reply to Client:** Because a majority has the data, Node A updates its `commitIndex` to `10` and finally tells the user: "Success!"
7.  **Follower Commit:** On the very next heartbeat, Node A tells B and C: "Hey, my `commitIndex` is now 10". Node B and C then update their own `commitIndex` to 10, and actually apply the data to their in-memory HashMap (Redis).

---

## 3. The Edge Case: Nodes Out of Sync
What if Node B crashes before Step 3, and wakes up an hour later? 

Node A has kept serving requests and is now on **Index 1000**.
Node B wakes up, but its log is stuck at **Index 9**.

1.  **The Mismatch:** Node A sends a heartbeat to Node B. The heartbeat says: `"Hey, my prevLogIndex is 1000"`.
2.  **The Rejection:** Node B looks at its log. It doesn't even *have* an Index 1000. So Node B replies to the Leader: `success = false`.
3.  **The Backtracking:** Node A (The Leader) keeps track of a variable called `nextIndex[]` for every follower. It realizes Node B is out of sync. So Node A decrements the index and tries again.
    *   Node A: "How about Index 999?" -> Node B: `false`
    *   Node A: "How about Index 998?" -> Node B: `false`
4.  **The Match:** Eventually, Node A asks: *"How about Index 9?"* Node B checks its log and says: *"Yes! I have Index 9!"*
5.  **The Overwrite:** Now that the Leader found the exact spot where they diverged, Node A sends all the logs from Index 10 to 1000 in one massive `AppendEntries` message. Node B forcefully overwrites any garbage data it had and perfectly mirrors the Leader.

**Optimization (Fast Backtracking):** In real systems like etcd, the Leader doesn't actually check one-by-one! When Node B rejects the heartbeat, it replies: *"False. But the highest index I have is 9"*. The Leader uses this metadata to instantly jump `nextIndex` down to 9, skipping thousands of useless network requests!

---

## 4. The Edge Case: Election with Stale Data
What if Node B crashes, misses 100 `PUT` requests, wakes up, and its election timer expires before the Leader can fix it? Node B will try to become the Leader!

**This is solved by the "Up-To-Date" Rule:**
1.  Node B changes to Candidate, increments its term to `43`, and sends a `RequestVote` message to Node A and Node C.
2.  Inside the `RequestVote` message, Node B **must** include its `lastLogIndex` (which is 9).
3.  Node A receives the vote request. It sees Term 43, which is higher than its own term (42). Normally, it would vote for Node B.
4.  **BUT**, Node A checks Node B's `lastLogIndex`. Node A's own log is at Index `1000`. Node B's is at `9`. 
5.  Because `1000 > 9`, Node A knows Node B is missing data. Node A **rejects the vote**.
6.  Node B fails to get a majority, fails to become Leader, and is eventually fixed by the real Leader (Node A).

---

## 5. Preventing Election Storms
What prevents nodes from constantly timing out and throwing a "coup" while the Leader is working perfectly fine? 

**The Solution:**
1. **Randomized Timers:** Followers have a randomized election timer between 150ms and 300ms.
2. **Fast Heartbeats:** The Leader sends empty `AppendEntries` heartbeats every 50ms.
3. **Suppression:** Because 50ms is much faster than 150ms, a Follower's timer never reaches zero. Every time a heartbeat arrives, the Follower forces its timer back to 150ms. An election ONLY happens if the Leader physically dies and stops sending heartbeats.

---

## 6. The Exploding Hard Drive (Data Loss & Fsync)
When Java writes data to a file, the Operating System often holds it in a temporary RAM buffer (Page Cache) before flushing it to the physical disk (fsync). If power is lost during this window, the data is destroyed.

**Why doesn't this corrupt Raft?**
Because of the **Wait for Majority** rule. 
Even if the Leader's OS hasn't flushed the data to the physical disk, the Leader is strictly forbidden from telling the Client "Success" until a *majority* of the Followers have also received the data and written it. 

If the Leader's hard drive melts, or it loses power before the fsync completes, the Client has *not* been told the write was successful yet. The Leader dies, and a Follower (who successfully saved the data) gets elected as the new Leader. Data is only ever lost if a *majority* of the cluster's physical hard drives explode at the exact same millisecond!

---

## 7. The Split Vote (Ties)
Even with randomized timers (150-300ms), there is a tiny mathematical chance that Node A and Node B both time out at the exact same millisecond. 

What happens?
1. Both nodes transition to Candidates.
2. Both nodes increment to the new term (e.g., Term 43).
3. Both nodes vote for *themselves*. 
4. Node A asks Node C for a vote. Node B asks Node C for a vote. 

**Scenario A (One wins quickly):**
If Node C receives Node A's request first, it grants the vote to Node A. When Node B's request arrives a millisecond later, Node C rejects it because the strict rule is: *"You can only vote once per Term"*. Node A gets 2 votes, wins, and becomes Leader.

**Scenario B (A True Tie):**
What if it's a 4-node cluster (or larger) and Node A gets 2 votes and Node B gets 2 votes? 
**Nobody gets a majority.** The election is a failure. 
1. Because nobody became Leader, nobody sends heartbeats.
2. Because there are no heartbeats, the Candidates' timers keep running.
3. The timers expire *again*, triggering a brand new election for Term 44.
4. When they reset their timers, they roll the dice again. This time, Node A rolls 160ms and Node B rolls 290ms. Node A times out first, easily wins Term 44, and the tie is broken!

---

## 8. The Purpose of Empty Heartbeats
You might wonder: *"Why does the Leader send a heartbeat every 50ms even when no users are sending PUT requests? Why waste the bandwidth?"*

The empty heartbeats act as a constant **"I am still alive, do not panic"** signal. 

Remember that every Follower's 150-300ms randomized timer is *always* counting down to zero, 24/7. If a Follower's timer hits zero, it assumes the Leader has died and it forcefully starts an election. 

If the database is idle (no users saving data), and the Leader stops sending network packets:
1. Node A (Leader) sits quietly.
2. 150 milliseconds later, Node B's timer hits zero. 
3. Node B falsely assumes Node A is dead, triggers an election, and steals the leadership!
4. 200 milliseconds later, Node C's timer hits zero. It steals the leadership!

This would cause an **Election Storm**. The cluster would spend 100% of its CPU power fighting over who is the leader, thousands of times an hour, even when no users are using the database.

By sending an empty `AppendEntries` message every 50ms, the Leader forces the Followers to reset their timers back to 150ms. The Followers get trapped in an infinite loop of resetting their timers, keeping the cluster completely peaceful and stable when no data is flowing!

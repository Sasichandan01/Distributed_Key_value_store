package com.cloudwick.kvstore;

import com.cloudwick.kvstore.grpc.AppendRequest;
import com.cloudwick.kvstore.grpc.AppendResponse;
import com.cloudwick.kvstore.grpc.KVStoreServiceGrpc;
import com.cloudwick.kvstore.grpc.VoteRequest;
import com.cloudwick.kvstore.grpc.VoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RaftNode represents a single server in our distributed database cluster.
 * It is responsible for handling all the complex Raft Consensus logic (elections, heartbeats, replication).
 * By keeping this separate from the storage logic, the code is much cleaner.
 */
public class RaftNode {
    
    // In Raft, a node can only be in one of three states at any given time.
    public enum Role {
        FOLLOWER,       // The default state. Just listens to the Leader.
        PRE_CANDIDATE,  // [NEW] Checks if it can win before incrementing term to prevent election storms.
        CANDIDATE,      // The state when the election timer expires and it wants to become Leader.
        LEADER          // The boss. Accepts client requests and replicates them to Followers.
    }

    // --- Core Raft State Variables ---
    
    // Every node starts as a FOLLOWER.
    private Role role = Role.FOLLOWER;
    
    // The "Term" acts as a logical clock in Raft. Every time there is a new election, this increments.
    // If a node sees a message with a higher Term, it immediately knows its own data is stale.
    private int currentTerm = 0;
    
    // Keeps track of who this node voted for in the currentTerm. 
    // Null means it hasn't voted yet. A node can only vote ONCE per term.
    private Integer votedFor = null;
    
    // Tracks the current known leader of the cluster (used for Leader Hinting)
    private Integer currentLeaderId = null;

    // --- Networking Variables ---
    
    // The port number (used as the unique ID for this server, e.g., 50051)
    private final int nodeId; 
    
    // The port numbers of all the OTHER servers in the cluster (e.g., [50052, 50053])
    private final List<Integer> peerIds;
    
    // The shared in-memory log list, passed from ServerMain
    private final List<com.cloudwick.kvstore.grpc.LogEntry> logList;
    
    // --- Leader State (Reinitialized after election) ---
    // Tracks the next log index to send to that follower
    private final Map<Integer, Integer> nextIndex = new java.util.concurrent.ConcurrentHashMap<>();
    // Tracks the highest log index known to be replicated on server
    private final Map<Integer, Integer> matchIndex = new java.util.concurrent.ConcurrentHashMap<>();
    
    // --- Two-Phase Commit State ---
    private volatile int commitIndex = -1;
    private volatile int lastApplied = -1;
    private java.util.function.Consumer<Integer> applyCallback = null;
    
    // The auto-generated gRPC network clients. We use these "stubs" to physically send 
    // messages (like AppendEntries or RequestVote) to the other servers over the network.
    private final Map<Integer, KVStoreServiceGrpc.KVStoreServiceBlockingStub> peerStubs = new HashMap<>();

    // --- Timers (The heartbeat of Raft) ---
    
    // A thread pool that allows us to run background timers.
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    // The timer that counts down until this node should start an election.
    private ScheduledFuture<?> electionTimer;
    
    // The timer (only used by the Leader) to send heartbeats every 50ms.
    private ScheduledFuture<?> heartbeatTimer;
    
    // Used to generate the randomized election timeout (150ms - 300ms) to prevent split votes.
    private final Random random = new Random();
    
    // Tracks the last time we heard from a healthy Leader (used to ignore disruptive elections)
    private volatile long lastMessageFromLeaderTime = 0;

    /**
     * Constructor: Initializes the node and connects it to its peers.
     */
    public RaftNode(int nodeId, List<Integer> peerIds, List<com.cloudwick.kvstore.grpc.LogEntry> logList) {
        this.nodeId = nodeId;
        this.peerIds = peerIds;
        this.logList = logList;

        // Setup gRPC stubs for peers. This creates the actual network connections to the other nodes.
        for (int peerId : peerIds) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", peerId)
                    .usePlaintext() // No SSL/TLS encryption for this basic project
                    .build();
            peerStubs.put(peerId, KVStoreServiceGrpc.newBlockingStub(channel));
        }

        // As soon as the server boots up, start the election countdown!
        resetElectionTimer();
    }
    
    public void setApplyCallback(java.util.function.Consumer<Integer> applyCallback) {
        this.applyCallback = applyCallback;
    }
    
    private void applyStateMachine() {
        if (commitIndex > lastApplied) {
            if (applyCallback != null) {
                applyCallback.accept(commitIndex);
            }
            lastApplied = commitIndex;
        }
    }

    /**
     * Resets the election timer to a new random value between 150ms and 300ms.
     * WHY RANDOM? If two nodes timeout at the exact same millisecond, they will split the votes.
     * By randomizing it, one node will almost always timeout slightly faster and win.
     */
    public synchronized void resetElectionTimer() {
        // Cancel the old timer if it exists
        if (electionTimer != null && !electionTimer.isDone()) {
            electionTimer.cancel(false);
        }
        
        // Pick a random number between 150 and 300
        int timeout = 150 + random.nextInt(150);
        
        // Schedule the startElection() method to run after the timeout expires
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Called automatically when the electionTimer hits zero.
     * This means the node hasn't heard from a Leader in a while, so it assumes the Leader is dead.
     */
    private synchronized void startElection() {
        // If we are already the Leader, we shouldn't be holding elections!
        if (role == Role.LEADER) return;

        System.out.println("Node " + nodeId + " starting PRE-VOTE! Timer expired.");
        
        // 1. Transition to PRE_CANDIDATE state
        setRole(Role.PRE_CANDIDATE);
        
        // 2. We do NOT increment the term yet! We just test the waters for currentTerm + 1.
        int nextTerm = currentTerm + 1;
        
        // 3. Reset our own election timer in case this pre-election fails
        resetElectionTimer();

        // Keep track of how many pre-votes we get. We start with 1 (our own vote).
        AtomicInteger votesReceived = new AtomicInteger(1); 

        // Create the pre-voting ballot
        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(nextTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(logList.size() - 1)
                .setLastLogTerm(logList.isEmpty() ? 0 : logList.get(logList.size() - 1).getTerm())
                .setIsPreVote(true) // [NEW] Flag it as a Pre-Vote!
                .build();

        // Ask every other node in the cluster for a pre-vote
        for (Map.Entry<Integer, KVStoreServiceGrpc.KVStoreServiceBlockingStub> entry : peerStubs.entrySet()) {
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = entry.getValue();

            new Thread(() -> {
                try {
                    VoteResponse response = stub.requestVote(request);
                    
                    synchronized (RaftNode.this) {
                        if (role == Role.PRE_CANDIDATE && nextTerm == request.getTerm() && response.getVoteGranted()) {
                            votesReceived.incrementAndGet();
                            
                            // If we get a majority of pre-votes, we run a REAL election!
                            if (votesReceived.get() > (peerIds.size() + 1) / 2) {
                                startRealElection();
                            }
                        } else if (response.getTerm() > currentTerm) {
                            stepDown(response.getTerm()); 
                        }
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }).start();
        }
    }
    
    private synchronized void startRealElection() {
        if (role != Role.PRE_CANDIDATE) return;
        System.out.println("Node " + nodeId + " passed PRE-VOTE! Starting REAL election.");
        
        setRole(Role.CANDIDATE);
        setCurrentTerm(currentTerm + 1);
        votedFor = nodeId; 
        resetElectionTimer();

        AtomicInteger votesReceived = new AtomicInteger(1); 

        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(currentTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(logList.size() - 1)
                .setLastLogTerm(logList.isEmpty() ? 0 : logList.get(logList.size() - 1).getTerm())
                .setIsPreVote(false)
                .build();

        for (Map.Entry<Integer, KVStoreServiceGrpc.KVStoreServiceBlockingStub> entry : peerStubs.entrySet()) {
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = entry.getValue();
            new Thread(() -> {
                try {
                    VoteResponse response = stub.requestVote(request);
                    synchronized (RaftNode.this) {
                        if (role == Role.CANDIDATE && currentTerm == request.getTerm() && response.getVoteGranted()) {
                            votesReceived.incrementAndGet();
                            if (votesReceived.get() > (peerIds.size() + 1) / 2) {
                                becomeLeader();
                            }
                        } else if (response.getTerm() > currentTerm) {
                            stepDown(response.getTerm()); 
                        }
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }).start();
        }
    }

    /**
     * Called when this node successfully gets a majority of votes.
     */
    private void becomeLeader() {
        System.out.println("Node " + nodeId + " BECAME LEADER for term " + currentTerm + "!");
        setRole(Role.LEADER);
        currentLeaderId = nodeId;
        
        // Initialize Leader State Trackers
        for (int peerId : peerIds) {
            nextIndex.put(peerId, logList.size()); // We assume they are completely caught up at first
            matchIndex.put(peerId, -1);            // We know they have nothing confirmed yet
        }
        
        // Update Prometheus metrics for observability
        MetricsExporter.LEADER_ELECTIONS.labels(String.valueOf(nodeId)).inc();
        
        // Leaders don't run elections, so cancel the timer
        if (electionTimer != null) electionTimer.cancel(false);
        
        // Immediately start sending empty heartbeats to suppress the Followers' timers!
        startHeartbeats();
    }

    /**
     * Starts a timer that triggers every 50ms to send an empty AppendEntries message to all peers.
     */
    private void startHeartbeats() {
        if (heartbeatTimer != null && !heartbeatTimer.isDone()) {
            heartbeatTimer.cancel(false);
        }
        // Immediately record that we are the active leader
        lastMessageFromLeaderTime = System.currentTimeMillis();
        heartbeatTimer = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);
    }

    /**
     * Sends the actual heartbeat message to all followers.
     */
    private synchronized void sendHeartbeats() {
        if (role != Role.LEADER) return;
        
        // We are actively sending heartbeats, so we consider the leader (ourselves) to be alive
        lastMessageFromLeaderTime = System.currentTimeMillis();

        for (int peerId : peerIds) {
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = peerStubs.get(peerId);
            
            // Calculate what logs this specific follower is missing
            int nxt = nextIndex.getOrDefault(peerId, logList.size());
            final int prevIdx = nxt - 1;
            int prevTerm = (prevIdx >= 0 && prevIdx < logList.size()) ? logList.get(prevIdx).getTerm() : 0;
            
            final List<com.cloudwick.kvstore.grpc.LogEntry> entriesToSend;
            if (nxt < logList.size()) {
                entriesToSend = new java.util.ArrayList<>(logList.subList(nxt, logList.size()));
            } else {
                entriesToSend = new java.util.ArrayList<>();
            }

            AppendRequest request = AppendRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevIdx)
                    .setPrevLogTerm(prevTerm)
                    .addAllEntries(entriesToSend)
                    .setLeaderCommit(commitIndex)
                    .build();

            new Thread(() -> {
                try {
                    AppendResponse response = stub.appendEntries(request);
                    synchronized (RaftNode.this) {
                        if (response.getSuccess()) {
                            if (!entriesToSend.isEmpty()) {
                                matchIndex.put(peerId, prevIdx + entriesToSend.size());
                                nextIndex.put(peerId, matchIndex.get(peerId) + 1);
                            }
                        } else if (response.getTerm() > currentTerm) {
                            stepDown(response.getTerm()); 
                        } else {
                            // FAST CATCH-UP LOGIC!
                            // Follower rejected due to log mismatch. They gave us a hint.
                            int hint = response.getLastLogIndex();
                            nextIndex.put(peerId, hint + 1);
                            // It will try to send the missing logs on the next heartbeat tick!
                        }
                    }
                } catch (Exception e) {
                    // Ignore unreachable peers
                }
            }).start();
        }
    }

    /**
     * Surrender authority and return to FOLLOWER state. 
     * This happens when we discover a Leader with a higher term.
     */
    private synchronized void stepDown(int newTerm) {
        System.out.println("Node " + nodeId + " stepping down to FOLLOWER. New term: " + newTerm);
        setCurrentTerm(newTerm);
        setRole(Role.FOLLOWER);
        votedFor = null; // Clear our vote so we can vote in this new term
        currentLeaderId = null; // We don't know who the new leader is until they send a heartbeat
        
        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        resetElectionTimer(); // Start counting down for elections again
    }

    // ========================================================================
    // --- Handlers for incoming RPCs (Called by KVStoreServiceImpl) ---
    // ========================================================================

    /**
     * Called when ANOTHER node asks US for a vote.
     */
    public synchronized VoteResponse handleRequestVote(VoteRequest request) {
        // --- PREVENT DISRUPTIONS (Raft Section 6) ---
        // If we heard from a Leader within the minimum election timeout (150ms), 
        // we completely ignore all elections. The current Leader is healthy!
        if (System.currentTimeMillis() - lastMessageFromLeaderTime < 150) {
            return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
        }

        // If it's a Pre-Vote, we don't step down yet. We just check if they are eligible.
        if (request.getTerm() > currentTerm && !request.getIsPreVote()) {
            stepDown(request.getTerm());
        }

        boolean voteGranted = false;
        
        // Log consistency check (Raft Election Restriction)
        int myLastLogIndex = logList.size() - 1;
        int myLastLogTerm = myLastLogIndex >= 0 ? logList.get(myLastLogIndex).getTerm() : 0;
        
        boolean logIsUpToDate = (request.getLastLogTerm() > myLastLogTerm) || 
                                (request.getLastLogTerm() == myLastLogTerm && request.getLastLogIndex() >= myLastLogIndex);
        
        if (request.getIsPreVote()) {
            // For Pre-Vote, we just check if their next term is higher and their log is up to date
            if (request.getTerm() > currentTerm && logIsUpToDate) {
                voteGranted = true;
            }
        } else {
            // Real Vote
            if (request.getTerm() == currentTerm && (votedFor == null || votedFor == request.getCandidateId()) && logIsUpToDate) {
                voteGranted = true;
                votedFor = request.getCandidateId(); // Lock in our vote
                resetElectionTimer(); // Reset timer so we don't start our own election
            }
        }

        return VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(voteGranted)
                .build();
    }

    /**
     * Called when the LEADER sends us a heartbeat or new data to replicate.
     */
    public synchronized AppendResponse handleAppendEntries(AppendRequest request) {
        // If the leader's term is higher, accept it.
        if (request.getTerm() > currentTerm) {
            stepDown(request.getTerm());
        }

        boolean success = false;
        
        // Reject the payload if the "Leader" has a lower term than us (they are an old, deposed leader)
        if (request.getTerm() >= currentTerm) {
            setRole(Role.FOLLOWER); 
            resetElectionTimer();
            currentLeaderId = request.getLeaderId(); // Remember who the leader is!
            lastMessageFromLeaderTime = System.currentTimeMillis(); // We just heard from the healthy leader!
            
            // --- LOG MATCHING CHECK ---
            // 1. Does our local log even go up to the prevLogIndex?
            // 2. If it does, does the term at prevLogIndex match the leader's prevLogTerm?
            int prevLogIndex = request.getPrevLogIndex();
            int prevLogTerm = request.getPrevLogTerm();
            
            boolean logOk = true;
            if (prevLogIndex >= 0) {
                if (logList.size() <= prevLogIndex) {
                    logOk = false; // We are missing logs entirely!
                } else if (logList.get(prevLogIndex).getTerm() != prevLogTerm) {
                    logOk = false; // We have a conflicting log from an old overthrown leader!
                }
            }
            
            if (logOk) {
                success = true;
                
                // --- TWO-PHASE COMMIT ---
                if (request.getLeaderCommit() > commitIndex) {
                    commitIndex = Math.min(request.getLeaderCommit(), logList.size() - 1);
                    applyStateMachine();
                }
            } else {
                success = false;
                System.out.println("Node " + nodeId + " rejected AppendEntries. Missing/Conflicting logs. Requesting catch-up!");
            }
        }

        return AppendResponse.newBuilder()
                .setTerm(currentTerm)
                .setSuccess(success)
                .setLastLogIndex(logList.size() - 1) // Provide the Fast Catch-Up hint to the leader!
                .build();
    }

    /**
     * Called by the Leader to broadcast a new PUT/DELETE operation to all Followers.
     * It waits until a MAJORITY of followers successfully save it before returning true.
     */
    public boolean replicateLog() {
        if (role != Role.LEADER) return false;
        
        int targetIndex = logList.size() - 1; // The index of the entry we just appended
        AtomicInteger successCount = new AtomicInteger(1); // We (the Leader) already saved it
        
        CountDownLatch latch = new CountDownLatch(peerStubs.size());

        for (int peerId : peerIds) {
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = peerStubs.get(peerId);
            new Thread(() -> {
                try {
                    // Loop continuously until this follower has successfully replicated up to targetIndex,
                    // or until we lose our Leader status!
                    while (role == Role.LEADER && matchIndex.getOrDefault(peerId, -1) < targetIndex) {
                        
                        int nxt = nextIndex.getOrDefault(peerId, logList.size());
                        int prevIdx = nxt - 1;
                        int prevTerm = (prevIdx >= 0 && prevIdx < logList.size()) ? logList.get(prevIdx).getTerm() : 0;
                        
                        List<com.cloudwick.kvstore.grpc.LogEntry> entriesToSend = new java.util.ArrayList<>();
                        if (nxt < logList.size()) {
                            entriesToSend = new java.util.ArrayList<>(logList.subList(nxt, logList.size()));
                        }
                        
                        AppendRequest request = AppendRequest.newBuilder()
                                .setTerm(currentTerm)
                                .setLeaderId(nodeId)
                                .setPrevLogIndex(prevIdx)
                                .setPrevLogTerm(prevTerm)
                                .addAllEntries(entriesToSend)
                                .setLeaderCommit(commitIndex)
                                .build();
                                
                        AppendResponse response = stub.appendEntries(request);
                        
                        synchronized (RaftNode.this) {
                            if (response.getSuccess()) {
                                // They saved the batch!
                                matchIndex.put(peerId, prevIdx + entriesToSend.size());
                                nextIndex.put(peerId, matchIndex.get(peerId) + 1);
                                successCount.incrementAndGet();
                                
                                // Advance commitIndex if majority has replicated it
                                int newCommitIndex = matchIndex.get(peerId);
                                if (newCommitIndex > commitIndex) {
                                    int count = 1; // us
                                    for (int pId : peerIds) {
                                        if (matchIndex.getOrDefault(pId, -1) >= newCommitIndex) count++;
                                    }
                                    if (count > peerIds.size() / 2 && logList.get(newCommitIndex).getTerm() == currentTerm) {
                                        commitIndex = newCommitIndex;
                                        applyStateMachine();
                                    }
                                }
                                
                                break; // Exit the loop, they are caught up!
                            } else {
                                if (response.getTerm() > currentTerm) {
                                    stepDown(response.getTerm());
                                    break;
                                }
                                
                                // FAST CATCH-UP LOGIC
                                // The Follower rejected because their logs didn't match ours.
                                // We read the hint they gave us, and immediately retry the while-loop!
                                int hint = response.getLastLogIndex();
                                System.out.println("Node " + nodeId + " received hint " + hint + " from Follower " + peerId);
                                nextIndex.put(peerId, hint + 1);
                            }
                        }
                    }
                } catch (Exception e) {
                    // Follower is crashed. Ignore.
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return successCount.get() > peerIds.size() / 2; 
    }

    // ========================================================================
    // --- Getters & Setters (With Prometheus Instrumentation) ---
    // ========================================================================

    public synchronized Role getRole() { return role; }
    
    public synchronized void setRole(Role role) { 
        this.role = role;
        // Update Prometheus metric (0=Follower, 1=Candidate, 2=Leader)
        int roleVal = role == Role.FOLLOWER ? 0 : (role == Role.CANDIDATE ? 1 : 2);
        MetricsExporter.NODE_ROLE.labels(String.valueOf(nodeId)).set(roleVal);
    }
    
    public synchronized int getCurrentTerm() { return currentTerm; }
    
    public synchronized void setCurrentTerm(int term) { 
        this.currentTerm = term; 
        // Update Prometheus metric
        MetricsExporter.CURRENT_TERM.labels(String.valueOf(nodeId)).set(term);
    }
    
    public synchronized Integer getVotedFor() { return votedFor; }
    public synchronized void setVotedFor(Integer votedFor) { this.votedFor = votedFor; }
    
    public synchronized Integer getCurrentLeaderId() { return currentLeaderId; }
    
    public int getNodeId() { return nodeId; }
    public List<Integer> getPeerIds() { return peerIds; }
}

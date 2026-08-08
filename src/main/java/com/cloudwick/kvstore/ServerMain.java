package com.cloudwick.kvstore;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ServerMain is the Entry Point of the application.
 * When you type `java ServerMain` in the terminal, this is where the code starts executing.
 * 
 * Its only job is to wire all the components (Hard Drive, Raft, Metrics, Network) together 
 * and turn on the server.
 */
public class ServerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Run with: java ServerMain <port> <peer1,peer2>
        // Example: java ServerMain 50051 50052,50053
        
        // 1. Get our own Port Number from the terminal (defaults to 50051)
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 50051;
        
        // 2. Parse the list of other servers we need to connect to
        List<Integer> peerIds = new ArrayList<>();
        if (args.length > 1) {
            String[] peers = args[1].split(",");
            for (String peer : peers) {
                if (!peer.trim().isEmpty()) {
                    peerIds.add(Integer.parseInt(peer.trim()));
                }
            }
        }
        
        System.out.println("Starting Node on port: " + port);
        System.out.println("Peers: " + peerIds);
        
        // 3. Initialize the Hard Drive Storage (WriteAheadLog)
        // We name it using our port (e.g. kvstore_50051.wal) so that if we run 3 servers 
        // on the same laptop, they don't accidentally overwrite each other's files!
        WriteAheadLog wal = new WriteAheadLog("kvstore_" + port + ".wal");
        
        // 3.5 Read the entire WAL into memory so the Raft Leader can access historical logs for catching up followers
        List<com.cloudwick.kvstore.grpc.LogEntry> logList = java.util.Collections.synchronizedList(wal.readAll());
        
        // 4. Initialize the Raft Consensus Engine
        // This will immediately start the election timers in the background.
        RaftNode raftNode = new RaftNode(port, peerIds, logList);
        
        // 5. Initialize the Prometheus Metrics Web Server
        // We run this on port + 1000 (e.g. 51051) so you can view it in your web browser.
        MetricsExporter metrics = new MetricsExporter();
        metrics.start(port + 1000);
        
        // 6. Initialize the gRPC Network Server
        // We pass the WAL, logList, and RaftNode into the Service so it can use them when clients send requests.
        Server server = ServerBuilder.forPort(port)
                .addService(new KVStoreServiceImpl(wal, logList, raftNode))
                .build()
                .start();
        
        System.out.println("KV Store Server started, listening on " + port);
        
        // 7. Graceful Shutdown Hook
        // If someone hits CTRL+C in the terminal, we want to cleanly close our hard drive file 
        // and stop the web servers so we don't corrupt data.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            if (server != null) {
                server.shutdown();
            }
            try {
                wal.close();
                metrics.stop();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.err.println("*** server shut down");
        }));
        
        // Keep the main thread alive forever so the server doesn't instantly exit.
        server.awaitTermination();
    }
}

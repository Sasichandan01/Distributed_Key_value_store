package com.cloudwick.kvstore;

import com.cloudwick.kvstore.grpc.KVStoreServiceGrpc;
import com.cloudwick.kvstore.grpc.PutRequest;
import com.cloudwick.kvstore.grpc.PutResponse;
import com.cloudwick.kvstore.grpc.GetRequest;
import com.cloudwick.kvstore.grpc.GetResponse;
import com.cloudwick.kvstore.grpc.DeleteRequest;
import com.cloudwick.kvstore.grpc.DeleteResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class ClientMain {
    
    // The list of all known servers in our cluster
    private static final int[] CLUSTER_PORTS = {50051, 50052, 50053};
    
    // We remember who the leader was last time to save time on the next request!
    private static int currentKnownLeaderPort = 50051; 

    public static void main(String[] args) {
        System.out.println("--- Starting Smart Interactive Client ---");
        System.out.println("Available commands:");
        System.out.println("  PUT <key> <value>");
        System.out.println("  GET <key>");
        System.out.println("  EXIT");
        
        java.util.Scanner scanner = new java.util.Scanner(System.in);
        while (true) {
            System.out.print("\n> ");
            if (!scanner.hasNextLine()) break;
            
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;
            
            if (line.equalsIgnoreCase("EXIT")) {
                break;
            }
            
            String[] parts = line.split(" ", 3);
            String cmd = parts[0].toUpperCase();
            
            if (cmd.equals("PUT") && parts.length == 3) {
                boolean success = sendPutRequest(parts[1], parts[2]);
                if (success) System.out.println("Successfully saved data.");
            } else if (cmd.equals("GET") && parts.length == 2) {
                sendGetRequest(parts[1]);
            } else {
                System.out.println("Invalid command format.");
            }
        }
        System.out.println("Client closed.");
    }
    
    /**
     * "Smart Client" Implementation:
     * This method tries to send a PUT request to the known leader.
     * If the server replies "I am not the leader", the client automatically 
     * catches the error and tries the next server in the list until it succeeds!
     */
    private static boolean sendPutRequest(String key, String value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFromUtf8(value))
                .build();

        int attempts = 0;
        int maxAttempts = 3; // Prevent infinite loops if the cluster is completely down

        while (attempts < maxAttempts) {
            attempts++;
            int targetPort = currentKnownLeaderPort;
            
            System.out.println("Attempting PUT to Node at port " + targetPort + "...");
            
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", targetPort)
                    .usePlaintext()
                    .build();
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = KVStoreServiceGrpc.newBlockingStub(channel);

            try {
                PutResponse response = stub.put(request);
                
                // If we get here, it succeeded!
                System.out.println("SUCCESS! Node " + targetPort + " is the Leader.");
                channel.shutdown();
                return true;
                
            } catch (StatusRuntimeException e) {
                // If the node rejects us because it is a Follower...
                if (e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE) {
                    String errorMsg = e.getStatus().getDescription();
                    System.err.println("Node " + targetPort + " rejected the request: " + errorMsg);
                    
                    // --- LEADER HINTING LOGIC ---
                    // Parse the error message to see if the Follower gave us a hint!
                    if (errorMsg != null && errorMsg.contains("Leader is ")) {
                        try {
                            String portStr = errorMsg.substring(errorMsg.lastIndexOf(" ") + 1);
                            if (!portStr.equals("unknown")) {
                                int hintedPort = Integer.parseInt(portStr);
                                System.out.println("--> Received hint! Switching directly to port " + hintedPort);
                                currentKnownLeaderPort = hintedPort;
                            }
                        } catch (Exception parseEx) {
                            // If parsing fails, fall back to trying another random port
                            currentKnownLeaderPort = (targetPort == 50051) ? 50052 : 50053;
                        }
                    } else {
                        // If there is no hint, just guess another port
                        currentKnownLeaderPort = (targetPort == 50051) ? 50052 : 50053;
                    }
                } else {
                    System.err.println("Network error connecting to Node " + targetPort);
                    // Guess another port
                    currentKnownLeaderPort = (targetPort == 50051) ? 50052 : 50053;
                }
            } finally {
                channel.shutdown();
            }
            
            try { Thread.sleep(500); } catch (InterruptedException ignored) {} // Brief pause before retry
        }
        
        System.err.println("FATAL: Could not find any Leader in the cluster after " + maxAttempts + " attempts!");
        return false;
    }
    
    private static void sendGetRequest(String key) {
        int attempts = 0;
        int maxAttempts = 3;

        while (attempts < maxAttempts) {
            attempts++;
            int targetPort = currentKnownLeaderPort;
            
            System.out.println("Attempting GET from Node at port " + targetPort + "...");
            
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", targetPort)
                    .usePlaintext()
                    .build();
            KVStoreServiceGrpc.KVStoreServiceBlockingStub stub = KVStoreServiceGrpc.newBlockingStub(channel);
            
            try {
                GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());
                if (response.getFound()) {
                    System.out.println("Found data: " + response.getValue().toStringUtf8());
                } else {
                    System.out.println("Data not found!");
                }
                channel.shutdown();
                return; // Success!
            } catch (StatusRuntimeException e) {
                System.err.println("Network error connecting to Node " + targetPort + " for GET request.");
                // Guess another port
                currentKnownLeaderPort = (targetPort == 50051) ? 50052 : 50053;
            } finally {
                channel.shutdown();
            }
            
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
        System.err.println("FATAL: Could not reach any nodes for GET request!");
    }
}

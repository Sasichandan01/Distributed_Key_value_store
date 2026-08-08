package com.cloudwick.kvstore;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

public class MetricsExporter {
    
    // Counters
    public static final Counter PUT_REQUESTS = Counter.build()
            .name("kvstore_put_requests_total")
            .help("Total PUT requests processed.")
            .labelNames("node_id")
            .register();
            
    public static final Counter GET_REQUESTS = Counter.build()
            .name("kvstore_get_requests_total")
            .help("Total GET requests processed.")
            .labelNames("node_id")
            .register();
            
    public static final Counter LEADER_ELECTIONS = Counter.build()
            .name("raft_leader_elections_won_total")
            .help("Total number of times this node won a leader election.")
            .labelNames("node_id")
            .register();

    // Gauges
    public static final Gauge CURRENT_TERM = Gauge.build()
            .name("raft_current_term")
            .help("The current Raft term of the node.")
            .labelNames("node_id")
            .register();
            
    public static final Gauge NODE_ROLE = Gauge.build()
            .name("raft_node_role")
            .help("The current role of the node (0=Follower, 1=Candidate, 2=Leader).")
            .labelNames("node_id")
            .register();

    private HTTPServer server;

    public void start(int metricsPort) throws IOException {
        server = new HTTPServer(metricsPort);
        System.out.println("Prometheus metrics server started on port " + metricsPort);
    }
    
    public void stop() {
        if (server != null) {
            server.stop();
        }
    }
}

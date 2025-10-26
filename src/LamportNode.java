import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.RequestProcessorFactoryFactory;
//import org.apache.xmlrpc.server.RequestProcessorFactory.*;
import org.apache.xmlrpc.webserver.WebServer;
import org.apache.xmlrpc.XmlRpcException;

import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class LamportNode {
    private int nodeId;
    private int port;
    private List<NodeInfo> peers;
    private int clock = 0;
    private List<Request> requestQueue = new CopyOnWriteArrayList<>();

    public LamportNode(int nodeId, int port, List<NodeInfo> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = peers;
    }

    // --- Lamport clock helpers ---
    private synchronized void updateClock(int receivedTime) {
        clock = Math.max(clock, receivedTime) + 1;
    }

    private synchronized int incrementClock() {
        clock++;
        return clock;
    }

    // --- XML-RPC exposed methods ---
    public boolean requestCS(int timestamp, int requesterId) {
        updateClock(timestamp);
        requestQueue.add(new Request(timestamp, requesterId));
        Collections.sort(requestQueue);
        System.out.println("[Node " + nodeId + "] Received REQUEST from Node " + requesterId);
        return true;
    }

    public boolean releaseCS(int requesterId) {
        System.out.println("[Node " + nodeId + "] Received RELEASE from Node " + requesterId);
        requestQueue.removeIf(r -> r.nodeId == requesterId);
        return true;
    }

    // --- Critical section logic ---
    public void requestCriticalSection() throws Exception {
        int timestamp = incrementClock();
        requestQueue.add(new Request(timestamp, nodeId));
        Collections.sort(requestQueue);
        System.out.println("[Node " + nodeId + "] REQUEST CS at time " + timestamp);

        // Send request to all peers
        for (NodeInfo peer : peers) {
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL("http://localhost:" + peer.port + "/"));
            XmlRpcClient client = new XmlRpcClient();
            client.setConfig(config);

            Vector<Object> params = new Vector<>();
            params.add(timestamp);
            params.add(nodeId);
            client.execute("handler.requestCS", params);
        }

        // Wait until this node is at top of queue
        while (requestQueue.isEmpty() || requestQueue.get(0).nodeId != nodeId) {
            Thread.sleep(500);
        }

        enterCriticalSection();

        // Send release to all peers
        for (NodeInfo peer : peers) {
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL("http://localhost:" + peer.port + "/"));
            XmlRpcClient client = new XmlRpcClient();
            client.setConfig(config);

            Vector<Object> params = new Vector<>();
            params.add(nodeId);
            client.execute("handler.releaseCS", params);
        }
    }

    private void enterCriticalSection() throws InterruptedException {
        System.out.println("[Node " + nodeId + "] ENTERING CRITICAL SECTION");
        Thread.sleep(3000); // Simulate work
        System.out.println("[Node " + nodeId + "] EXITING CRITICAL SECTION");
        requestQueue.removeIf(r -> r.nodeId == nodeId);
    }

    // --- Start XML-RPC server with factory ---
    public void startServer() throws Exception {
        LamportNodeHandler.setNode(this); // attach current node

        WebServer server = new WebServer(port);
        XmlRpcServer xmlRpcServer = server.getXmlRpcServer();

        PropertyHandlerMapping phm = new PropertyHandlerMapping();
        phm.addHandler("handler", LamportNodeHandler.class); // register the class
        xmlRpcServer.setHandlerMapping(phm);

        server.start();
        System.out.println("[Node " + nodeId + "] Listening on port " + port);
    }

    // --- Helper classes ---
    public static class NodeInfo {
        int nodeId;
        int port;
        NodeInfo(int id, int p) { nodeId = id; port = p; }
    }

    public static class Request implements Comparable<Request> {
        int timestamp;
        int nodeId;
        Request(int ts, int id) { timestamp = ts; nodeId = id; }

        @Override
        public int compareTo(Request o) {
            if (timestamp == o.timestamp)
                return Integer.compare(nodeId, o.nodeId);
            return Integer.compare(timestamp, o.timestamp);
        }
    }

    // --- Main runner i have assumed we have 3 nodes---
    public static void main(String[] args) throws Exception {
        Map<Integer, NodeInfo> nodes = new HashMap<>();
        nodes.put(1, new NodeInfo(1, 9001));
        nodes.put(2, new NodeInfo(2, 9002));
        nodes.put(3, new NodeInfo(3, 9003));

        for (int id : nodes.keySet()) {
            NodeInfo current = nodes.get(id);
            List<NodeInfo> peers = new ArrayList<>();
            for (NodeInfo n : nodes.values()) {
                if (n.nodeId != id) peers.add(n);
            }

            LamportNode node = new LamportNode(current.nodeId, current.port, peers);

            new Thread(() -> {
                try {
                    node.startServer();
                    Thread.sleep(2000 + new Random().nextInt(2000)); // staggered start
                    node.requestCriticalSection();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

public class LamportNodeHandler {
    private static LamportNode node; // static, shared among all instances

    public LamportNodeHandler() {
        // XML-RPC requires public no-arg constructor
    }

    public static void setNode(LamportNode n) {
        node = n;
    }

    public boolean requestCS(int timestamp, int requesterId) {
        return node.requestCS(timestamp, requesterId);
    }

    public boolean releaseCS(int requesterId) {
        return node.releaseCS(requesterId);
    }
}

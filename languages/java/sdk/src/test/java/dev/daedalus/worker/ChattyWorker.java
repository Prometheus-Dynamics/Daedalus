package dev.daedalus.worker;

import java.util.Map;

/** A worker whose node prints to System.out: the print must reach stderr, not the packet stream. */
public final class ChattyWorker {
    private ChattyWorker() {}

    @Node(id = "demo.chatty", inputs = {"x:i64"}, outputs = {"out:i64"})
    public static Object chatty(Map<String, Object> in, Context cx) {
        System.out.println("hello from a body");
        return in.get("x");
    }

    public static void main(String[] args) throws Exception {
        Worker.serve(ChattyWorker.class);
    }
}

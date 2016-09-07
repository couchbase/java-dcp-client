package examples;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

import java.util.concurrent.atomic.AtomicInteger;

public class CountDocs {

    public static void main(String[] args) throws Exception {
        // Configure the client with a custom bucket name against localhost.
        Client client = Client.configure()
            .hostnames("localhost")
            .bucket("travel-sample")
            .build();

        // Attach a Listener for the control events which discards them all.
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf controlEvent) {
                controlEvent.release();
            }
        });

        // Attach a Listener for the data events which counts the mutations.
        final AtomicInteger numDocsFound = new AtomicInteger();
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
//                    String key = DcpMutationMessage.key(event).toString(CharsetUtil.UTF_8);
//                    String content = DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
//                    System.out.println("Found Key " + key + " with Content " + content);
                    numDocsFound.incrementAndGet();
                }
                event.release();
            }
        });

        // Connect to the cluster
        client.connect().await();


        client.initFromBeginningToNow().await();
        client.startStreams().await();

        while(true) {
            Thread.sleep(1000);
            System.out.println("Found " + numDocsFound.get() + " number of docs so far.");
            if (client.sessionState().isAtEnd()) {
                break;
            }
        }

        System.err.println("DONE");

        client.disconnect().await();

        // Start streaming of all partitions from beginning with no end
//        client.initializeFromBeginningToNoEnd().await();
//        client.startStreams().await();
//
//        while(true) {
//            Thread.sleep(1000);
//            System.out.println("Found " + numDocsFound.get() + " number of docs so far.");
//        }

        // Shut down once done.
        // client.disconnect().await();
    }
}

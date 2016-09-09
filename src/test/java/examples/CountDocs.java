package examples;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.transcoder.JacksonTransformers;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class CountDocs {

    public static void main(String[] args) throws Exception {
        // Configure the client with a custom bucket name against localhost.
        final Client client = Client.configure()
            .hostnames("localhost")
            .bucket("travel-sample")
            .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 10240)
            .bufferAckWatermark(75)
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
                    numDocsFound.incrementAndGet();
                }
                client.acknowledgeBuffer(event);
                event.release();
            }
        });


        // Connect to the cluster
        client.connect().await();

        client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).await();

//        byte[] state = client.sessionState().toJson();
//
//        System.err.println(new String(state, CharsetUtil.UTF_8));
//        client.initFromNowToNoEnd().await();
//
//        client.initFromJson(state).await();

        client.startStreaming().await();

        while(true) {
            Thread.sleep(1000);
            System.out.println("Found " + numDocsFound.get() + " number of docs so far.");
            if (client.sessionState().isAtEnd()) {
                break;
            }
        }

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

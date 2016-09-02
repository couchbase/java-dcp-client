package examples;


import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;

public class Rebalance {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    public static void main(String... args) throws Exception {

        Client client = Client.configure()
            .hostnames("10.142.150.102")
            .bucket("default")
            .build();

        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                event.release();
            }
        });

        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                event.release();
            }
        });

        client.connect().await();

        client.initializeFromNowToNoEnd().await();

        client.startStreams(1, 2, 3, 4).await();

        Thread.sleep(10000000);
    }
}

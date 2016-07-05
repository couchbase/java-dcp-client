package examples;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.TimeUnit;

public class CountBytesPerSecond {

    public static void main(String... args) {

        Client client = Client
            .configure()
            .clusterAt("127.0.0.1")
            .bucket("travel-sample")
            .dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ByteBuf event) {
                    System.err.println(event);
                }
            })
            .build();

        client.connect().await(5, TimeUnit.SECONDS);
    }
}

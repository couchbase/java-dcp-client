# Couchbase Java DCP Client
This repository contains a pure java-based implementation for a Couchbase
DCP (Database Change Protocol) client. It is currently a work in progress
and intended to be the primary DCP client going forward, eventually 
deprecating the experimental supported inside `core-io`.

It supports:

 - [x] Low Overhead Streaming
 - [x] Async from top to bottom
 - [x] Stream specific vbuckets
 - [x] Manual Start/Stop of streams
 - [x] Noop Acknowledgements
 - [x] Flow Control
 - [ ] Simple Session Loading/Persistence

 - [ ] Full Rebalance

# Installation
Currently no artifacts are published to maven central, but you can check
out the project from github and build it on your own. It is a maven-based
project so simply run

```
$ git clone https://github.com/couchbaselabs/java-dcp-client.git
$ cd java-dcp-client
$ mvn install
```

Right now it will install the `com.couchbase.client:dcp-client` artifact
with the `0.1.0-SNAPSHOT` version. You can then depend on it in your
project.

# Basic Usage
The simplest way is to initiate a stream against `localhost` and open
all streams available. You always need to attach a callback for both the
config and the data events - in the simplest case all the messages are
just discarded. It's important to release the buffers!

The following example connects to the `beer-sample` bucket, streams
all the documents it has, counts the number of documents and prints
out the result:

```java
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicInteger;

public class CountDocs {

    public static void main(String[] args) throws Exception {
        // Configure the client with a custom bucket name against localhost.
        Client client = Client.configure()
            .hostnames("localhost")
            .bucket("beer-sample")
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
                event.release();
            }
        });

        // Connect to the cluster
        client.connect().await();

        // Start streaming of all partitions from beginning with no end
        client.startFromBeginningWithNoEnd().await();

        while(true) {
            Thread.sleep(1000);
            System.out.println("Found " + numDocsFound.get() + " number of docs so far.");
        }

        // Shut down once done.
        // client.disconnect().await();
    }
}
```

## Dealing with Messages and ByteBufs
To save allocations the actual data you are interacting with are raw
netty `ByteBuf`s that may be pooled, depending on the configuration. So
it is always important to `release()` them when not needed anymore.

Since working with the raw buffers is not fun, the client provides
flyweights that allow you to extract the information out of the buffers
easily. Consult the docs for information on which message types to expect
when, but as an example if you want to print the key and content of an 
incoming mutation in the data handler you can do it like this:

```java
if (DcpMutationMessage.is(event)) {
    String key = DcpMutationMessage.key(event).toString(CharsetUtil.UTF_8);
    String content = DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
    System.out.println("Found Key " + key + " with Content " + content);
}
```


# Advanced Usage
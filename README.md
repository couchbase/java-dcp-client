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
 - [x] Session Loading/Persistence for restartability
 - [x] Rebalance Support
 - [ ] (Re)Start from specific session state
 - [ ] Pausing and Restarts

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
        
        // Initialize the session state to stream from 0 to infinity
        client.initializeFromBeginningToNoEnd().await();
        
        // Use this if you want to start now and no backfill:
        // client.initializeFromNowToNoEnd().await();

        // Start streaming on all partitions
        client.startStreams().await();

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

## Flow Control
To handle slow clients better and to make it possible that the client signals
backpressure to the server (that it should stop sending new data when the
client is busy processing the previous ones) flow control tuneables are
available.

Handling flow control consist of two stages: first, you need to enable
it during bootstrap and then acknowledge specific message types as soon
as you are done processing them.

### Configuring Flow Control
To activate flow control, the `DcpControl.Names.CONNECTION_BUFFER_SIZE`
control param needs to be set to a value greater than zero. A reasonable
start value to test would be "10240" (10K).

Next, you also need to set the `bufferAckWatermark` to a value which is
equal or smaller than the connection buffer size. Every time a message
is acknowledged the client accumulates up to the watermark and only if
the watermark is exceeded the acknowledgement is sent. This helps with
cutting down on network traffic and to reduce the workload on the server
side for accounting.

### Acknowledging Messages
If you do not acknowledge the bytes read for specific messages, the server
will stop streaming new messages when the `CONNECTION_BUFFER_SIZE` is
reached.

The following messages need to be acknowledged by the user:

 - `DcpSnapshotMarkerMessage` (on the `ControlEventHandler`)
 - `DcpMutationMessage` (on the `DataEventHandler`)
 - `DcpDeletionMessage` (on the `DataEventHandler`)
 - `DcpExpirationMessage` (on the `DataEventHandler`)
 
Acknowledging works by passing the number of bytes from the event to the
`Client#acknowledgeBytes` method. Note that the vbucket id also needs to
be passed since the client needs to know against which connection the
acknowledge message should be performed.

A simple way to do this is the following:

```java
client.acknowledgeBuffer(event);
```

This method extracts the vbucket ID and gets the number of readable bytes
out of it. When you already did consume the bytes and the reader index
of the buffer is not the number of bytes orginally, you can fall back to
the lower level API:

```java
client.acknowledgeBuffer(vbid, numBytes);
```


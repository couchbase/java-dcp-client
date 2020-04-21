# Couchbase Java DCP Client
This repository contains a purely java-based implementation for a Couchbase DCP (Database Change Protocol) client.

**Important: The `java-dcp-client` is not officially supported by Couchbase directly. It is used as a fundamental building block for higher-level (supported) libraries like our kafka or elasticsearch connectors. Use at your own Risk!** 


It supports:

 - [x] Low Overhead Streaming
 - [x] Async from top to bottom
 - [x] Stream specific vbuckets
 - [x] Manual Start/Stop of streams
 - [x] Noop Acknowledgements and Dead Connection Detection
 - [x] Flow Control
 - [x] Session Management for restartability
 - [x] Rebalance Support
 - [x] Export current session state for durability (ships with JSON)
 - [x] Start and Restart from specific session state (import durable state)
 - [x] Pausing and Restarts
 - [x] Stream up to a specific point in time for a vbucket and then stop
 - [x] Proper shutdown/disconnect and cleanup

# Installation
We publish the releases (including pre-releases to maven central):

```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>dcp-client</artifactId>
    <version>0.27.0</version>
</dependency>
```

If you want the bleeding edge, you can check
out the project from github and build it on your own. It is a maven-based
project so simply run

```
$ git clone https://github.com/couchbase/java-dcp-client.git
$ cd java-dcp-client
$ mvn install
```

This local build will install the `com.couchbase.client:dcp-client` artifact
with the next `SNAPSHOT` version. You can then depend on it in your
project.

# Basic Usage
The simplest way is to initiate a stream against `localhost` and open
all streams available. You always need to attach a callback for both the
config and the data events - in the simplest case all the messages are
just discarded. **It's important to release the buffers!**

Please check out the [examples](https://github.com/couchbase/java-dcp-client/tree/master/src/test/java/examples)!

The following example connects to the `travel-sample` bucket and prints
out all subsequent mutations and deletions that occur.

```java
// Connect to localhost and use the travel-sample bucket
final Client client = Client.configure()
    .hostnames("localhost")
    .bucket("travel-sample")
    .build();

// Don't do anything with control events in this example
client.controlEventHandler(new ControlEventHandler() {
    @Override
    public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        event.release();
    }
});

// Print out Mutations and Deletions
client.dataEventHandler(new DataEventHandler() {
    @Override
    public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpMutationMessage.is(event)) {
            System.out.println("Mutation: " + DcpMutationMessage.toString(event));
            // You can print the content via DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        } else if (DcpDeletionMessage.is(event)) {
            System.out.println("Deletion: " + DcpDeletionMessage.toString(event));
        }
        event.release();
    }
});

// Connect the sockets
client.connect().await();

// Initialize the state (start now, never stop)
client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();

// Start streaming on all partitions
client.startStreaming().await();

// Sleep for some time to print the mutations
// The printing happens on the IO threads!
Thread.sleep(TimeUnit.MINUTES.toMillis(10));

// Once the time is over, shutdown.
client.disconnect().await();
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
    String key = DcpMutationMessage.keyString(event);
    String content = DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
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

 - `DcpSnapshotMarkerRequest` (on the `ControlEventHandler`)
 - `DcpMutationMessage` (on the `DataEventHandler`)
 - `DcpDeletionMessage` (on the `DataEventHandler`)
 - `DcpExpirationMessage` (on the `DataEventHandler`)

Acknowledging works by calling the `ChannelFlowController#ack` method.

A simple way to do this is the following:

```java
flowController.ack(event);
```

This method extracts the number of readable bytes out of it.
When you already did consume the bytes and the reader index
of the buffer is not the number of bytes orginally, you can fall back to
the lower level API:

```java
flowController.ack(numBytes);
```

### SSL (Couchbase Enterprise feature)

Read in details about SSL in Couchbase on
[our documentation](http://developer.couchbase.com/documentation/server/4.5/sdk/java/managing-connections.html).
Here we will just post quick start steps:

1. Download and store in file cluster certificate from "Security" -> "Root Certificate" section on Admin Console.
2. Import this certificate using keytool:

        keytool -importcert -keystore /tmp/keystore \
                            -storepass secret \
                            -file /tmp/cluster.cert

3. And update configuration of the DCP client:

    ``` java
    final Client client = Client.configure()
            .hostnames("localhost")
            .bucket("travel-sample")
            .sslEnabled(true)
            .sslKeystoreFile("/tmp/keystore")
            .sslKeystorePassword("secret")
            .build();
    ```

### System Events

Since the 0.7.0 release, the client implements a notification service, which allows you to react on events, which are
not tied directly to protocol and data transmission.  For example, connection errors, or notifications about stream
completion when the end sequence number wasn't set to infinity. The following example subscribes a handler to system
events to find out when partition 42 is done with data transmission:

``` java
client.systemEventHandler(new SystemEventHandler() {
    @Override
    public void onEvent(CouchbaseEvent event) {
        if (event instanceof StreamEndEvent) {
            StreamEndEvent streamEnd = (StreamEndEvent) event;
            if (streamEnd.partition() == 42) {
                System.out.println("Stream for partition 42 has ended (reason: " + streamEnd.reason() + ")");
            }
        }
    }
});
```

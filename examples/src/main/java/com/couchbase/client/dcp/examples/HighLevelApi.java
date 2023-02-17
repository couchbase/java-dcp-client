/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.examples;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.FlowControlMode;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;

/**
 * This example demonstrates how to use the high level API to register a listener
 * that performs blocking operations and uses automatic flow control.
 * <p>
 * It also shows how to safely track, save, and restore the stream offsets
 * across program runs.
 */
public class HighLevelApi {

  private static final int BYTES_PER_MEGABYTE = 1024 * 1024;

  public static void main(String[] args) throws Exception {

    final String bucketName = "travel-sample";

    try (Client client = Client.builder()
        .credentials("Administrator", "password")
        .seedNodes("localhost")
        .bucket(bucketName)
        .flowControl(64 * BYTES_PER_MEGABYTE)
        .userAgent("HighLevelApiExample", "0.1", "bucket:" + bucketName)
        .build()) {

      // If the listener detects a fatal error, it will tell the main thread
      // by submitting the error to this queue.
      final LinkedBlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

      // Preserves stream state between runs of this program.
      final StreamOffsetTracker offsetTracker = new StreamOffsetTracker(
          Paths.get("stream-offsets-" + bucketName + ".json"));

      /*
       * Register a listener to receive database change events, with automatic
       * flow control. In automatic flow control mode, you can and should do
       * your work (blocking I/O, expensive computations, etc.) in the callback
       * handler thread.
       *
       * Processing events in the callback thread implicitly generates
       * backpressure which the DCP client uses to regulate its flow control
       * buffer and prevent OutOfMemory errors.
       *
       * This example just prints the document key and content length.
       */
      client.listener(new DatabaseChangeListener() {
        @Override
        public void onFailure(StreamFailure streamFailure) {
          fatalErrorQueue.offer(streamFailure.getCause());
        }

        @Override
        public void onMutation(Mutation mutation) {
          System.out.println("MUT: " + mutation.getKey() + " (" + mutation.getContent().length + " bytes)");
          offsetTracker.accept(mutation); // remember we processed this stream element
        }

        @Override
        public void onDeletion(Deletion deletion) {
          System.out.println("DEL: " + deletion.getKey());
          offsetTracker.accept(deletion); // remember we processed this stream element
        }

      }, FlowControlMode.AUTOMATIC); // let the library handle flow control

      // Connect the client. Need to connect in order to discover number of partitions.
      client.connect().block();

      // Restore saved stream state (or initialize state for streaming from beginning).
      offsetTracker.load(client.numPartitions());

      // Save stream state when program exits. A real program might save the offsets
      // periodically as well.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          offsetTracker.save();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }));

      // Start the streams! The offset map's keyset determine which partitions are streamed,
      // and the values determine the starting point for each partition.
      client.resumeStreaming(offsetTracker.offsets()).block();

      // Wait until a fatal error occurs or the program is killed.
      fatalErrorQueue.take().printStackTrace();
    }
  }

  /**
   * A helper class that's basically a wrapper around a concurrent hash map,
   * with methods for saving and loading the contents of the map to disk.
   */
  private static class StreamOffsetTracker {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
      // required in order to deserialize JSON to StreamOffset
      mapper.registerModule(new ParameterNamesModule());
    }

    private final Path path; // filesystem location of offset storage file

    private final ConcurrentMap<Integer, StreamOffset> offsets = new ConcurrentHashMap<>();

    public StreamOffsetTracker(Path path) {
      this.path = requireNonNull(path).toAbsolutePath();
    }

    public void accept(DocumentChange change) {
      offsets.put(change.getVbucket(), change.getOffset());
    }

    public void save() throws IOException {
      Path temp = Files.createTempFile(path.getParent(), "stream-offsets-", "-tmp.json");
      Files.write(temp, mapper.writeValueAsBytes(offsets));
      Files.move(temp, path, ATOMIC_MOVE);
      System.out.println("Saved stream offsets to " + path);
      System.out.println("The next time this example runs, it will resume streaming from this point.");
    }

    public void load(int numPartitions) throws IOException {
      System.out.println("Restoring stream offsets from " + path.toAbsolutePath());

      // Assume there's no save file and we're going to stream from the beginning.
      offsets.clear();
      for (int i = 0; i < numPartitions; i++) {
        offsets.put(i, StreamOffset.ZERO);
      }

      // Overlay the saved offsets.
      try (InputStream is = new FileInputStream(path.toFile())) {
        offsets.putAll(mapper.readValue(is, new TypeReference<Map<Integer, StreamOffset>>() {
        }));

      } catch (FileNotFoundException e) {
        System.out.println("No saved offsets.");
      }
    }

    public Map<Integer, StreamOffset> offsets() {
      return offsets;
    }
  }

}

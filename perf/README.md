# Glue for the Couchbase Perfrunner framework.

**All commands must be run from the project root directory.**

To build the test client:

    perf/build.sh

To run the test client:

    perf/run.sh <connection-string> <dcp-message-count> \
                <config-properties-file> [process-tracer]

* `connection-string` - A Couchbase connection string with username, password, host(s), and bucket.
                        Example: `couchbase://alice:password@127.0.0.1/my-bucket`
* `dcp-message-count` - Total number of DCP mutation, expiration, and deletion messages
                        the client should receive before terminating.
* `config-properties-file` - Path to a Java properties file containing the settings
                             for this run. Pre-defined configurations are available
                             in the `perf/config` directory.
* `process-tracer` - An arbitrary, opaque string that will be passed to the spawned
                     client Java process as a command line argument. It may be used
                     to distinguish the client process from other Java processes.

Reports are generated in the `target/perf` directory.

Example:

    perf/build.sh
    perf/run.sh couchbase://Administrator:password@127.0.0.1/my-bucket 500000 \
                perf/config/compression-enabled.properties xyzzy


#!/bin/sh

# See README.md for example usage.

args="$@"

mvn exec:java -Dexec.cleanupDaemonThreads=false \
              -Dexec.classpathScope=test \
              -Dexec.mainClass=com.couchbase.client.dcp.perfrunner.PerformanceTestDriver \
              "-Dexec.args=$args"

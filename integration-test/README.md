To run the integration tests, go to the `aggregator` directory and run `mvn verify`.

To debug in your IDE, first go to the `aggregator` directory and run `mvn package`.
Then set a breakpoint in one of the integration tests and run the test in debug mode.
When the agent container starts up, it will log a message:

    DCP Test Agent listening for debugger on port ...

You can then debug the agent "remotely" by attaching a debugger to that
local port and setting a breakpoint in one of the `*ServiceImpl` classes.

NOTE: When running the integration tests from your IDE, you'll need to rebuild
the agent whenever you make changes to the service classes. To rebuild,
run `mvn package` in `aggregator` project.

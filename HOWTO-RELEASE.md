# Release Instructions

This is a guide for Couchbase employees. It describes how to cut a release of this project
and publish it to the Maven Central Repository.

## Prerequisites

You will need:

* The `gpg` command-line program and a PGP key. Mac users, grab `gpg` from
  https://gpgtools.org and enjoy
  [this setup guide](http://notes.jerzygangi.com/the-best-pgp-tutorial-for-mac-os-x-ever/).
  Generate a PGP key with your `couchbase.com` email address and uploaded it
  to a public keyserver. For bonus points, have a few co-workers sign your key.
* To tell git about your signing key: `git config --global user.signingkey DEADBEEF`
  (replace `DEADBEEF` with the id of your PGP key).
* A local Docker installation, if you wish to run the integration tests.

## Let's do this!

Start by checking the snapshot builds found [here](https://github.com/couchbase/java-dcp-client/actions/workflows/deploy-snapshot.yml) to ensure that everything looks good with the current commit.

To run the integration tests, run `mvn clean verify`
without specifying a profile.
When you're satisfied with the test results, it's time to...

## Bump the project version number

1. Edit `pom.xml` and remove the `-SNAPSHOT` suffix from the version string.
2. Edit `examples/pom.xml` and update the `dcp.client.version` property to match the version string in Step 1.
3. Commit these changes, with message "Prepare x.y.z release"
   (where x.y.z is the version you're releasing). Note: the snapshot build for that commit will fail because we removed the snapshot suffix. This is expected.

## Tag the release

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).

Use the previous version's tag message (e.g. `git show 0.11.0`) as a template for
the new version's tag message and then push the tag.

## Go! Go! Go!

Run the release workflow found [here](https://github.com/couchbase/java-dcp-client/actions/workflows/deploy-release.yml) supplying the tag name you previously pushed.

## Prepare for next dev cycle

Increment the version number in `pom.xml` and restore the `-SNAPSHOT` suffix.
Update the `dcp.client.version` property in `examples/pom.xml` to refer to the
new snapshot version.
Commit and push to Gerrit. Breathe in. Breathe out.

## Troubleshooting

* Take another look at the Prerequisites section. Did you miss anything?
* [This gist](https://gist.github.com/danieleggert/b029d44d4a54b328c0bac65d46ba4c65) has
  some tips for making git and gpg play nice together.
* If deployment fails because the artifacts are missing PGP signatures, make sure your Maven
  command line includes `-Prelease` (or `-Pstage`) when running `mvn deploy`.
  Note that this is a *profile* so it's specified with `-P`.

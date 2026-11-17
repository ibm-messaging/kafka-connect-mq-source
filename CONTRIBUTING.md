# Contributing to the Kafka Connect source connector for IBM MQ

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

## Pull requests are always welcome

We are always thrilled to receive pull requests, and do our best to process them as fast as possible. Not sure if that typo is worth a pull request? Do it! We will appreciate it.

To contribute code or documentation, please submit a [pull request](https://github.com/ibm-messaging/kafka-connect-mq-source/pulls).

A good way to familiarize yourself with the codebase and contribution process is
to look for and tackle low-hanging fruit in the [issue tracker](https://github.com/ibm-messaging/kafka-connect-mq-source/issues).

## Testing with ARM64

For instructions on testing with ARM64, see [IBM MQ 9.3.3.0 container image now available for Apple Silicon](https://community.ibm.com/community/user/blogs/richard-coppen/2023/06/30/ibm-mq-9330-container-image-now-available-for-appl).

## Create issues

If you would like to implement a new feature, please [raise an issue](https://github.com/ibm-messaging/kafka-connect-mq-source/issues)
before sending a pull request so the feature can be discussed before you start working on it.

> Note: We appreciate your effort, and want to avoid a situation where a contribution
requires extensive rework (by you or by us), sits in backlog for a long time, or
cannot be accepted at all!

### ...but check for existing issues first

Please take a moment to check that an issue doesn't already exist documenting your bug report or improvement proposal. If it does, it never hurts to add a quick "+1" or "I have this problem too". This will help prioritize the most common problems and requests.

### Merge approval

The project maintainers use LGTM (Looks Good To Me) in comments on the code
review to indicate acceptance.

For a list of the maintainers, see the [MAINTAINERS.md](MAINTAINERS.md) page.

## Releasing

Releases are built and uploaded by the
[Build and create release](.github/workflows/github-build-release.yml) GitHub Actions workflow.
It starts in one of two ways. Publishing a release directly in the GitHub UI does not start it.
You need write access to the repository.

1. Set the version in `pom.xml` to the release version (for example `2.8.0`) and merge that change to `main`.
   The workflow stops if the `pom.xml` version does not match the tag.
2. Start the release in **one** of these ways. The tag must look like `v<major>.<minor>.<patch>`.
   - **From GitHub:** open **Actions > Build and create release > Run workflow**, select `main`, and enter the
     new tag (for example `v2.8.0`). Leave **overwrite** unticked. You do not need to create the tag yourself.
     The workflow builds the tip of `main`, and after the build succeeds it creates the tag on that commit.
   - **From the terminal:** tag a commit that is on `main` and push the tag. The push starts the workflow.
     ```
     git tag v2.8.0
     git push upstream v2.8.0
     ```
3. When the run succeeds, open the new **draft** release under **Releases**. It has generated notes and
   three jars attached:
   - `kafka-connect-mq-source-<version>-jar-with-dependencies.jar`
   - `kafka-connect-mq-source-<version>-dependencies-exc-mq.jar`
   - `kafka-connect-mq-source-<version>.jar`

   Check the notes and the jars, then click **Publish release**.

### If a run fails

Run the workflow again from the Actions tab with the same tag. Re-running is only possible this way,
because `overwrite` is not available on a tag push. If the tag was already created, the re-run builds from that tag,
not from the current tip of `main`.

- If the release is still a **draft**, tick **overwrite** to replace jars that were already uploaded.
  Nobody can have downloaded a draft, so this is safe.
- If the release is already **published**, **overwrite** is refused. Published jars are never replaced, because
  their checksums would change for people who already downloaded them. To fix a published release,
  make a new patch release instead.

### Checks the workflow makes

It stops before building if:
- the tag does not match `v<major>.<minor>.<patch>`, for example `v2.8` or `2.8.0`;
- it was started manually from a branch other than `main`;
- the tag already exists but points at a commit that is not on `main`;
- the `pom.xml` version does not match the tag.

## Legal

Each source file must include a license header for the Apache
Software License 2.0.
e.g.

```txt
/**
 * Copyright <date>-<date> <holder>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
```

We have tried to make it as easy as possible to make contributions. This
applies to how we handle the legal aspects of contribution. We use the
same approach - the [Developer's Certificate of Origin 1.1 (DCO)](https://github.com/hyperledger/fabric/blob/master/docs/source/DCO1.1.txt) - that the Linux® Kernel [community](https://elinux.org/Developer_Certificate_Of_Origin)
uses to manage code contributions.

The rules are pretty simple: if you
can certify the below (from [developercertificate.org](http://developercertificate.org/)):

```txt
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
660 York Street, Suite 102,
San Francisco, CA 94110 USA

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

then you just add a line to end of the git commit message:

```txt
Signed-off-by: John Doe <john.doe@example.com>
```

using your real name. Sorry, no pseudonyms or anonymous contributions.

Many Git UI tools have support for adding the `Signed-off-by` line to the end of your commit
message. This line can be automatically added by the `git commit` command by using the `-s` option.

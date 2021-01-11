**Concourse Pipeline** [![Concourse Build Status](https://prod.ci.gpdb.pivotal.io/api/v1/teams/main/pipelines/gpdb_master/badge)](https://prod.ci.gpdb.pivotal.io/teams/main/pipelines/gpdb_master) |
**Travis Build** [![Travis Build Status](https://travis-ci.org/greenplum-db/gpdb.svg?branch=master)](https://travis-ci.org/greenplum-db/gpdb)

----------------------------------------------------------------------

![Greenplum](logo-greenplum.png)

Greenplum Database (GPDB) is an advanced, fully featured, open
source data warehouse, based on PostgreSQL. It provides powerful and rapid analytics on
petabyte scale data volumes. Uniquely geared toward big data
analytics, Greenplum Database is powered by the world’s most advanced
cost-based query optimizer delivering high analytical query
performance on large data volumes.

The Greenplum project is released under the [Apache 2
license](http://www.apache.org/licenses/LICENSE-2.0). We want to thank
all our past and present community contributors and are really interested in
all new potential contributions. For the Greenplum Database community
no contribution is too small, we encourage all types of contributions.

## Overview

A Greenplum cluster consists of a __master__ server, and multiple
__segment__ servers. All user data resides in the segments, the master
contains only metadata. The master server, and all the segments, share
the same schema.

Users always connect to the master server, which divides up the query
into fragments that are executed in the segments, and collects the results.

More information can be found on the [project website](https://greenplum.org/).

## Building Greenplum Database with GPORCA
GPORCA is a cost-based optimizer which is used by Greenplum Database in
conjunction with the PostgreSQL planner.  It is also known as just ORCA,
and Pivotal Optimizer. The code for GPORCA resides in a
separate repository, below are steps outlining how to build Greenplum with
GPORCA enabled.

### Installing dependencies (for macOS developers)
Follow [these macOS steps](README.macOS.md) for getting your system ready for GPDB

### Installing dependencies (for Linux developers)
Follow [appropriate linux steps](README.linux.md) for getting your system ready for GPDB

<a name="buildOrca"></a>
### Build the optimizer
#### Automatically with Conan dependency manager

```bash
cd depends
./configure
make
make install_local
cd ..
```

#### Manually
Follow the directions in the [GPORCA README](https://github.com/greenplum-db/gporca).

**Note**: Get the latest GPORCA `git pull --ff-only` if you see an error message like below:

    checking Checking ORCA version... configure: error: Your ORCA version is expected to be 2.33.XXX

### Build the database

```
# Configure build environment to install at /usr/local/gpdb
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb

# Compile and install
make -j8
make -j8 install

# Bring in greenplum environment into your running shell
source /usr/local/gpdb/greenplum_path.sh

# Start demo cluster
make create-demo-cluster
# (gpdemo-env.sh contains __PGPORT__ and __MASTER_DATA_DIRECTORY__ values)
source gpAux/gpdemo/gpdemo-env.sh
```

The directory and the TCP ports for the demo cluster can be changed on the fly.
Instead of `make cluster`, consider:

```
DATADIRS=/tmp/gpdb-cluster PORT_BASE=5555 make cluster
```

The TCP port for the regression test can be changed on the fly:

```
PGPORT=5555 make installcheck-world
```

Once build and started, run `psql` and check the GPOPT (e.g. GPORCA) version:

```
select gp_opt_version();
```

To turn GPORCA off and use Postgres planner for query optimization:
```
set optimizer=off;
```

If you want to clean all generated files
```
make distclean
```

## Running tests

* The default regression tests

```
make installcheck-world
```

* The top-level target __installcheck-world__ will run all regression
  tests in GPDB against the running cluster. For testing individual
  parts, the respective targets can be run separately.

* The PostgreSQL __check__ target does not work. Setting up a
  Greenplum cluster is more complicated than a single-node PostgreSQL
  installation, and no-one's done the work to have __make check__
  create a cluster. Create a cluster manually or use gpAux/gpdemo/
  (example below) and run the toplevel __make installcheck-world__
  against that. Patches are welcome!

* The PostgreSQL __installcheck__ target does not work either, because
  some tests are known to fail with Greenplum. The
  __installcheck-good__ schedule in __src/test/regress__ excludes those
  tests.

* When adding a new test, please add it to one of the GPDB-specific tests,
  in greenplum_schedule, rather than the PostgreSQL tests inherited from the
  upstream. We try to keep the upstream tests identical to the upstream
  versions, to make merging with newer PostgreSQL releases easier.

## Alternative Configurations

### Building GPDB without GPORCA

Currently, GPDB is built with GPORCA by default so latest GPORCA libraries and headers need
to be available in the environment. [Build and Install](#buildOrca) the latest GPORCA.

If you want to build GPDB without GPORCA, configure requires `--disable-orca` flag to be set.
```
# Clean environment
make distclean

# Configure build environment to install at /usr/local/gpdb
./configure --disable-orca --with-perl --with-python --with-libxml --prefix=/usr/local/gpdb
```

### Building GPDB with PXF

PXF is an extension framework for GPDB to enable fast access to external hadoop datasets.
Refer to [PXF extension](gpcontrib/pxf/README.md) for more information.

Currently, GPDB is built with PXF by default (--enable-pxf is on).
In order to build GPDB without pxf, simply invoke `./configure` with additional option `--disable-pxf`.
PXF requires curl, so `--enable-pxf` is not compatible with the `--without-libcurl` option.

### Building GPDB with gpperfmon enabled

gpperfmon tracks a variety of queries, statistics, system properties, and metrics.
To build with it enabled, change your `configure` to have an additional option
`--enable-gpperfmon`

See [more information about gpperfmon here](gpAux/gpperfmon/README.md)

gpperfmon is dependent on several libraries like apr, apu, and libsigar

### Building GPDB with Python3 enabled

GPDB supports Python3 with plpython3u UDF

See [how to enable Python3](src/pl/plpython/README.md) for details.


### Building GPDB client tools on Windows

See [Building GPDB client tools on Windows](README.windows.md) for details.

## Development with Docker

See [README.docker.md](README.docker.md).

We provide a docker image with all dependencies required to compile and test
GPDB [(See Usage)](src/tools/docker/README.md). You can view the dependency dockerfile at `./src/tools/docker/centos6-admin/Dockerfile`.
The image is hosted on docker hub at `pivotaldata/gpdb-dev:centos6-gpadmin`.

A quickstart guide to Docker can be found on the [Pivotal Engineering Journal](http://engineering.pivotal.io/post/docker-gpdb/).

## Development with Vagrant

There is a Vagrant-based [quickstart guide for developers](src/tools/vagrant/README.md).

## Code layout

The directory layout of the repository follows the same general layout
as upstream PostgreSQL. There are changes compared to PostgreSQL
throughout the codebase, but a few larger additions worth noting:

* __gpMgmt/__

  Contains Greenplum-specific command-line tools for managing the
  cluster. Scripts like gpinit, gpstart, gpstop live here. They are
  mostly written in Python.

* __gpAux/__

  Contains Greenplum-specific release management scripts, and vendored
  dependencies. Some additional directories are submodules and will be
  made available over time.

* __gpcontrib/__

  Much like the PostgreSQL contrib/ directory, this directory contains
  extensions such as gpfdist, PXF and gpmapreduce which are Greenplum-specific.

* __doc/__

  In PostgreSQL, the user manual lives here. In Greenplum, the user
  manual is maintained separately and only the reference pages used
  to build man pages are here.

* __gpdb-doc/__

  Contains the Greenplum documentation in DITA XML format. Refer to
  `gpdb-doc/README.md` for information on how to build, and work with
  the documentation.

* __ci/__

  Contains configuration files for the GPDB continuous integration system.

* __src/backend/cdb/__

  Contains larger Greenplum-specific backend modules. For example,
  communication between segments, turning plans into parallelizable
  plans, mirroring, distributed transaction and snapshot management,
  etc. __cdb__ stands for __Cluster Database__ - it was a workname used in
  the early days. That name is no longer used, but the __cdb__ prefix
  remains.

* __src/backend/gpopt/__

  Contains the so-called __translator__ library, for using the GPORCA
  optimizer with Greenplum. The translator library is written in C++
  code, and contains glue code for translating plans and queries
  between the DXL format used by GPORCA, and the PostgreSQL internal
  representation.

* __src/backend/fts/__

  FTS is a process that runs in the master node, and periodically
  polls the segments to maintain the status of each segment.

## Contributing

Greenplum is maintained by a core team of developers with commit rights to the
[main gpdb repository](https://github.com/greenplum-db/gpdb) on GitHub. At the
same time, we are very eager to receive contributions from anybody in the wider
Greenplum community. This section covers all you need to know if you want to see
your code or documentation changes be added to Greenplum and appear in the
future releases.

### Getting started

Greenplum is developed on GitHub, and anybody wishing to contribute to it will
have to [have a GitHub account](https://github.com/signup/free) and be familiar
with [Git tools and workflow](https://wiki.postgresql.org/wiki/Working_with_Git).
It is also recommend that you follow the [developer's mailing list](https://greenplum.org/community/)
since some of the contributions may generate more detailed discussions there.

Once you have your GitHub account, [fork](https://github.com/greenplum-db/gpdb/fork)
this repository so that you can have your private copy to start hacking on and to
use as source of pull requests.

Anybody contributing to Greenplum has to be covered by either the Corporate or
the Individual Contributor License Agreement. If you have not previously done
so, please fill out and submit the [Contributor License Agreement](https://cla.pivotal.io/sign/greenplum).
Note that we do allow for really trivial changes to be contributed without a
CLA if they fall under the rubric of [obvious fixes](https://cla.pivotal.io/about#obvious-fixes).
However, since our GitHub workflow checks for CLA by default you may find it
easier to submit one instead of claiming an "obvious fix" exception.

### Licensing of Greenplum contributions

If the contribution you're submitting is original work, you can assume that Pivotal
will release it as part of an overall Greenplum release available to the downstream
consumers under the Apache License, Version 2.0. However, in addition to that, Pivotal
may also decide to release it under a different license (such as [PostgreSQL License](https://www.postgresql.org/about/licence/) to the upstream consumers that require it. A typical example here would be Pivotal
upstreaming your contribution back to PostgreSQL community (which can be done either
verbatim or your contribution being upstreamed as part of the larger changeset).

If the contribution you're submitting is NOT original work you have to indicate the name
of the license and also make sure that it is similar in terms to the Apache License 2.0.
Apache Software Foundation maintains a list of these licenses under [Category A](https://www.apache.org/legal/resolved.html#category-a). In addition to that, you may be required to make proper attribution in the
[NOTICE file](https://github.com/greenplum-db/gpdb/blob/master/NOTICE) similar to [these examples](https://github.com/greenplum-db/gpdb/blob/master/NOTICE#L278).

Finally, keep in mind that it is NEVER a good idea to remove licensing headers from
the work that is not your original one. Even if you are using parts of the file that
originally had a licensing header at the top you should err on the side of preserving it.
As always, if you are not quite sure about the licensing implications of your contributions,
feel free to reach out to us on the developer mailing list.

### Coding guidelines

Your chances of getting feedback and seeing your code merged into the project
greatly depend on how granular your changes are. If you happen to have a bigger
change in mind, we highly recommend engaging on the developer's mailing list
first and sharing your proposal with us before you spend a lot of time writing
code. Even when your proposal gets validated by the community, we still recommend
doing the actual work as a series of small, self-contained commits. This makes
the reviewer's job much easier and increases the timeliness of feedback.

When it comes to C and C++ parts of Greenplum, we try to follow
[PostgreSQL Coding Conventions](https://www.postgresql.org/docs/devel/source.html).
In addition to that we require that:
   * All Python code passes [Pylint](https://www.pylint.org/)
   * All Go code is formatted according to [gofmt](https://golang.org/cmd/gofmt/)

We recommend using ```git diff --color``` when reviewing your changes so that you
don't have any spurious whitespace issues in the code that you submit.

All new functionality that is contributed to Greenplum should be covered by regression
tests that are contributed alongside it. If you are uncertain on how to test or document
your work, please raise the question on the gpdb-dev mailing list and the developer
community will do its best to help you.

At the very minimum you should always be running
```make installcheck-world```
to make sure that you're not breaking anything.

### Changes applicable to upstream PostgreSQL

If the change you're working on touches functionality that is common between PostgreSQL
and Greenplum, you may be asked to forward-port it to PostgreSQL. This is not only so
that we keep reducing the delta between the two projects, but also so that any change
that is relevant to PostgreSQL can benefit from a much broader review of the upstream
PostgreSQL community. In general, it is a good idea to keep both code bases handy so
you can be sure whether your changes may need to be forward-ported.

### Submission timing

To improve the odds of the right discussion of your patch or idea happening, pay attention
to what the community work cycle is. For example, if you send in a brand new idea in the
beta phase of a release, we may defer review or target its inclusion for a later version.
Feel free to ask on the mailing list to learn more about the Greenplum release policy and timing.

### Patch submission

Once you are ready to share your work with the Greenplum core team and the rest of
the Greenplum community, you should push all the commits to a branch in your own
repository forked from the official Greenplum and
[send us a pull request](https://help.github.com/articles/about-pull-requests/).

We welcome submissions which are work in-progress in order to get feedback early
in the development process.  When opening the pull request, select "Draft" in
the dropdown menu when creating the PR to clearly mark the intent of the pull
request. Prefixing the title with "WIP:" is also good practice.

All new features should be submitted against the main master branch. Bugfixes
should too be submitted against master unless they only exist in a supported
back-branch. If the bug exists in both master and back-branches, explain this
in the PR description.

### Validation checks and CI

Once you submit your pull request, you will immediately see a number of validation
checks performed by our automated CI pipelines. There also will be a CLA check
telling you whether your CLA was recognized. If any of these checks fails, you
will need to update your pull request to take care of the issue. Pull requests
with failed validation checks are very unlikely to receive any further peer
review from the community members.

Keep in mind that the most common reason for a failed CLA check is a mismatch
between an email on file and an email recorded in the commits submitted as
part of the pull request.

If you cannot figure out why a certain validation check failed, feel free to
ask on the developer's mailing list, but make sure to include a direct link
to a pull request in your email.

### Patch review

A submitted pull request with passing validation checks is assumed to be available
for peer review. Peer review is the process that ensures that contributions to Greenplum
are of high quality and align well with the road map and community expectations. Every
member of the Greenplum community is encouraged to review pull requests and provide
feedback. Since you don't have to be a core team member to be able to do that, we
recommend following a stream of pull reviews to anybody who's interested in becoming
a long-term contributor to Greenplum. As [Linus would say](https://en.wikipedia.org/wiki/Linus's_Law)
"given enough eyeballs, all bugs are shallow".

One outcome of the peer review could be a consensus that you need to modify your
pull request in certain ways. GitHub allows you to push additional commits into
a branch from which a pull request was sent. Those additional commits will be then
visible to all of the reviewers.

A peer review converges when it receives at least one +1 and no -1s votes from
the participants. At that point you should expect one of the core team
members to pull your changes into the project.

Greenplum prides itself on being a collaborative, consensus-driven environment.
We do not believe in vetoes and any -1 vote casted as part of the peer review
has to have a detailed technical explanation of what's wrong with the change.
Should a strong disagreement arise it may be advisable to take the matter onto
the mailing list since it allows for a more natural flow of the conversation.

At any time during the patch review, you may experience delays based on the
availability of reviewers and core team members. Please be patient. That being
said, don't get discouraged either. If you're not getting expected feedback for
a few days add a comment asking for updates on the pull request itself or send
an email to the mailing list.

### Direct commits to the repository

On occasion you will see core team members committing directly to the repository
without going through the pull request workflow. This is reserved for small changes
only and the rule of thumb we use is this: if the change touches any functionality
that may result in a test failure, then it has to go through a pull request workflow.
If, on the other hand, the change is in the non-functional part of the code base
(such as fixing a typo inside of a comment block) core team members can decide to
just commit to the repository directly.

## Documentation

For Greenplum Database documentation, please check the
[online documentation](http://docs.greenplum.org/).

For further information beyond the scope of this README, please see
[our wiki](https://github.com/greenplum-db/gpdb/wiki)

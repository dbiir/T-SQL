## Concourse pipeline configuration and scripts

This directory contains pipelines, task files, and scripts to run various sets
of builds and tests on
[our Concourse continuous integration server](http://gpdb.data.pivotal.ci/).

[Learn more about Concourse overall](http://concourse.ci/)

We use Concourse because:

- its builds and test runs are on generic container images, and thus repeatable
  and reproducible if we encounter test failures
- its configuration is fully encoded and repeatable if necessary
- its interface encourages us all to react to test failures

## Concourse Directory Contract

### Directory Structure
The Concourse directory should contain this README and three sub-directories only:

* pipelines
  * templates
* tasks
* scripts

#### Pipelines Directory
There should be just a handful of pipelines in this directory:

* `gpdb_master-generated.yml` the pipeline that compiles, tests, and
  produces installers from the master branch of gpdb. This is a
  generated and should not be edited directly.  The template
  [pipelines/templates/gpdb-tpl.yml](pipelines/templates/gpdb-tpl.yml)
  should be edited and the utility
  [pipelines/gen_pipeline.py](pipelines/gen_pipeline.py) should be
  used to generate production and developer pipeliens. Please review
  the pipelines [README.md](pipelines/README.md) for additional
  information.
* `dev_generate_installer.yml` which compiles and generates an installer for
  the given source and saves it to a dev bucket.
* `pr_pipeline.yml` which compiles and tests pull requests.
* `concourse-upgrade.yml` which uses Concourse to upgrade itself.
* `pipeline_coverity.yml` run a build with coverity flags, separate so that it
  isn't accidentally triggered

#### Tasks Directory
All task yamls should live in this directory.
If a task file is not referenced in the `pipelines` directory it is considered abandoned and can be removed.

There are some exceptions to this rule.
Please do not create any more exceptions, and remove these as the occasion arises:

* `package_tarball.yml` is being used by
  [a gporca pipeline](https://github.com/greenplum-db/gporca/blob/master/concourse/pipeline.yml)
* `test_icg.yml` is being used by
  [a gporca pipeline](https://github.com/greenplum-db/gporca/blob/master/concourse/pipeline.yml)
* `build_with_orca.yml` is being used by
  [a gporca pipeline](https://github.com/greenplum-db/gporca/blob/master/concourse/pipeline.yml)

#### Scripts Directory
All script files should live in this directory.
If a script file is not referenced in any of the directories it is considered abandoned and can be removed.

There are some exceptions to this rule.
Please do not create any more exceptions, and remove these as the occasion arises:

* `package_tarball.bash` is being used by
  [a gporca pipeline](https://github.com/greenplum-db/gporca/blob/master/concourse/pipeline.yml)

### The Concourse Deployment
There is a `gpdb` team in the
[Concourse instance](http://gpdb.data.pivotal.ci/).
Use this team to create any pipelines instead of the `main` team.

### Creating Your Own Pipeline
Many developers want to create their own copies of the master pipeline.

To accommodate this without naming confusion, workload instability,
nor artifact collision, please follow instructions in pipelines
[README.md](pipelines/README.md) for additional information.

#### Notes and warnings

* Clean up your dev pipelines when you are finished with them. (Use `fly destroy-pipeline`)
* Be sure to use a unique name for your pipeline. Don't blow away the master
  pipeline by using `gpdb_master` when setting your dev pipeline. Try to prefix
  your pipeline name with `dev:`
* Please pause the pulse jobs (the rightmost column of builds). If you are
  working on one, that's fine, but pause all of the others. We've had issues
  with too many pulse jobs running at once.

### Updating This README
Changes should be proposed to this contract with a PR.

### Triggering pulse projects
Use `gpdb5-pulse-worker` tag to invoke the job to trigger and monitor Pulse projects 

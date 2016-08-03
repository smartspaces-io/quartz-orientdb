# A OrientDB-based store for Quartz.

This is a OrientDB-backed job store for the [Quartz scheduler](http://quartz-scheduler.org/).


## Usage

Set your Quartz properties to something like this:

    # Use the MongoDB store
    org.quartz.jobStore.classpaces=io.smartspaces.scheduling.quartz.orientdb.OrientDBJobStore
    # MongoDB URI (optional if 'org.quartz.jobStore.addresses' is set)
    org.quartz.jobStore.orientDbUri=PLOCAL:/home/project/database
    # database name
    org.quartz.jobStore.dbName=quartz
    # thread count setting is ignored by the OrientDB store but Quartz requires it (is this true?)
    org.quartz.threadPool.threadCount=1



## Project TODOs

Table prefixes need to be added.

Queries that can be pre-compiled should be precompiled. This will speed up access.

Figure out why can't handle regularly scheduled jobs with a cycle of less than 40 seconds.

The code needs a lot of refactoring and cleanup. 

## Copyright & License

(c) Keith M. Hughes, 2016.

[Apache Public License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
Forked from code from MuleSoft.


## FAQ

### Project Origins

The project was originally started by MuleSoft to support MongoDB. It was then forked by Michael S. Klishin and Alex Petrov to support all Quartz trigger types and tried to be as feature complete as possible for MongoDB. A Quartz JobStore was needed for OrientDB and the basic concepts of OrientDB are close enough to MongoDB to start from MongoDB code.

simpledblayer-mongo
===================


SimpleDbLayer is the MongoDB layer for [simpledblayer](https://github.com/mercmobily/simpledblayer)
It provides an incredibly fast mplementation of joins, by caching results in the records themselves. So, to fetch a person's record, along with her email addresses, addresses, and phone number, it's actually just _one_ DB operation.

The MongoDB layer deals with:

* Update related tables when a main table is changed (deletions, updates, inserts)
* Re-working records marked as "dirty" (if you change the schema for a table, you will need to mark as "dirty" all of the records in related tables, since they will contain outdated information)

If a record is "dirty", the cache will be re-generated on the fly whenever a record is fetched.



## Limitations

Unfortunately, doing a mass-update on a table with children in a 1:n relationship is not allowed because of [MongoDB bug #1243](https://jira.mongodb.org/browse/server-1243), which prevents multuple update in cache from working (the `$` positional operator won't work for sub-arrays).

## Running tests

If you got this package with Git, to run tests make sure you run first:

    git submodule init
    git submodule update 

This will get the list of tests from the main simpledblayer module, making the `test.js` file workable.

If you got this package via NPM, ( Working on it, sorry -- Merc. )


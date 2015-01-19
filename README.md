simpledblayer-mongo
===================


SimpleDbLayer is the MongoDB layer for [simpledblayer](https://github.com/mercmobily/simpledblayer)
It provides an incredibly fast mplementation of joins, by caching results in the records themselves. So, to fetch a person's record, along with her email addresses, addresses, and phone number, it's actually just _one_ read DB operation (!) thanks to the aggressive caching happening (all children data is kept with the record).

The MongoDB layer deals with:

* Update related collections when a collection is changed. This means that each record's cache is always up to date: if you update an email address, the affected contacts' cache is also updated (deletions, updates, inserts)
* Re-work records marked as "dirty". If you change the schema for a collection, you will need to mark as "dirty" all of the records in related tables, since they will contain outdated information)

If a record is "dirty", the cache will be re-generated on the fly whenever a record is fetched.

## Limitations

Unfortunately, doing a mass-update on a table with children in a 1:n relationship is not allowed because of [MongoDB bug #1243](https://jira.mongodb.org/browse/server-1243), which prevents multuple update in cache from working (the `$` positional operator won't work for sub-arrays).


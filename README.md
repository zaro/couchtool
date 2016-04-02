# couchtool

couchtool is a simple backup/restore tool for Apache CouchDB and databases compatible with its API.
The on disk format used is one file per document/attachment. This was chosen so that the backups are easily rsnapshot-able, without making a full copy on each snapshot.

## Installation

    npm install -g couchtool

## Usage

Show database info:

    # Print db size and document count
    couchtool dump http://localhost:5984/some_db

Create backup:

    # Dump all documents in ./some_db/ directory
    couchtool dump http://localhost:5984/some_db

Restore from backup:

    # Read and soreall documents from ./some_db/ directory
    couchtool restore http://localhost:5984/some_db

By default the the docs will be split in 10 directories *docs.0/* to *docs.9/*. If you want to change the number of directories you can use *--buckets* :

    # Split documents in 100 directories
    couchtool --buckets 100 restore http://localhost:5984/some_db

You can use the *buckets* command to find appropriate bucket number for large databases:

    # Show number of documents in each bucket
    couchtool buckets http://localhost:5984/some_db

Also the documents can be stored gziped on disk with --gzip :

    # Gzip documents and attachments on disk
    couchtool --gzip restore http://localhost:5984/some_db

## Source code

[GitHub](https://github.com/zaro/couchtool)

## Todo

- Support couchapps
- Connect to CouchDB over SSH

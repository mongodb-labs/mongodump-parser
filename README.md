# mongodump-parser

This Repository is **NOT** an officially supported MongoDB product.

This simple tool reads a
[mongodump](https://www.mongodb.com/docs/database-tools/mongodump/) archive
on its standard input, parses the archive’s header & collection metadata,
then writes the result as a
[MongoDB Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)
document to standard output.

To build it, just run `go build`.

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

const dumpExtJSON = `
{
  "header": {
    "concurrent_collections": 4,
    "version": "0.1",
    "server_version": "8.0.3-120-gbc35ab4",
    "tool_version": "100.7.1"
  },
  "collectionMetadata": [
    {
      "db": "testDB",
      "collection": "testColl",
      "metadata": {
        "indexes": [
          {
            "v": 2,
            "key": {
              "_id": 1
            },
            "name": "_id_"
          }
        ],
        "uuid": "f4df33f029b34b4fbd5326b5b5c286f3",
        "collectionName": "testColl",
        "type": "collection"
      },
      "size": 0,
      "type": "collection"
    },
    {
      "db": "admin",
      "collection": "system.users",
      "metadata": {
        "indexes": [
          {
            "v": 2,
            "key": {
              "_id": 1
            },
            "name": "_id_"
          },
          {
            "v": 2,
            "key": {
              "user": 1,
              "db": 1
            },
            "name": "user_1_db_1",
            "unique": true
          }
        ],
        "uuid": "ce53ac21899e478fb7e5402dd85bfafb",
        "collectionName": "system.users",
        "type": "collection"
      },
      "size": 0,
      "type": "collection"
    },
    {
      "db": "admin",
      "collection": "system.roles",
      "metadata": {
        "indexes": [
          {
            "v": 2,
            "key": {
              "_id": 1
            },
            "name": "_id_"
          },
          {
            "v": 2,
            "key": {
              "role": 1,
              "db": 1
            },
            "name": "role_1_db_1",
            "unique": true
          }
        ],
        "uuid": "89759f7707b647eea4badf6e74f21a1a",
        "collectionName": "system.roles",
        "type": "collection"
      },
      "size": 0,
      "type": "collection"
    },
    {
      "db": "admin",
      "collection": "system.version",
      "metadata": {
        "indexes": [
          {
            "v": 2,
            "key": {
              "_id": 1
            },
            "name": "_id_"
          }
        ],
        "uuid": "15e5e744f67d4c15bbd4983c4c4a40f5",
        "collectionName": "system.version",
        "type": "collection"
      },
      "size": 0,
      "type": "collection"
    }
  ]
}
`

func TestReport(t *testing.T) {
	expectReport := Report{}
	err := bson.UnmarshalExtJSON(
		[]byte(dumpExtJSON),
		false,
		&expectReport,
	)
	require.NoError(t, err, "should parse test’s ext JSON")

	file, err := os.Open("test.dump")
	require.NoError(t, err, "should open dump file")

	report, err := getReport(file, os.Stderr)
	require.NoError(t, err, "should parse dump")

	assert.Equal(t, expectReport, report, "should get expected report")
}

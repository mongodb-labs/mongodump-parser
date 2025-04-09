package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/mitchellh/go-wordwrap"
	"github.com/mongodb/mongo-tools/common/archive"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/term"
)

const (
	defaultColumnWidth = 80
)

var terminatorBytes = bytes.Repeat([]byte{0xff}, 4)

type Report struct {
	Header             bson.D
	CollectionMetadata []bson.D `bson:"collectionMetadata"`
}

func main() {
	colWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		colWidth = defaultColumnWidth
	}

	var cmd = cli.Command{
		Name:        "mongodump-parser",
		Usage:       "parse mongodump archive files",
		Description: wordwrap.WrapString("This tool reads a mongodump archive file from standard input, parses its header, then outputs the parse to standard output. This lets you see an archive’s contents without actually restoring it.", uint(colWidth-4)),
		Action: func(_ context.Context, cmd *cli.Command) error {
			return run(cmd)
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(cmd *cli.Command) error {
	report, err := getReport(os.Stdin, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "failed to parse archive")
	}

	json, err := bson.MarshalExtJSON(report, false, false)
	if err != nil {
		return errors.Wrap(err, "failed to encode archive report")
	}

	_, err = io.Copy(os.Stdout, bytes.NewBuffer(json))
	if err != nil {
		return errors.Wrap(err, "failed to output report")
	}

	return nil
}

func getReport(input io.Reader, errOut io.Writer) (Report, error) {
	err := checkMagicBytes(input)
	if err != nil {
		return Report{}, errors.Wrap(err, "this does not appear to be a mongodump archive")
	}

	header := bson.D{}
	err = readBSON(input, &header)
	if err != nil {
		return Report{}, errors.Wrap(err, "failed to read archive header")
	}

	bufInput := bufio.NewReader(input)

	mdDocs, err := getCollectionMetadata(bufInput, errOut)
	if err != nil {
		return Report{}, errors.Wrap(err, "failed to read collection metadata")
	}

	// TODO: We could optionally count documents per namespace and
	// extract the CRC, if we want.

	return Report{
		Header:             header,
		CollectionMetadata: mdDocs,
	}, nil
}

func getCollectionMetadata(bufInput *bufio.Reader, errOut io.Writer) ([]bson.D, error) {
	mdDocs := []bson.D{}

	for {
		next4, err := bufInput.Peek(4)
		if err != nil {
			return nil, errors.Wrap(err, "failed to check for end of collection metadata")
		}
		if bytes.Equal(next4, terminatorBytes) {
			break
		}

		mdDoc := bson.D{}
		err = readBSON(bufInput, &mdDoc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read collection metadata document")
		}

		for i := range mdDoc {
			if mdDoc[i].Key != "metadata" {
				continue
			}

			mdStr, ok := mdDoc[i].Value.(string)
			if !ok {
				return nil, errors.Wrapf(err, "expected collection metadata to be %T, not %T (%v)", mdStr, mdDoc[i].Value, mdDoc)
			}

			parsedMetadata := bson.D{}
			err := bson.UnmarshalExtJSON([]byte(mdStr), false, &parsedMetadata)
			if err != nil {
				_, _ = fmt.Fprintf(
					errOut,
					"failed to parse collection metadata string: %v",
					err,
				)
			} else {
				mdDoc[i].Value = parsedMetadata
			}
		}

		mdDocs = append(mdDocs, mdDoc)
	}

	return mdDocs, nil
}

func checkMagicBytes(input io.Reader) error {
	magicBytes := [4]byte{}
	_, err := io.ReadFull(input, magicBytes[:])
	if err != nil {
		return errors.Wrap(err, "failed to read archive magic bytes")
	}

	magicNum := binary.LittleEndian.Uint32(magicBytes[:])
	if magicNum != archive.MagicNumber {
		return fmt.Errorf("unexpected magic number header (%v, %d); should be %d", magicBytes, magicNum, archive.MagicNumber)
	}

	return nil
}

func readBSON[T any](rdr io.Reader, target *T) error {
	raw, err := bson.ReadDocument(rdr)
	if err != nil {
		return errors.Wrap(err, "failed to read BSON document")
	}

	docPtr := new(T)
	err = bson.Unmarshal(raw, docPtr)
	if err != nil {
		return errors.Wrapf(err, "failed to decode BSON document to %T", *docPtr)
	}

	*target = *docPtr

	return nil
}

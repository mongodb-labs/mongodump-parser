// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package bsonutil provides utilities for processing BSON data.
package bsonutil

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/mongodb/mongo-tools/common/json"
	"github.com/mongodb/mongo-tools/common/util"
	errors2 "github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var ErrNoSuchField = errors.New("no such field")

// IsEqual marshals two documents to raw BSON and compares them.
func IsEqual(left, right bson.D) (bool, error) {
	leftBytes, err := bson.Marshal(left)
	if err != nil {
		return false, err
	}

	rightBytes, err := bson.Marshal(right)
	if err != nil {
		return false, err
	}

	return bytes.Compare(leftBytes, rightBytes) == 0, nil
}

// ConvertLegacyExtJSONDocumentToBSON iterates through the document map and converts JSON
// values to their corresponding BSON values. It also replaces any extended JSON
// type value (e.g. $date) with the corresponding BSON type.
func ConvertLegacyExtJSONDocumentToBSON(doc map[string]interface{}) error {
	for key, jsonValue := range doc {
		var bsonValue interface{}
		var err error

		switch v := jsonValue.(type) {
		case map[string]interface{}, bson.D: // subdocument
			bsonValue, err = ParseSpecialKeys(v)
		default:
			bsonValue, err = ConvertLegacyExtJSONValueToBSON(v)
		}
		if err != nil {
			return err
		}

		doc[key] = bsonValue
	}
	return nil
}

// GetExtendedBsonD iterates through the document and returns a bson.D that adds type
// information for each key in document.
func GetExtendedBsonD(doc bson.D) (bson.D, error) {
	var err error
	var bsonDoc bson.D
	for _, docElem := range doc {
		var bsonValue interface{}
		switch v := docElem.Value.(type) {
		case map[string]interface{}, bson.D: // subdocument
			bsonValue, err = ParseSpecialKeys(v)
		default:
			bsonValue, err = ConvertLegacyExtJSONValueToBSON(v)
		}
		if err != nil {
			return nil, err
		}
		bsonDoc = append(bsonDoc, bson.E{
			Key:   docElem.Key,
			Value: bsonValue,
		})
	}
	return bsonDoc, nil
}

// FindValueByKey returns the value of keyName in document. If keyName is not found
// in the top-level of the document, ErrNoSuchField is returned as the error.
func FindValueByKey(keyName string, document *bson.D) (interface{}, error) {
	for _, key := range *document {
		if key.Key == keyName {
			return key.Value, nil
		}
	}
	return nil, ErrNoSuchField
}

// FindStringValueByKey returns the value of keyName in document as a String.
// Returns an error if keyName is not found in the top-level of the document,
// or if it is found but its value is not a string.
func FindStringValueByKey(keyName string, document *bson.D) (string, error) {
	value, err := FindValueByKey(keyName, document)
	if err != nil {
		return "", err
	}
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field present, but is not a string: %v", value)
	}
	return str, nil
}

// FindIntByKey returns the value of keyName in the document as an int for
// either int32 or int64 underlying type.
func FindIntByKey(keyName string, document *bson.D) (int, error) {
	raw, err := FindValueByKey(keyName, document)
	if err != nil {
		return 0, err
	}
	switch x := raw.(type) {
	case int32:
		return int(x), nil
	case int64:
		return int(x), nil
	case int:
		return x, nil
	default:
		return 0, fmt.Errorf("field '%s' is not an integer type", keyName)
	}
}

// FindSubdocumentByKey returns the value of keyName in document as a document.
// Returns an error if keyName is not found in the top-level of the document,
// or if it is found but its value is not a document.
func FindSubdocumentByKey(keyName string, document *bson.D) (bson.D, error) {
	value, err := FindValueByKey(keyName, document)
	if err != nil {
		return bson.D{}, err
	}
	doc, ok := value.(bson.D)
	if !ok {
		return bson.D{}, fmt.Errorf("field '%s' is not a document", keyName)
	}
	return doc, nil
}

// RemoveKey removes the given key. Returns the removed value and true if the
// key was found.
func RemoveKey(key string, document *bson.D) (interface{}, bool) {
	if document == nil {
		return nil, false
	}
	doc := *document
	for i, elem := range doc {
		if elem.Key == key {
			// Remove this key.
			*document = append(doc[:i], doc[i+1:]...)
			return elem.Value, true
		}
	}
	return nil, false
}

// ParseSpecialKeys takes a JSON document and inspects it for any extended JSON
// type (e.g $numberLong) and replaces any such values with the corresponding
// BSON type. (uses legacy extJSON parser).
func ParseSpecialKeys(special interface{}) (interface{}, error) {
	// first ensure we are using a correct document type
	var doc map[string]interface{}
	switch v := special.(type) {
	case bson.D:
		doc = v.Map()
	case map[string]interface{}:
		doc = v
	default:
		return nil, fmt.Errorf("%v (type %T) is not valid input to ParseSpecialKeys", special, special)
	}
	// check document to see if it is special
	switch len(doc) {
	case 1: // document has a single field
		if jsonValue, ok := doc["$date"]; ok {
			switch v := jsonValue.(type) {
			case string:
				return util.FormatDate(v)
			case bson.D:
				asMap := v.Map()
				if jsonValue, ok := asMap["$numberLong"]; ok {
					n, err := parseNumberLongField(jsonValue)
					if err != nil {
						return nil, err
					}
					return time.Unix(n/1e3, n%1e3*1e6), err
				}
				return nil, errors.New("expected $numberLong field in $date")
			case map[string]interface{}:
				if jsonValue, ok := v["$numberLong"]; ok {
					n, err := parseNumberLongField(jsonValue)
					if err != nil {
						return nil, err
					}
					return time.Unix(n/1e3, n%1e3*1e6), err
				}
				return nil, errors.New("expected $numberLong field in $date")

			case json.Number:
				n, err := v.Int64()
				return time.Unix(n/1e3, n%1e3*1e6), err
			case float64:
				n := int64(v)
				return time.Unix(n/1e3, n%1e3*1e6), nil
			case int32:
				n := int64(v)
				return time.Unix(n/1e3, n%1e3*1e6), nil
			case int64:
				return time.Unix(v/1e3, v%1e3*1e6), nil

			case json.ISODate:
				return v, nil

			default:
				return nil, errors.New("invalid type for $date field")
			}
		}

		if jsonValue, ok := doc["$code"]; ok {
			switch v := jsonValue.(type) {
			case string:
				return primitive.JavaScript(v), nil
			default:
				return nil, errors.New("expected $code field to have string value")
			}
		}

		if jsonValue, ok := doc["$oid"]; ok {
			switch v := jsonValue.(type) {
			case string:
				return primitive.ObjectIDFromHex(v)
			default:
				return nil, errors.New("expected $oid field to have string value")
			}
		}

		if jsonValue, ok := doc["$numberLong"]; ok {
			return parseNumberLongField(jsonValue)
		}

		if jsonValue, ok := doc["$numberInt"]; ok {
			switch v := jsonValue.(type) {
			case string:
				// all of decimal, hex, and octal are supported here
				n, err := strconv.ParseInt(v, 0, 32)
				return int32(n), err

			default:
				return nil, errors.New("expected $numberInt field to have string value")
			}
		}

		if jsonValue, ok := doc["$timestamp"]; ok {
			ts := json.Timestamp{}

			var tsDoc map[string]interface{}
			switch internalDoc := jsonValue.(type) {
			case map[string]interface{}:
				tsDoc = internalDoc
			case bson.D:
				tsDoc = internalDoc.Map()
			default:
				return nil, errors.New("expected $timestamp key to have internal document")
			}

			if seconds, ok := tsDoc["t"]; ok {
				if asUint32, err := util.ToUInt32(seconds); err == nil {
					ts.Seconds = asUint32
				} else {
					return nil, errors.New("expected $timestamp 't' field to be a numeric type")
				}
			} else {
				return nil, errors.New("expected $timestamp to have 't' field")
			}
			if inc, ok := tsDoc["i"]; ok {
				if asUint32, err := util.ToUInt32(inc); err == nil {
					ts.Increment = asUint32
				} else {
					return nil, errors.New("expected $timestamp 'i' field to be  a numeric type")
				}
			} else {
				return nil, errors.New("expected $timestamp to have 'i' field")
			}
			// see BSON spec for details on the bit fiddling here
			return primitive.Timestamp{T: ts.Seconds, I: ts.Increment}, nil
		}

		if jsonValue, ok := doc["$numberDecimal"]; ok {
			switch v := jsonValue.(type) {
			case string:
				return primitive.ParseDecimal128(v)
			default:
				return nil, errors.New("expected $numberDecimal field to have string value")
			}
		}

		if _, ok := doc["$undefined"]; ok {
			return primitive.Undefined{}, nil
		}

		if _, ok := doc["$maxKey"]; ok {
			return primitive.MaxKey{}, nil
		}

		if _, ok := doc["$minKey"]; ok {
			return primitive.MinKey{}, nil
		}

	case 2: // document has two fields
		if jsonValue, ok := doc["$code"]; ok {
			code := primitive.CodeWithScope{}
			switch v := jsonValue.(type) {
			case string:
				code.Code = primitive.JavaScript(v)
			default:
				return nil, errors.New("expected $code field to have string value")
			}

			if jsonValue, ok = doc["$scope"]; ok {
				switch v2 := jsonValue.(type) {
				case map[string]interface{}, bson.D:
					x, err := ParseSpecialKeys(v2)
					if err != nil {
						return nil, err
					}
					code.Scope = x
					return code, nil
				default:
					return nil, errors.New("expected $scope field to contain map")
				}
			} else {
				return nil, errors.New("expected $scope field with $code field")
			}
		}

		if jsonValue, ok := doc["$regex"]; ok {
			regex := primitive.Regex{}

			switch pattern := jsonValue.(type) {
			case string:
				regex.Pattern = pattern

			default:
				return nil, errors.New("expected $regex field to have string value")
			}
			if jsonValue, ok = doc["$options"]; !ok {
				return nil, errors.New("expected $options field with $regex field")
			}

			switch options := jsonValue.(type) {
			case string:
				regex.Options = options

			default:
				return nil, errors.New("expected $options field to have string value")
			}

			// Validate regular expression options
			for i := range regex.Options {
				switch o := regex.Options[i]; o {
				default:
					return nil, fmt.Errorf("invalid regular expression option '%v'", o)

				case 'g', 'i', 'm', 's': // allowed
				}
			}
			return regex, nil
		}

		if jsonValue, ok := doc["$binary"]; ok {
			binary := primitive.Binary{}

			switch data := jsonValue.(type) {
			case string:
				bytes, err := base64.StdEncoding.DecodeString(data)
				if err != nil {
					return nil, err
				}
				binary.Data = bytes

			default:
				return nil, errors.New("expected $binary field to have string value")
			}
			if jsonValue, ok = doc["$type"]; !ok {
				return nil, errors.New("expected $type field with $binary field")
			}

			switch typ := jsonValue.(type) {
			case string:
				kind, err := hex.DecodeString(typ)
				if err != nil {
					return nil, err
				} else if len(kind) != 1 {
					return nil, errors.New("expected single byte (as hexadecimal string) for $type field")
				}
				binary.Subtype = kind[0]

			default:
				return nil, errors.New("expected $type field to have string value")
			}
			return binary, nil
		}
	}

	// nothing matched, so we recurse deeper
	switch v := special.(type) {
	case bson.D:
		return GetExtendedBsonD(v)
	case map[string]interface{}:
		return ConvertLegacyExtJSONValueToBSON(v)
	default:
		return nil, fmt.Errorf("%v (type %T) is not valid input to ParseSpecialKeys", special, special)
	}
}

// ParseLegacyExtJSONValue takes any value generated by the json package and returns a
// BSON version of that value.
func ParseLegacyExtJSONValue(jsonValue interface{}) (interface{}, error) {
	switch v := jsonValue.(type) {
	case map[string]interface{}, bson.D: // subdocument
		return ParseSpecialKeys(v)

	default:
		return ConvertLegacyExtJSONValueToBSON(v)
	}
}

func parseNumberLongField(jsonValue interface{}) (int64, error) {
	switch v := jsonValue.(type) {
	case string:
		// all of decimal, hex, and octal are supported here
		return strconv.ParseInt(v, 0, 64)

	default:
		return 0, errors.New("expected $numberLong field to have string value")
	}
}

func Bson2Float64(data interface{}) (float64, bool) {
	switch v := data.(type) {
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case primitive.Decimal128:
		if bi, _, err := v.BigInt(); err == nil {
			intVal := bi.Int64()
			return float64(intVal), true
		}
		return 0, false
	}
	return 0, false
}

// MtoD converts a bson.M to a bson.D.
func MtoD(m bson.M) bson.D {
	doc := make(bson.D, 0, len(m))
	for key, value := range m {
		doc = append(doc, bson.E{key, value})
	}
	return doc
}

// MarshalExtJSONReversible is a wrapper around bson.MarshalExtJSON function,
// but would return an error if it cannot be reversed by bson.UnmarshalExtJSON.
//
// It is preferred to be used in mongodump to avoid generating un-reversible ext JSON.
func MarshalExtJSONReversible(
	val interface{},
	canonical bool,
	escapeHTML bool,
) ([]byte, error) {
	jsonBytes, err := bson.MarshalExtJSON(val, canonical, escapeHTML)
	if err != nil {
		return nil, err
	}

	reversedVal := reflect.New(reflect.TypeOf(val)).Elem().Interface()
	if unmarshalErr := bson.UnmarshalExtJSON(jsonBytes, canonical, &reversedVal); unmarshalErr != nil {
		return nil, errors2.Wrap(unmarshalErr, "marshal is not reversible")
	}

	return jsonBytes, nil
}

// MarshalExtJSONWithBSONRoundtripConsistency is a wrapper around bson.MarshalExtJSON
// which also validates that BSON objects that are marshaled to ExtJSON objects
// return a consistent BSON object when unmarshaled.
func MarshalExtJSONWithBSONRoundtripConsistency(
	val interface{},
	canonical bool,
	escapeHTML bool,
) ([]byte, error) {
	jsonBytes, err := MarshalExtJSONReversible(val, canonical, escapeHTML)
	if err != nil {
		return nil, err
	}

	originalBSON, err := bson.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("could not marshal into BSON")
	}

	reversedVal := reflect.New(reflect.TypeOf(val)).Elem().Interface()
	err = bson.UnmarshalExtJSON(jsonBytes, canonical, &reversedVal)
	if err != nil {
		return nil, err
	}

	reversedBSON, err := bson.Marshal(reversedVal)
	if err != nil {
		return nil, fmt.Errorf("could not marshal into BSON")
	}

	if !bytes.Equal(originalBSON, reversedBSON) {
		return nil, fmt.Errorf(
			"marshaling BSON to ExtJSON and back resulted in discrepancies",
		)
	}

	return jsonBytes, nil
}

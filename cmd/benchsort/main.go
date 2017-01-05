// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

type comparableRow struct {
	key    []types.Datum
	val    []types.Datum
	handle int64
}

var (
	genCmd = flag.NewFlagSet("gen", flag.ExitOnError)
	// runCmd  = flag.NewFlagSet("gen", flag.ExitOnError)
	// helpCmd = flag.NewFlagSet("help", flag.ExitOnError)

	logLevel = "warn"
	tmpDir   string
	keySize  int
	valSize  int
	scale    int
)

func init() {
	log.SetLevelByString(logLevel)

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	genCmd.StringVar(&tmpDir, "dir", cwd, "where to store the generated rows")
	genCmd.IntVar(&keySize, "keySize", 8, "the size of key")
	genCmd.IntVar(&valSize, "valSize", 8, "the size of vlaue")
	genCmd.IntVar(&scale, "scale", 100, "how many rows to generate")
}

func nextRow(r *rand.Rand, keySize int, valSize int) *comparableRow {
	key := make([]types.Datum, keySize)
	for i := range key {
		key[i] = types.NewDatum(r.Int())
	}

	val := make([]types.Datum, valSize)
	for j := range val {
		val[j] = types.NewDatum(r.Int())
	}

	handle := r.Int63()
	return &comparableRow{key: key, val: val, handle: handle}
}

func encodeRow(b []byte, row *comparableRow) ([]byte, error) {
	var (
		err  error
		head = make([]byte, 8)
		body []byte
	)

	body, err = codec.EncodeKey(body, row.key...)
	if err != nil {
		return b, errors.Trace(err)
	}
	body, err = codec.EncodeKey(body, row.val...)
	if err != nil {
		return b, errors.Trace(err)
	}
	body, err = codec.EncodeKey(body, types.NewIntDatum(row.handle))
	if err != nil {
		return b, errors.Trace(err)
	}

	binary.BigEndian.PutUint64(head, uint64(len(body)))

	b = append(b, head...)
	b = append(b, body...)

	return b, nil
}

func export() error {
	var (
		err        error
		rowBytes   []byte
		outputFile *os.File
	)

	_, err = os.Stat(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("tmpDir does not exist")
		}
		return errors.Trace(err)
	}

	fileName := path.Join(tmpDir, "data.out")
	_, err = os.Stat(fileName)
	if err == nil {
		return errors.New("data file (data.out) exists")
	}

	outputFile, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	defer outputFile.Close()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	for i := 1; i <= scale; i++ {
		rowBytes, err = encodeRow(rowBytes, nextRow(r, keySize, valSize))
		if err != nil {
			return errors.Trace(err)
		}
	}

	_, err = outputFile.Write(rowBytes)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func main() {
	flag.Parse()

	if len(os.Args) == 1 {
		fmt.Println("Usage:\n")
		fmt.Println("\tbenchsort command [arguments]\n")
		fmt.Println("The commands are:\n")
		fmt.Println("\tgen\t", "generate rows")
		fmt.Println("\trun\t", "run tests")
		fmt.Println("")
		fmt.Println("Use \"benchsort help [command]\" for more information about a command.")
		return
	}

	switch os.Args[1] {
	case "gen":
		genCmd.Parse(os.Args[2:])
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}

	if genCmd.Parsed() {
		// Sanity checks
		if keySize <= 0 {
			log.Fatal(errors.New("key size must be positive"))
		}
		if valSize <= 0 {
			log.Fatal(errors.New("value size must be positive"))
		}
		if scale <= 0 {
			log.Fatal(errors.New("scale must be positive"))
		}

		cLog("Generating...")
		start := time.Now()
		if err := export(); err != nil {
			log.Fatal(err)
		}
		cLog("Done!")
		cLogf("Data placed in: %s", path.Join(tmpDir, "data.out"))
		cLog("Time used: ", time.Since(start))
	}
}

func cLogf(format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	fmt.Println("\033[0;32m" + str + "\033[0m")
}

func cLog(args ...interface{}) {
	str := fmt.Sprint(args...)
	fmt.Println("\033[0;32m" + str + "\033[0m")
}

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
	runCmd = flag.NewFlagSet("gen", flag.ExitOnError)
	// helpCmd = flag.NewFlagSet("help", flag.ExitOnError)

	logLevel    = "warn"
	tmpDir      string
	keySize     int
	valSize     int
	bufSize     int
	scale       int
	inputRatio  int
	outputRatio int
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

	runCmd.StringVar(&tmpDir, "dir", cwd, "where to load the generated rows")
	runCmd.IntVar(&bufSize, "bufSize", 500000, "how many rows held in memory at a time")
	runCmd.IntVar(&inputRatio, "inputRatio", 100, "input percentage")
	runCmd.IntVar(&outputRatio, "outputRatio", 100, "output percentage")
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
		err         error
		outputBytes []byte
		outputFile  *os.File
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

	meta := make([]byte, 8)

	binary.BigEndian.PutUint64(meta, uint64(scale))
	outputBytes = append(outputBytes, meta...)
	binary.BigEndian.PutUint64(meta, uint64(keySize))
	outputBytes = append(outputBytes, meta...)
	binary.BigEndian.PutUint64(meta, uint64(valSize))
	outputBytes = append(outputBytes, meta...)

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	for i := 1; i <= scale; i++ {
		outputBytes, err = encodeRow(outputBytes, nextRow(r, keySize, valSize))
		if err != nil {
			return errors.Trace(err)
		}
	}

	if _, err := outputFile.Write(outputBytes); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func load() ([]*comparableRow, error) {
	var (
		err  error
		fd   *os.File
		meta = make([]byte, 8)
	)

	_, err = os.Stat(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("tmpDir does not exist")
		}
		return nil, errors.Trace(err)
	}

	fileName := path.Join(tmpDir, "data.out")
	fd, err = os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("data file (data.out) does not exist")
		}
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	if n, err := fd.Read(meta); err != nil || n != 8 {
		if n != 8 {
			return nil, errors.New("incorrect meta data")
		}
		return nil, errors.Trace(err)
	}

	scale = int(binary.BigEndian.Uint64(meta))
	if scale <= 0 {
		return nil, errors.New("number of rows should be positive")
	}

	if n, err := fd.Read(meta); err != nil || n != 8 {
		if n != 8 {
			return nil, errors.New("incorrect meta data")
		}
		return nil, errors.Trace(err)
	}

	keySize = int(binary.BigEndian.Uint64(meta))
	if keySize <= 0 {
		return nil, errors.New("key size should be positive")
	}

	if n, err := fd.Read(meta); err != nil || n != 8 {
		if n != 8 {
			return nil, errors.New("incorrect meta data")
		}
		return nil, errors.Trace(err)
	}
	valSize = int(binary.BigEndian.Uint64(meta))
	if valSize <= 0 {
		return nil, errors.New("value size should be positive")
	}

	cLogf("\tnumber of rows = %d, key size = %d, value size = %d", scale, keySize, valSize)

	var (
		head = make([]byte, 8)
		dcod = make([]types.Datum, 0, keySize+valSize+1)
		data = make([]*comparableRow, 0, scale)
	)

	for i := 1; i <= scale; i++ {
		if n, err := fd.Read(head); err != nil || n != 8 {
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.New("incorrect header")
		}

		rowSize := int(binary.BigEndian.Uint64(head))
		rowBytes := make([]byte, rowSize)

		if n, err := fd.Read(rowBytes); err != nil || n != rowSize {
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.New("incorrect row")
		}

		dcod, err = codec.Decode(rowBytes, keySize+valSize+1)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var newRow = &comparableRow{
			key:    dcod[:keySize],
			val:    dcod[keySize : keySize+valSize],
			handle: dcod[keySize+valSize:][0].GetInt64(),
		}
		data = append(data, newRow)
	}

	return data, nil
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
	case "run":
		runCmd.Parse(os.Args[2:])
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

	if runCmd.Parsed() {
		// Sanity checks

		var (
			err error
			// data []*comparableRow
		)
		cLog("Loading...")
		start := time.Now()
		if _, err = load(); err != nil {
			log.Fatal(err)
		}
		cLog("Done!")
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

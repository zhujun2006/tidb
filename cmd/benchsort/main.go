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
	"flag"
	"fmt"
	"os"

	"github.com/ngaut/log"
)

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

	var cwd, err = os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	genCmd.StringVar(&tmpDir, "dir", cwd, "where to store the generated rows")
	genCmd.IntVar(&keySize, "keySize", 8, "the size of key")
	genCmd.IntVar(&valSize, "valSize", 8, "the size of vlaue")
	genCmd.IntVar(&scale, "scale", 100, "how many rows to generate")
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
		cLogf("dir = %s", tmpDir)
		cLogf("keySize = %d", keySize)
		cLogf("valSize = %d", valSize)
		cLogf("scale = %d", scale)
	}
}

func cLogf(format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}

func cLog(args ...interface{}) {
	str := fmt.Sprint(args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}

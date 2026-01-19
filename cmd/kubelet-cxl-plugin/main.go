/*
 * Copyright (c) 2025, Intel Corporation.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	cxl "github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"
)

type CXLFlags struct {
	MyFlag1 string
	MyFlag2 string
}

func main() {
	cxlFlags := CXLFlags{
		MyFlag1: cxl.DefaultMyFlag1,
		MyFlag2: cxl.DefaultMyFlag2,
	}
	cliFlags := []cli.Flag{
		&cli.StringFlag{
			Name:        "myflag1",
			Aliases:     []string{"p"},
			Usage:       "myflag1 short desc",
			Value:       cxl.DefaultMyFlag1,
			Destination: &cxlFlags.MyFlag1,
			EnvVars:     []string{cxl.MyFlag1EnvVarName},
		},
		&cli.StringFlag{
			Name:        "myflag2",
			Aliases:     []string{"n"},
			Usage:       "myflag2 short desc",
			Value:       cxl.DefaultMyFlag2,
			Destination: &cxlFlags.MyFlag2,
			EnvVars:     []string{cxl.MyFlag2EnvVarName},
		},
	}

	if err := helpers.NewApp(cxl.DriverName, newDriver, cliFlags, &cxlFlags).Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

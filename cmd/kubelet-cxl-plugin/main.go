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
	SysfsRoot string
	ConfigStr string
}

func main() {
	cxlFlags := CXLFlags{
		SysfsRoot: cxl.DefaultSysfsRoot,
		ConfigStr: cxl.DefaultConfigStr,
	}
	cliFlags := []cli.Flag{
		&cli.StringFlag{
			Name:        "sysfs",
			Aliases:     []string{},
			Usage:       "",
			Value:       cxl.DefaultSysfsRoot,
			Destination: &cxlFlags.SysfsRoot,
			EnvVars:     []string{cxl.SysfsRootEnvVarName},
		},
		&cli.StringFlag{
			Name:        "config-str",
			Aliases:     []string{"c"},
			Usage:       "driver configuration as a string",
			Value:       cxl.DefaultConfigStr,
			Destination: &cxlFlags.ConfigStr,
			EnvVars:     []string{cxl.ConfigStrEnvVarName},
		},
	}

	if err := helpers.NewApp(cxl.DriverName, newDriver, cliFlags, &cxlFlags).Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

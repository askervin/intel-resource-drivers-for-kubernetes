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
	SysfsRoot  string
	ConfigStr  string
	ConfigFile string
	NRIName    string
	NRIIdx     string
	NRISocket  string
}

func main() {
	cxlFlags := CXLFlags{
		SysfsRoot: cxl.DefaultSysfsRoot,
		ConfigStr: cxl.DefaultConfigStr,
		NRIName:   "kubelet-cxl-plugin",
		NRIIdx:    "30",
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
			Usage:       "driver configuration as a JSON or YAML string",
			Value:       cxl.DefaultConfigStr,
			Destination: &cxlFlags.ConfigStr,
			EnvVars:     []string{cxl.ConfigStrEnvVarName},
		},
		&cli.StringFlag{
			Name:        "config-file",
			Aliases:     []string{"f"},
			Usage:       "path to a JSON or YAML configuration file (mutually exclusive with -c)",
			Destination: &cxlFlags.ConfigFile,
		},
		&cli.StringFlag{
			Name:        "nri-name",
			Usage:       "NRI plugin name to register",
			Value:       cxlFlags.NRIName,
			Destination: &cxlFlags.NRIName,
			EnvVars:     []string{"NRI_PLUGIN_NAME"},
		},
		&cli.StringFlag{
			Name:        "nri-idx",
			Usage:       "NRI plugin index (place in the plugin chain, higher is later)",
			Value:       cxlFlags.NRIIdx, // TODO: before or after resource mgmt plugins?
			Destination: &cxlFlags.NRIIdx,
			EnvVars:     []string{"NRI_PLUGIN_IDX"},
		},
		&cli.StringFlag{
			Name:        "nri-socket",
			Usage:       "NRI socket path (empty for default)",
			Value:       cxlFlags.NRISocket,
			Destination: &cxlFlags.NRISocket,
			EnvVars:     []string{"NRI_SOCKET"},
		},
	}

	if err := helpers.NewApp(cxl.DriverName, newDriver, cliFlags, &cxlFlags).Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

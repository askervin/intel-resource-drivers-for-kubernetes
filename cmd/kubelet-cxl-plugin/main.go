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
	"time"

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

	// PoolDriverName is the DRA driver name of the pool helper,
	// empty disables it.
	PoolDriverName string
	// PoolPrepareTimeout is how long the pool helper waits for a
	// memdev to appear.
	PoolPrepareTimeout time.Duration

	// PoolLend enables lending pool memory to running containers: the
	// token device "dynamic", lease sockets and companion pods.
	PoolLend bool
	// CXLRequestBin is the host path of the static cxl-request CLI
	// that is bind-mounted into containers with a token claim.
	CXLRequestBin string
	// LeaseWaitDefault is how long a lease request waits for Ready
	// when the request does not say.
	LeaseWaitDefault time.Duration
	// CompanionImage is the image of the companion pods.
	CompanionImage string
}

func main() {
	cxlFlags := CXLFlags{
		SysfsRoot: cxl.DefaultSysfsRoot,
		ConfigStr: cxl.DefaultConfigStr,
		NRIName:   "kubelet-cxl-plugin",
		NRIIdx:    "30",

		PoolDriverName:     cxl.PoolDriverName,
		PoolPrepareTimeout: defaultPoolPrepareTimeout,

		PoolLend:         true,
		CXLRequestBin:    defaultCXLRequestBin,
		LeaseWaitDefault: defaultLeaseWait,
		CompanionImage:   defaultCompanionImage,
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
		&cli.StringFlag{
			Name:        "pool-driver-name",
			Usage:       "DRA driver name of the CXL memory pool helper (empty disables it)",
			Value:       cxlFlags.PoolDriverName,
			Destination: &cxlFlags.PoolDriverName,
			EnvVars:     []string{"POOL_DRIVER_NAME"},
		},
		&cli.DurationFlag{
			Name:        "pool-prepare-timeout",
			Usage:       "how long the pool helper waits for a hotplugged CXL memory device to appear",
			Value:       cxlFlags.PoolPrepareTimeout,
			Destination: &cxlFlags.PoolPrepareTimeout,
			EnvVars:     []string{"POOL_PREPARE_TIMEOUT"},
		},
		&cli.BoolFlag{
			Name:        "pool-lend",
			Usage:       "lend pool memory to running containers that claim the token device \"dynamic\" (needs the pool helper and NRI)",
			Value:       cxlFlags.PoolLend,
			Destination: &cxlFlags.PoolLend,
			EnvVars:     []string{"POOL_LEND"},
		},
		&cli.StringFlag{
			Name:        "cxl-request-bin",
			Usage:       "host path of the static cxl-request CLI, bind-mounted into containers with a token claim",
			Value:       cxlFlags.CXLRequestBin,
			Destination: &cxlFlags.CXLRequestBin,
			EnvVars:     []string{"CXL_REQUEST_BIN"},
		},
		&cli.DurationFlag{
			Name:        "lease-wait-default",
			Usage:       "how long a lease request waits for the lease to become Ready when the request does not say",
			Value:       cxlFlags.LeaseWaitDefault,
			Destination: &cxlFlags.LeaseWaitDefault,
			EnvVars:     []string{"LEASE_WAIT_DEFAULT"},
		},
		&cli.StringFlag{
			Name:        "companion-image",
			Usage:       "container image of the companion pods that hold leased pool devices",
			Value:       cxlFlags.CompanionImage,
			Destination: &cxlFlags.CompanionImage,
			EnvVars:     []string{"COMPANION_IMAGE"},
		},
	}

	if err := helpers.NewApp(cxl.DriverName, newDriver, cliFlags, &cxlFlags).Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

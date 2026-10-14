/*
 * Copyright (c) 2026, Intel Corporation.  All Rights Reserved.
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

// Command cxl-request borrows CXL memory, of the node's local memdevs or
// of a memory pool, for the running container from kubelet-cxl-plugin
// through the lease socket $CXL_LEASE_SOCKET, which the plugin mounts
// into containers that claim the token device "dynamic" (DeviceClass
// cxl-dynamic). See doc/cxl/POOL.md and doc/cxl/LOCAL.md.
//
//	cxl-request memory <size> [SOURCE] [--serial S] [--wait D] [--request-id ID]
//	cxl-request memory <size> [SOURCE] [--serial S] --dax [--read-only] [--wait D] [--exec CMD ARGS...]
//	cxl-request shared <serial> [--exclusive] [--read-only] [--wait D] [--exec CMD ARGS...]
//	cxl-request fd <lease> --exec CMD ARGS...
//	cxl-request release <lease>|--all [--wait D]    (or: cxl-release ...)
//	cxl-request list
//
// SOURCE is --source local|pool|any or --local (= --source local): memory
// of a local memdev, of the pool (the default), or local if a local
// memdev fits, else pool. --serial selects the device by its serial.
// A memory lease is lent as a NUMA node, or with --dax passed as a devdax
// fd; a shared lease is a pool device passed as a devdax fd. --exclusive
// takes all of a shared device's "hosts" capacity, so that no other claim
// can have the device while the lease lives. --read-only opens the fd
// read-only.
//
// It prints the plugin's JSON response line. With --exec the dax device
// fd of a dax lease is passed to CMD as fd 3, with CXL_LEASE_FD=3,
// CXL_LEASE, CXL_LEASE_DAX, CXL_LEASE_SIZE and CXL_LEASE_READONLY (1 or
// 0) in its environment.
// Exit codes: 0 ok, 1 error, 2 usage, 3 lease failed or not Ready in time.
// Without --exec the command's exit code is passed through.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	exitOK      = 0
	exitError   = 1
	exitUsage   = 2
	exitNotUsed = 3 // failed, timeout, not Ready
	socketEnv   = "CXL_LEASE_SOCKET"
	leaseFd     = 3
)

// request and response mirror the plugin's protocol (broker.go).
type request struct {
	Op        string `json:"op"`
	Kind      string `json:"kind,omitempty"`
	Size      string `json:"size,omitempty"`
	Serial    string `json:"serial,omitempty"`
	Source    string `json:"source,omitempty"`
	Wait      string `json:"wait,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	Lease     string `json:"lease,omitempty"`
	All       bool   `json:"all,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Exclusive bool   `json:"exclusive,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type response struct {
	Lease     string      `json:"lease,omitempty"`
	State     string      `json:"state,omitempty"`
	Kind      string      `json:"kind,omitempty"`
	Source    string      `json:"source,omitempty"`
	Mode      string      `json:"mode,omitempty"`
	Exclusive bool        `json:"exclusive,omitempty"`
	ReadOnly  bool        `json:"readOnly,omitempty"`
	Node      *int        `json:"node,omitempty"`
	Size      uint64      `json:"size,omitempty"`
	Serial    string      `json:"serial,omitempty"`
	Dax       string      `json:"dax,omitempty"`
	Error     string      `json:"error,omitempty"`
	Leases    *[]response `json:"leases,omitempty"`
}

const usage = `usage:
  cxl-request memory <size> [SOURCE] [--serial SERIAL] [--wait DURATION] [--request-id ID]
  cxl-request memory <size> [SOURCE] [--serial SERIAL] --dax [--read-only] [--wait DURATION] [--exec CMD ARGS...]
  cxl-request shared <serial> [--exclusive] [--read-only] [--wait DURATION] [--exec CMD ARGS...]
  cxl-request fd <lease> --exec CMD ARGS...
  cxl-request release <lease>|--all [--wait DURATION]   (also: cxl-release ...)
  cxl-request list
The socket is $CXL_LEASE_SOCKET. --wait 0s returns at once.
memory: exclusive memory, lent as a NUMA node, or with --dax passed as a
devdax fd. SOURCE: --source pool (the default), --source local or --local
(a memdev of this node), --source any (local if one fits, else pool).
--serial selects the device. shared: a shared pool device, passed as a
devdax fd; --exclusive keeps everybody else off it while the lease lives.
--read-only: the fd is opened read-only.
`

func main() {
	os.Exit(run(os.Args, os.Stdout, os.Stderr))
}

// command is a parsed command line.
type command struct {
	req    request
	exec   []string
	source string // --source
	local  bool   // --local
	serial string // --serial
}

func usageErr(stderr io.Writer, format string, args ...any) int {
	fmt.Fprintf(stderr, "cxl-request: "+format+"\n", args...)
	fmt.Fprint(stderr, usage)
	return exitUsage
}

// parseArgs parses the arguments after the program name. An argv[0] of
// cxl-release means "release".
func parseArgs(prog string, args []string) (*command, error) {
	if strings.HasPrefix(filepath.Base(prog), "cxl-release") {
		args = append([]string{"release"}, args...)
	}
	if len(args) == 0 {
		return nil, errors.New("no command")
	}
	cmd, pos, err := parseOptions(args[1:])
	if err != nil {
		return nil, err
	}
	if err := setCommand(cmd, args[0], pos); err != nil {
		return nil, err
	}
	if err := checkCommand(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

// parseOptions returns a command with the options of args, and the
// positional arguments. --exec ends the options: the rest is the command
// to run.
func parseOptions(args []string) (*command, []string, error) {
	cmd := &command{}
	values := map[string]*string{"--wait": &cmd.req.Wait, "--request-id": &cmd.req.RequestID, "--source": &cmd.source, "--serial": &cmd.serial}
	flags := map[string]*bool{"--all": &cmd.req.All, "--exclusive": &cmd.req.Exclusive, "--read-only": &cmd.req.ReadOnly, "--local": &cmd.local}
	var pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		name, val, inline := strings.Cut(a, "=")
		dst, isValue := values[name]
		switch {
		case a == "--exec":
			cmd.exec = args[i+1:]
			if len(cmd.exec) == 0 {
				return nil, nil, errors.New("--exec needs a command")
			}
			return cmd, pos, nil
		case isValue && inline:
			*dst = val
		case isValue:
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("%s needs a value", name)
			}
			i++
			*dst = args[i]
		case flags[a] != nil:
			*flags[a] = true
		case a == "--dax":
			cmd.req.Mode = "dax"
		case strings.HasPrefix(a, "-"):
			return nil, nil, fmt.Errorf("unknown option %s", a)
		default:
			pos = append(pos, a)
		}
	}
	return cmd, pos, nil
}

// setCommand sets the request of the command name with its positional
// arguments.
func setCommand(cmd *command, name string, pos []string) error {
	one := func(what string, dst *string) error {
		if len(pos) != 1 {
			return fmt.Errorf("%s: expected one %s", name, what)
		}
		*dst = pos[0]
		return nil
	}
	switch name {
	case "memory":
		cmd.req.Op, cmd.req.Kind = "lease", "memory"
		return one("size", &cmd.req.Size)
	case "shared":
		cmd.req.Op, cmd.req.Kind = "lease", "shared"
		return one("serial", &cmd.req.Serial)
	case "fd":
		cmd.req.Op = "fd"
		return one("lease", &cmd.req.Lease)
	case "release":
		cmd.req.Op = "release"
		switch {
		case cmd.req.All && len(pos) == 0:
		case !cmd.req.All && len(pos) == 1:
			cmd.req.Lease = pos[0]
		default:
			return errors.New("release: expected one lease or --all")
		}
	case "list":
		cmd.req.Op = "list"
		if len(pos) != 0 {
			return errors.New("list takes no arguments")
		}
	default:
		return fmt.Errorf("unknown command %q", name)
	}
	return nil
}

// checkCommand returns an error if the options do not fit the command,
// and sets the source and serial of a memory request.
func checkCommand(cmd *command) error {
	dax := cmd.req.Kind == "shared" || cmd.req.Mode == "dax"
	if err := checkSource(cmd); err != nil {
		return err
	}
	switch {
	case cmd.req.Mode != "" && cmd.req.Kind != "memory":
		return errors.New("--dax is for memory (shared leases are always dax)")
	case cmd.req.Exclusive && cmd.req.Kind != "shared":
		return errors.New("--exclusive is for shared (memory leases are exclusive)")
	case cmd.req.ReadOnly && !dax:
		return errors.New("--read-only is for shared and memory --dax")
	}
	return checkExec(cmd, dax)
}

// checkSource returns an error if the source options do not fit the
// command, and sets them on a memory request.
func checkSource(cmd *command) error {
	source := cmd.source
	switch {
	case cmd.local && source != "" && source != "local":
		return fmt.Errorf("--local conflicts with --source %s", source)
	case cmd.local:
		source = "local"
	}
	switch {
	case source != "" && cmd.req.Kind != "memory":
		return errors.New("--source and --local are for memory (shared leases are pool leases)")
	case cmd.serial != "" && cmd.req.Kind != "memory":
		return errors.New("--serial is for memory (shared takes the serial as its argument)")
	case source != "" && source != "local" && source != "pool" && source != "any":
		return fmt.Errorf("invalid --source %q (local, pool or any)", source)
	}
	cmd.req.Source = source
	if cmd.serial != "" {
		cmd.req.Serial = cmd.serial
	}
	return nil
}

// checkExec returns an error if --exec does not fit the command: it needs
// an fd, which only a dax lease and "fd" give.
func checkExec(cmd *command, dax bool) error {
	switch {
	case cmd.exec == nil && cmd.req.Op == "fd":
		return errors.New("fd needs --exec")
	case cmd.exec == nil:
		return nil
	case cmd.req.Op == "lease" && !dax:
		return errors.New("--exec is for shared and memory --dax")
	case cmd.req.Op != "lease" && cmd.req.Op != "fd":
		return errors.New("--exec is for shared, memory --dax and fd")
	}
	return nil
}

func run(argv []string, stdout, stderr io.Writer) int {
	cmd, err := parseArgs(argv[0], argv[1:])
	if err != nil {
		return usageErr(stderr, "%v", err)
	}
	sock := os.Getenv(socketEnv)
	if sock == "" {
		fmt.Fprintf(stderr, "cxl-request: %s is not set (the pod needs a claim on DeviceClass cxl-dynamic)\n", socketEnv)
		return exitError
	}
	line, fd, err := roundTrip(sock, &cmd.req)
	if err != nil {
		fmt.Fprintf(stderr, "cxl-request: %v\n", err)
		return exitError
	}
	var resp response
	if err := json.Unmarshal(line, &resp); err != nil {
		closeFd(fd)
		fmt.Fprintf(stderr, "cxl-request: invalid response %q: %v\n", line, err)
		return exitError
	}
	if cmd.exec == nil {
		closeFd(fd)
		fmt.Fprintf(stdout, "%s\n", bytes.TrimSpace(line))
		return exitCode(&cmd.req, &resp)
	}
	if fd < 0 {
		fmt.Fprintf(stderr, "cxl-request: %s\n", bytes.TrimSpace(line))
		if code := exitCode(&cmd.req, &resp); code != exitOK {
			return code
		}
		return exitNotUsed
	}
	return execWithFd(cmd.exec, fd, &resp, stdout, stderr)
}

// exitCode maps a response to the exit code.
func exitCode(req *request, resp *response) int {
	if resp.State == "Failed" {
		return exitNotUsed
	}
	if resp.Error != "" {
		return exitError
	}
	switch req.Op {
	case "lease":
		if resp.State == "Ready" || req.Wait == "0" || req.Wait == "0s" {
			return exitOK
		}
		return exitNotUsed
	case "release":
		if resp.Leases != nil {
			for _, l := range *resp.Leases {
				if l.State != "Released" {
					return exitNotUsed
				}
			}
			return exitOK
		}
		if resp.State != "Released" {
			return exitNotUsed
		}
	}
	return exitOK
}

// roundTrip sends the request and reads one response line with the fd
// that came with it (-1 for none).
func roundTrip(sock string, req *request) ([]byte, int, error) {
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return nil, -1, err
	}
	defer c.Close()
	data, err := json.Marshal(req)
	if err != nil {
		return nil, -1, err
	}
	if _, err := c.Write(append(data, '\n')); err != nil {
		return nil, -1, err
	}
	return readResponse(c)
}

// readResponse reads a line; the fd arrives with its first bytes.
func readResponse(c *net.UnixConn) ([]byte, int, error) {
	fd := -1
	buf := make([]byte, 64*1024)
	oob := make([]byte, syscall.CmsgSpace(4*4))
	n, oobn, _, _, err := c.ReadMsgUnix(buf, oob)
	if err != nil && n == 0 {
		return nil, -1, fmt.Errorf("reading response: %w", err)
	}
	if oobn > 0 {
		fd, err = parseFd(oob[:oobn])
		if err != nil {
			return nil, -1, err
		}
	}
	line := buf[:n]
	if !bytes.Contains(line, []byte{'\n'}) {
		rest, err := bufio.NewReader(c).ReadBytes('\n')
		line = append(line, rest...)
		if err != nil && !bytes.Contains(line, []byte{'\n'}) {
			closeFd(fd)
			return nil, -1, fmt.Errorf("reading response: %w", err)
		}
	}
	if i := bytes.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	return line, fd, nil
}

// parseFd returns the first fd of SCM_RIGHTS messages and closes others.
func parseFd(oob []byte) (int, error) {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return -1, fmt.Errorf("control message: %w", err)
	}
	fd := -1
	for i := range msgs {
		fds, err := syscall.ParseUnixRights(&msgs[i])
		if err != nil {
			continue
		}
		for _, f := range fds {
			if fd < 0 {
				fd = f
			} else {
				_ = syscall.Close(f)
			}
		}
	}
	return fd, nil
}

func closeFd(fd int) {
	if fd >= 0 {
		_ = syscall.Close(fd)
	}
}

// execWithFd runs the command with the fd as fd 3 and the lease in its
// environment, forwards signals, and returns its exit code. (A fork
// rather than an in-place exec: dup2 onto fd 3 could clobber a file the
// Go runtime holds there.)
func execWithFd(argv []string, fd int, resp *response, stdout, stderr io.Writer) int {
	f := os.NewFile(uintptr(fd), resp.Dax)
	defer f.Close()
	c := exec.Command(argv[0], argv[1:]...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, stdout, stderr
	c.ExtraFiles = []*os.File{f} // fd 3
	c.Env = append(os.Environ(),
		"CXL_LEASE_FD="+strconv.Itoa(leaseFd),
		"CXL_LEASE="+resp.Lease,
		"CXL_LEASE_DAX="+resp.Dax,
		"CXL_LEASE_SIZE="+strconv.FormatUint(resp.Size, 10),
		"CXL_LEASE_READONLY="+map[bool]string{false: "0", true: "1"}[resp.ReadOnly],
	)
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(sigs)
	if err := c.Start(); err != nil {
		fmt.Fprintf(stderr, "cxl-request: %v\n", err)
		return exitError
	}
	_ = f.Close() // the child has it
	go func() {
		for s := range sigs {
			_ = c.Process.Signal(s)
		}
	}()
	err := c.Wait()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return exitOK
	case errors.As(err, &ee):
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return 128 + int(ws.Signal())
		}
		return ee.ExitCode()
	default:
		fmt.Fprintf(stderr, "cxl-request: %v\n", err)
		return exitError
	}
}

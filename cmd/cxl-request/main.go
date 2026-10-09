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

// Command cxl-request borrows CXL pool memory for the running container
// from kubelet-cxl-plugin through the lease socket $CXL_LEASE_SOCKET,
// which the plugin mounts into containers that claim the token device
// "dynamic" (DeviceClass cxl-dynamic). See doc/cxl/POOL.md.
//
//	cxl-request memory <size> [--wait D]
//	cxl-request shared <serial> [--wait D] [--exec CMD ARGS...]
//	cxl-request fd <lease> --exec CMD ARGS...
//	cxl-request release <lease>|--all [--wait D]    (or: cxl-release ...)
//	cxl-request list
//
// It prints the plugin's JSON response line. With --exec the dax device
// fd of a shared lease is passed to CMD as fd 3, with CXL_LEASE_FD=3,
// CXL_LEASE, CXL_LEASE_DAX and CXL_LEASE_SIZE in its environment.
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
	Wait      string `json:"wait,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	Lease     string `json:"lease,omitempty"`
	All       bool   `json:"all,omitempty"`
}

type response struct {
	Lease  string      `json:"lease,omitempty"`
	State  string      `json:"state,omitempty"`
	Kind   string      `json:"kind,omitempty"`
	Node   *int        `json:"node,omitempty"`
	Size   uint64      `json:"size,omitempty"`
	Serial string      `json:"serial,omitempty"`
	Dax    string      `json:"dax,omitempty"`
	Error  string      `json:"error,omitempty"`
	Leases *[]response `json:"leases,omitempty"`
}

const usage = `usage:
  cxl-request memory <size> [--wait DURATION]
  cxl-request shared <serial> [--wait DURATION] [--exec CMD ARGS...]
  cxl-request fd <lease> --exec CMD ARGS...
  cxl-request release <lease>|--all [--wait DURATION]   (also: cxl-release ...)
  cxl-request list
The socket is $CXL_LEASE_SOCKET. --wait 0s returns at once.
`

func main() {
	os.Exit(run(os.Args, os.Stdout, os.Stderr))
}

// command is a parsed command line.
type command struct {
	req  request
	exec []string
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
	cmd := &command{}
	var pos []string
	for i := 1; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--exec":
			cmd.exec = args[i+1:]
			if len(cmd.exec) == 0 {
				return nil, errors.New("--exec needs a command")
			}
			i = len(args)
		case a == "--wait":
			if i+1 >= len(args) {
				return nil, errors.New("--wait needs a duration")
			}
			i++
			cmd.req.Wait = args[i]
		case strings.HasPrefix(a, "--wait="):
			cmd.req.Wait = strings.TrimPrefix(a, "--wait=")
		case a == "--request-id":
			if i+1 >= len(args) {
				return nil, errors.New("--request-id needs a value")
			}
			i++
			cmd.req.RequestID = args[i]
		case a == "--all":
			cmd.req.All = true
		case strings.HasPrefix(a, "-"):
			return nil, fmt.Errorf("unknown option %s", a)
		default:
			pos = append(pos, a)
		}
	}
	one := func(what string) (string, error) {
		if len(pos) != 1 {
			return "", fmt.Errorf("%s: expected one %s", args[0], what)
		}
		return pos[0], nil
	}
	var err error
	switch args[0] {
	case "memory":
		cmd.req.Op, cmd.req.Kind = "lease", "memory"
		cmd.req.Size, err = one("size")
		if err == nil && cmd.exec != nil {
			err = errors.New("--exec is for shared leases")
		}
	case "shared":
		cmd.req.Op, cmd.req.Kind = "lease", "shared"
		cmd.req.Serial, err = one("serial")
	case "fd":
		cmd.req.Op = "fd"
		cmd.req.Lease, err = one("lease")
		if err == nil && cmd.exec == nil {
			err = errors.New("fd needs --exec")
		}
	case "release":
		cmd.req.Op = "release"
		switch {
		case cmd.req.All && len(pos) == 0:
		case !cmd.req.All && len(pos) == 1:
			cmd.req.Lease = pos[0]
		default:
			err = errors.New("release: expected one lease or --all")
		}
	case "list":
		cmd.req.Op = "list"
		if len(pos) != 0 {
			err = errors.New("list takes no arguments")
		}
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		return nil, err
	}
	if cmd.exec != nil && cmd.req.Op != "lease" && cmd.req.Op != "fd" {
		return nil, errors.New("--exec is for shared and fd")
	}
	return cmd, nil
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

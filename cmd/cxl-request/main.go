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

// Command cxl-request requests CXL memory for the running container, of
// the node's local memdevs or of a memory pool, from kubelet-cxl-plugin
// through the lease socket $CXL_LEASE_SOCKET, which the plugin mounts into
// containers of claims on DeviceClass cxl-lease. See doc/cxl/LEASES.md.
//
//	cxl-request request [--source local|pool] [--mode numa|dax] [--shared] [--read-only]
//	                    [--size SIZE] [--serial SERIAL] [--wait DURATION] [--request-id ID]
//	                    [--exec CMD [ARG...]]
//	cxl-request release (--lease ID | --all) [--wait DURATION]
//	cxl-request list
//	cxl-request fd  --lease ID [--wait DURATION] --exec CMD [ARG...]
//	cxl-request env --lease ID [--wait DURATION] [--exec CMD [ARG...]]
//	cxl-request help
//
// request, release and list print the plugin's JSON response line, env
// prints the environment of the lease as sorted KEY=VALUE lines. With
// --exec CMD runs with the environment of the lease, and with the fd of a
// dax lease as fd 3. Exit codes: 0 ok, 1 error, 2 usage, 3 the lease is
// Failed, not Ready (request, fd, env) or not Released (release) within
// --wait, or fd of a lease that is not a dax lease. With --exec the exit
// code of CMD is passed through.
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
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
)

const (
	exitOK      = 0
	exitError   = 1
	exitUsage   = 2
	exitNotUsed = 3 // failed, not Ready or not Released in time, not a dax lease
	execFd      = 3 // the fd of a dax lease in the command
)

const usage = `usage:
  cxl-request request [--source local|pool] [--mode numa|dax] [--shared] [--read-only]
                      [--size SIZE] [--serial SERIAL] [--wait DURATION] [--request-id ID]
                      [--exec CMD [ARG...]]
  cxl-request release (--lease ID | --all) [--wait DURATION]
  cxl-request list
  cxl-request fd  --lease ID [--wait DURATION] --exec CMD [ARG...]
  cxl-request env --lease ID [--wait DURATION] [--exec CMD [ARG...]]
  cxl-request help
request: memory for this container, exclusive unless --shared:
  --source    local (a memdev of this node) or pool; default local, then pool
  --mode      numa (lent as a NUMA node) or dax (a devdax fd); default numa,
              dax with --shared or for a shared device
  --shared    one host of a shared pool device (default: all of its hosts)
  --read-only a dax fd that allows reads only
  --size      at least this much, a quantity such as 512Mi
  --serial    this device (0x hex or decimal)
  --wait      until Ready (Go duration; 0s returns at once); default: the
              plugin's --lease-wait; release: until Released
  --request-id the same id returns the same lease
  --exec      run CMD with the lease's environment and a dax lease's fd 3
release, fd, env: --lease ID of a lease of this pod; release --all: all of them.
The socket is $CXL_LEASE_SOCKET.
`

// options lists the options of each operation; --exec is the last one.
var options = map[string][]string{
	api.OpRequest: {"--source", "--mode", "--shared", "--read-only", "--size", "--serial", "--wait", "--request-id", "--exec"},
	api.OpRelease: {"--lease", "--all", "--wait"},
	api.OpList:    {},
	api.OpFd:      {"--lease", "--wait", "--exec"},
	api.OpEnv:     {"--lease", "--wait", "--exec"},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// command is a parsed command line.
type command struct {
	req  api.Request
	exec []string // nil without --exec
}

// usageErr prints a usage error and the usage, and returns the exit code
// of usage errors.
func usageErr(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "cxl-request: %v\n", err)
	fmt.Fprint(stderr, usage)
	return exitUsage
}

// parseArgs returns the command of the arguments after the program name:
// the operation, then its options. --exec ends the options: the rest is
// the command to run.
func parseArgs(args []string) (*command, error) {
	if len(args) == 0 {
		return nil, errors.New("no operation")
	}
	cmd := &command{req: api.Request{Op: args[0]}}
	if _, ok := options[cmd.req.Op]; !ok {
		return nil, fmt.Errorf("unknown operation %q", cmd.req.Op)
	}
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		name, val, inline := strings.Cut(rest[i], "=")
		if err := cmd.checkOption(name, rest[i]); err != nil {
			return nil, err
		}
		if name == "--exec" {
			if inline || i+1 >= len(rest) {
				return nil, errors.New("--exec needs a command")
			}
			cmd.exec = rest[i+1:]
			break
		}
		if !inline && !cmd.isFlag(name) {
			if i+1 >= len(rest) {
				return nil, fmt.Errorf("%s needs a value", name)
			}
			i++
			val = rest[i]
		}
		if err := cmd.setOption(name, val, inline); err != nil {
			return nil, err
		}
	}
	if cmd.req.Op == api.OpFd && cmd.exec == nil {
		return nil, errors.New("fd needs --exec")
	}
	return cmd, cmd.req.Validate()
}

// values returns the options with a value of the command.
func (cmd *command) values() map[string]*string {
	return map[string]*string{
		"--source": &cmd.req.Source, "--mode": &cmd.req.Mode, "--size": &cmd.req.Size, "--serial": &cmd.req.Serial,
		"--wait": &cmd.req.Wait, "--request-id": &cmd.req.RequestID, "--lease": &cmd.req.Lease,
	}
}

// flags returns the options without a value of the command.
func (cmd *command) flags() map[string]*bool {
	return map[string]*bool{"--shared": &cmd.req.Shared, "--read-only": &cmd.req.ReadOnly, "--all": &cmd.req.All}
}

// isFlag tells whether an option takes no value.
func (cmd *command) isFlag(name string) bool {
	return cmd.flags()[name] != nil
}

// checkOption returns an error if arg, an option name, is not an option of
// the operation of the command.
func (cmd *command) checkOption(name, arg string) error {
	switch {
	case !strings.HasPrefix(name, "--"):
		return fmt.Errorf("unexpected argument %q", arg)
	case cmd.values()[name] == nil && !cmd.isFlag(name) && name != "--exec":
		return fmt.Errorf("unknown option %s", name)
	case name == "--exec" && (cmd.req.Op == api.OpRelease || cmd.req.Op == api.OpList):
		return errors.New("--exec is for request, fd and env")
	case !slices.Contains(options[cmd.req.Op], name):
		return fmt.Errorf("%s is not an option of %s", name, cmd.req.Op)
	}
	return nil
}

// setOption sets an option of the command: a flag to true, or to val if
// it was given inline (--opt=VAL), an option with a value to val.
func (cmd *command) setOption(name, val string, inline bool) error {
	flag := cmd.flags()[name]
	switch {
	case flag != nil && inline:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("%s: invalid value %q", name, val)
		}
		*flag = b
	case flag != nil:
		*flag = true
	default:
		*cmd.values()[name] = val
	}
	return nil
}

// run runs the command line of the arguments after the program name and
// returns the exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		fmt.Fprint(stdout, usage)
		return exitOK
	}
	cmd, err := parseArgs(args)
	if err != nil {
		return usageErr(stderr, err)
	}
	sock := os.Getenv(api.SocketEnv)
	if sock == "" {
		fmt.Fprintf(stderr, "cxl-request: %s is not set (the pod needs a claim on DeviceClass cxl-lease)\n", api.SocketEnv)
		return exitError
	}
	line, fd, err := roundTrip(sock, &cmd.req)
	if err != nil {
		fmt.Fprintf(stderr, "cxl-request: %v\n", err)
		return exitError
	}
	var resp api.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		closeFd(fd)
		fmt.Fprintf(stderr, "cxl-request: invalid response %q: %v\n", line, err)
		return exitError
	}
	if cmd.exec == nil {
		closeFd(fd)
		return output(cmd, &resp, line, stdout, stderr)
	}
	return execute(cmd, &resp, line, fd, stdout, stderr)
}

// output prints the response of a command without --exec, and returns the
// exit code: env prints the environment of a Ready lease, the other
// operations the response line.
func output(cmd *command, resp *api.Response, line []byte, stdout, stderr io.Writer) int {
	code := exitCode(&cmd.req, resp)
	switch {
	case cmd.req.Op != api.OpEnv:
		fmt.Fprintf(stdout, "%s\n", bytes.TrimSpace(line))
	case code == exitOK:
		for _, kv := range api.ExecEnv(resp.Env, resp.Lease, -1) {
			fmt.Fprintln(stdout, kv)
		}
	default:
		fmt.Fprintf(stderr, "cxl-request: %s\n", bytes.TrimSpace(line))
	}
	return code
}

// execute runs the command of --exec with the environment of a Ready
// lease, and its fd for a dax lease of request and fd, and returns its
// exit code.
func execute(cmd *command, resp *api.Response, line []byte, fd int, stdout, stderr io.Writer) int {
	code := exitCode(&cmd.req, resp)
	if code == exitOK && resp.State != api.StateReady {
		code = exitNotUsed // CMD needs the memory
	}
	if code != exitOK {
		closeFd(fd)
		fmt.Fprintf(stderr, "cxl-request: %s\n", bytes.TrimSpace(line))
		return code
	}
	envFd := -1
	switch {
	case cmd.req.Op == api.OpEnv || resp.Mode != api.ModeDax:
		closeFd(fd)
		fd = -1
	case fd < 0:
		fmt.Fprintf(stderr, "cxl-request: no fd with the response of dax lease %s\n", resp.Lease)
		return exitError
	default:
		envFd = execFd
	}
	return runCommand(cmd.exec, fd, api.ExecEnv(resp.Env, resp.Lease, envFd), stdout, stderr)
}

// exitCode returns the exit code of a response.
func exitCode(req *api.Request, resp *api.Response) int {
	switch {
	case resp.State == api.StateFailed:
		return exitNotUsed
	case req.Op == api.OpFd && resp.Mode != "" && resp.Mode != api.ModeDax:
		return exitNotUsed
	case resp.Error != "":
		return exitError
	}
	switch req.Op {
	case api.OpRequest:
		if d, err := time.ParseDuration(req.Wait); resp.State == api.StateReady || (err == nil && d == 0) {
			return exitOK
		}
		return exitNotUsed
	case api.OpFd, api.OpEnv:
		if resp.State != api.StateReady {
			return exitNotUsed
		}
	case api.OpRelease:
		return releaseExitCode(resp)
	}
	return exitOK
}

// releaseExitCode returns the exit code of a response to release: ok when
// the lease, or every lease, is Released.
func releaseExitCode(resp *api.Response) int {
	all := []api.Response{*resp}
	if resp.Leases != nil {
		all = *resp.Leases
	}
	for _, l := range all {
		if l.State != api.StateReleased {
			return exitNotUsed
		}
	}
	return exitOK
}

// roundTrip sends the request and reads one response line with the fd
// that came with it (-1 for none).
func roundTrip(sock string, req *api.Request) ([]byte, int, error) {
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

// runCommand runs the command with env added to the environment and fd as
// fd 3 (none if fd < 0), forwards signals, and returns its exit code. (A
// fork rather than an in-place exec: dup2 onto fd 3 could clobber a file
// the Go runtime holds there.)
func runCommand(argv []string, fd int, env []string, stdout, stderr io.Writer) int {
	c := exec.Command(argv[0], argv[1:]...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, stdout, stderr
	c.Env = append(os.Environ(), env...)
	var f *os.File
	if fd >= 0 {
		f = os.NewFile(uintptr(fd), "dax")
		defer f.Close()
		c.ExtraFiles = []*os.File{f} // fd 3
	}
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(sigs)
	if err := c.Start(); err != nil {
		fmt.Fprintf(stderr, "cxl-request: %v\n", err)
		return exitError
	}
	if f != nil {
		_ = f.Close() // the child has it
	}
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

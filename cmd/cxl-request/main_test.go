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

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

func TestParseArgs(t *testing.T) {
	for _, c := range []struct {
		prog string
		args []string
		req  request
		exec []string
		err  string
	}{
		{"cxl-request", []string{"memory", "512Mi"}, request{Op: "lease", Kind: "memory", Size: "512Mi"}, nil, ""},
		{"cxl-request", []string{"memory", "512Mi", "--wait", "10s"}, request{Op: "lease", Kind: "memory", Size: "512Mi", Wait: "10s"}, nil, ""},
		{"cxl-request", []string{"memory", "--wait=0s", "1Gi", "--request-id", "x"}, request{Op: "lease", Kind: "memory", Size: "1Gi", Wait: "0s", RequestID: "x"}, nil, ""},
		{"cxl-request", []string{"shared", "0xc1ae0001", "--exec", "python3", "-c", "--wait"}, request{Op: "lease", Kind: "shared", Serial: "0xc1ae0001"}, []string{"python3", "-c", "--wait"}, ""},
		{"cxl-request", []string{"fd", "abcd0123", "--exec", "cat"}, request{Op: "fd", Lease: "abcd0123"}, []string{"cat"}, ""},
		{"cxl-request", []string{"release", "abcd0123"}, request{Op: "release", Lease: "abcd0123"}, nil, ""},
		{"cxl-request", []string{"release", "--all"}, request{Op: "release", All: true}, nil, ""},
		{"/usr/local/bin/cxl-release", []string{"abcd0123"}, request{Op: "release", Lease: "abcd0123"}, nil, ""},
		{"cxl-release", []string{"--all", "--wait", "1m"}, request{Op: "release", All: true, Wait: "1m"}, nil, ""},
		{"cxl-request", []string{"list"}, request{Op: "list"}, nil, ""},
		{"cxl-request", nil, request{}, nil, "no command"},
		{"cxl-request", []string{"memory"}, request{}, nil, "expected one size"},
		{"cxl-request", []string{"memory", "1Gi", "--exec", "x"}, request{}, nil, "--exec is for shared"},
		{"cxl-request", []string{"fd", "abc"}, request{}, nil, "fd needs --exec"},
		{"cxl-request", []string{"release"}, request{}, nil, "one lease or --all"},
		{"cxl-request", []string{"release", "a", "--all"}, request{}, nil, "one lease or --all"},
		{"cxl-request", []string{"list", "--bogus"}, request{}, nil, "unknown option"},
		{"cxl-request", []string{"shared", "1", "--exec"}, request{}, nil, "needs a command"},
		{"cxl-request", []string{"frob"}, request{}, nil, "unknown command"},
	} {
		cmd, err := parseArgs(c.prog, c.args)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%v: expected error %q, got %v", c.args, c.err, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%v: %v", c.args, err)
			continue
		}
		if cmd.req != c.req || !reflect.DeepEqual(cmd.exec, c.exec) {
			t.Errorf("%v: got %+v %v", c.args, cmd.req, cmd.exec)
		}
	}
}

func TestExitCode(t *testing.T) {
	released := []response{{State: "Released"}}
	releasing := []response{{State: "Releasing"}}
	for _, c := range []struct {
		req  request
		resp response
		code int
	}{
		{request{Op: "lease"}, response{State: "Ready"}, exitOK},
		{request{Op: "lease"}, response{State: "Pending"}, exitNotUsed},
		{request{Op: "lease", Wait: "0s"}, response{State: "Pending"}, exitOK},
		{request{Op: "lease"}, response{State: "Failed", Error: "timeout"}, exitNotUsed},
		{request{Op: "lease"}, response{Error: "not a container of the claim's pod"}, exitError},
		{request{Op: "release"}, response{State: "Released"}, exitOK},
		{request{Op: "release"}, response{State: "Releasing"}, exitNotUsed},
		{request{Op: "release", All: true}, response{Leases: &released}, exitOK},
		{request{Op: "release", All: true}, response{Leases: &releasing}, exitNotUsed},
		{request{Op: "list"}, response{Leases: &[]response{}}, exitOK},
	} {
		if got := exitCode(&c.req, &c.resp); got != c.code {
			t.Errorf("%+v %+v: exit %d, want %d", c.req, c.resp, got, c.code)
		}
	}
}

// fakeServer answers one request per connection with resp, and with the
// file as SCM_RIGHTS if fdFile is not empty.
func fakeServer(t *testing.T, resp string, fdFile string) (string, chan request) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cxlreq")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "cxl.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	reqs := make(chan request, 10)
	go func() {
		for {
			c, err := l.AcceptUnix()
			if err != nil {
				return
			}
			line, _ := bufio.NewReader(c).ReadBytes('\n')
			var r request
			_ = json.Unmarshal(line, &r)
			reqs <- r
			if fdFile != "" {
				f, _ := os.Open(fdFile)
				_, _, _ = c.WriteMsgUnix([]byte(resp+"\n"), syscall.UnixRights(int(f.Fd())), nil)
				f.Close()
			} else {
				_, _ = c.Write([]byte(resp + "\n"))
			}
			c.Close()
		}
	}()
	return sock, reqs
}

func TestRunPrintsResponse(t *testing.T) {
	resp := `{"lease":"000000a1","state":"Ready","kind":"memory","node":2,"size":268435456,"serial":"0xc1ee0001"}`
	sock, reqs := fakeServer(t, resp, "")
	t.Setenv(socketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"cxl-request", "memory", "256Mi", "--wait", "30s"}, &out, &errb); code != exitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if strings.TrimSpace(out.String()) != resp {
		t.Fatalf("output %q", out.String())
	}
	if r := <-reqs; r != (request{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "30s"}) {
		t.Fatalf("request %+v", r)
	}

	t.Setenv(socketEnv, "")
	if code := run([]string{"cxl-request", "list"}, &out, &errb); code != exitError || !strings.Contains(errb.String(), socketEnv) {
		t.Fatalf("no socket: exit %d %s", code, errb.String())
	}
	if code := run([]string{"cxl-request", "bogus"}, &out, &errb); code != exitUsage {
		t.Fatalf("usage: exit %d", code)
	}
}

func TestRunExecWithFd(t *testing.T) {
	dir := t.TempDir()
	dax := filepath.Join(dir, "dax")
	if err := os.WriteFile(dax, []byte("TAG-1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	resp := `{"lease":"000000a2","state":"Ready","kind":"shared","size":268435456,"serial":"0xc1ae0001","dax":"/dev/dax0.0"}`
	sock, _ := fakeServer(t, resp, dax)
	t.Setenv(socketEnv, sock)
	var out, errb bytes.Buffer
	script := `echo "$CXL_LEASE_FD $CXL_LEASE $CXL_LEASE_DAX $CXL_LEASE_SIZE"; head -c 8 <&3; exit 7`
	code := run([]string{"cxl-request", "shared", "0xc1ae0001", "--exec", "/bin/sh", "-c", script}, &out, &errb)
	if code != 7 {
		t.Fatalf("exit %d (stderr %s)", code, errb.String())
	}
	want := "3 000000a2 /dev/dax0.0 268435456\nTAG-1234"
	if out.String() != want {
		t.Fatalf("output %q, want %q", out.String(), want)
	}

	// without --exec the JSON is printed and the fd dropped
	out.Reset()
	if code := run([]string{"cxl-request", "shared", "0xc1ae0001"}, &out, &errb); code != exitOK || strings.TrimSpace(out.String()) != resp {
		t.Fatalf("exit %d output %q", code, out.String())
	}
}

func TestRunExecWithoutFd(t *testing.T) {
	sock, _ := fakeServer(t, `{"lease":"000000a3","state":"Failed","kind":"shared","error":"timeout: not Ready after 2m0s"}`, "")
	t.Setenv(socketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"cxl-request", "shared", "0x1", "--exec", "/bin/true"}, &out, &errb); code != exitNotUsed {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(errb.String(), "timeout") {
		t.Fatalf("stderr %q", errb.String())
	}
}

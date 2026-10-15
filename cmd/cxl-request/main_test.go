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
	"time"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
)

func TestParseArgs(t *testing.T) {
	for _, c := range []struct {
		args []string
		req  api.Request
		exec []string
		err  string
	}{
		// every synopsis line
		{[]string{"request", "--size", "512Mi"}, api.Request{Op: "request", Size: "512Mi"}, nil, ""},
		{[]string{"request", "--source", "local", "--mode", "dax", "--read-only", "--size=64Mi", "--serial", "0xc100e2e1", "--wait", "10s", "--request-id", "r1", "--exec", "prog", "--wait"},
			api.Request{Op: "request", Source: "local", Mode: "dax", ReadOnly: true, Size: "64Mi", Serial: "0xc100e2e1", Wait: "10s", RequestID: "r1"}, []string{"prog", "--wait"}, ""},
		{[]string{"request", "--source=pool", "--size", "512Mi", "--mode", "dax", "--exec", "python3", "app.py"},
			api.Request{Op: "request", Source: "pool", Size: "512Mi", Mode: "dax"}, []string{"python3", "app.py"}, ""},
		{[]string{"request", "--serial", "0xc1ae0001"}, api.Request{Op: "request", Serial: "0xc1ae0001"}, nil, ""},
		{[]string{"request", "--serial", "0xc1ae0001", "--shared", "--read-only", "--exec", "prog"},
			api.Request{Op: "request", Serial: "0xc1ae0001", Shared: true, ReadOnly: true}, []string{"prog"}, ""},
		{[]string{"request", "--shared", "--size", "256Mi"}, api.Request{Op: "request", Shared: true, Size: "256Mi"}, nil, ""},
		{[]string{"request", "--shared=true", "--size", "256Mi"}, api.Request{Op: "request", Shared: true, Size: "256Mi"}, nil, ""},
		{[]string{"request", "--size", "1Gi", "--wait", "0s", "--request-id", "r1"}, api.Request{Op: "request", Size: "1Gi", Wait: "0s", RequestID: "r1"}, nil, ""},
		{[]string{"release", "--lease", "1a2b3c4d"}, api.Request{Op: "release", Lease: "1a2b3c4d"}, nil, ""},
		{[]string{"release", "--all", "--wait=1m"}, api.Request{Op: "release", All: true, Wait: "1m"}, nil, ""},
		{[]string{"list"}, api.Request{Op: "list"}, nil, ""},
		{[]string{"fd", "--lease", "1a2b3c4d", "--wait", "60s", "--exec", "prog"}, api.Request{Op: "fd", Lease: "1a2b3c4d", Wait: "60s"}, []string{"prog"}, ""},
		{[]string{"env", "--lease", "1a2b3c4d"}, api.Request{Op: "env", Lease: "1a2b3c4d"}, nil, ""},
		{[]string{"env", "--lease=1a2b3c4d", "--exec", "numactl", "--membind=2", "prog"}, api.Request{Op: "env", Lease: "1a2b3c4d"}, []string{"numactl", "--membind=2", "prog"}, ""},
		// validation errors
		{[]string{"request", "--size", "1Gi", "--source", "any"}, api.Request{}, nil, `invalid source "any" (local or pool)`},
		{[]string{"request", "--size", "1Gi", "--mode", "devdax"}, api.Request{}, nil, `invalid mode "devdax" (numa or dax)`},
		{[]string{"request", "--shared", "--mode", "numa"}, api.Request{}, nil, "shared memory is never onlined as RAM: use mode dax"},
		{[]string{"request", "--shared", "--source", "local"}, api.Request{}, nil, "local devices are not shareable; share a ResourceClaim between pods instead"},
		{[]string{"request", "--size", "1Gi", "--read-only"}, api.Request{}, nil, "readOnly needs mode dax"},
		{[]string{"request"}, api.Request{}, nil, "size or serial is required"},
		{[]string{"request", "--serial", "zz"}, api.Request{}, nil, `invalid serial "zz"`},
		{[]string{"release", "--all", "--exec", "cat"}, api.Request{}, nil, "--exec is for request, fd and env"},
		{[]string{"list", "--exec", "cat"}, api.Request{}, nil, "--exec is for request, fd and env"},
		{[]string{"fd", "--lease", "1a2b3c4d"}, api.Request{}, nil, "fd needs --exec"},
		{[]string{"release"}, api.Request{}, nil, "release takes --lease ID or --all"},
		{[]string{"release", "--lease", "1a2b3c4d", "--all"}, api.Request{}, nil, "release takes --lease ID or --all"},
		{[]string{"request", "--size", "1Gi", "--wait", "soon"}, api.Request{}, nil, `invalid wait "soon"`},
		{[]string{"release", "--all", "--wait", "-1s"}, api.Request{}, nil, `invalid wait "-1s"`},
		// other usage errors
		{nil, api.Request{}, nil, "no operation"},
		{[]string{"memory", "512Mi"}, api.Request{}, nil, `unknown operation "memory"`},
		{[]string{"request", "512Mi"}, api.Request{}, nil, `unexpected argument "512Mi"`},
		{[]string{"request", "--size"}, api.Request{}, nil, "--size needs a value"},
		{[]string{"request", "--dax", "--size", "1Gi"}, api.Request{}, nil, "unknown option --dax"},
		{[]string{"request", "--size", "1Gi", "--lease", "1a2b3c4d"}, api.Request{}, nil, "--lease is not an option of request"},
		{[]string{"release", "--all", "--size", "1Gi"}, api.Request{}, nil, "--size is not an option of release"},
		{[]string{"list", "--all"}, api.Request{}, nil, "--all is not an option of list"},
		{[]string{"env", "--lease", "1a2b3c4d", "--read-only"}, api.Request{}, nil, "--read-only is not an option of env"},
		{[]string{"env"}, api.Request{}, nil, "env needs --lease ID"},
		{[]string{"fd", "--exec", "cat"}, api.Request{}, nil, "fd needs --lease ID"},
		{[]string{"request", "--size", "1Gi", "--exec"}, api.Request{}, nil, "--exec needs a command"},
		{[]string{"request", "--shared=maybe"}, api.Request{}, nil, `--shared: invalid value "maybe"`},
	} {
		cmd, err := parseArgs(c.args)
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
	released := []api.Response{{State: api.StateReleased}}
	releasing := []api.Response{{State: api.StateReleasing}}
	for _, c := range []struct {
		req  api.Request
		resp api.Response
		code int
	}{
		{api.Request{Op: "request"}, api.Response{State: "Ready"}, exitOK},
		{api.Request{Op: "request"}, api.Response{State: "Pending"}, exitNotUsed},
		{api.Request{Op: "request", Wait: "0s"}, api.Response{State: "Pending"}, exitOK},
		{api.Request{Op: "request", Wait: "0"}, api.Response{State: "Pending"}, exitOK},
		{api.Request{Op: "request"}, api.Response{State: "Failed", Error: "timeout"}, exitNotUsed},
		{api.Request{Op: "request"}, api.Response{Error: "not a container of the claim's pod"}, exitError},
		{api.Request{Op: "release"}, api.Response{State: "Released"}, exitOK},
		{api.Request{Op: "release"}, api.Response{State: "Releasing"}, exitNotUsed},
		{api.Request{Op: "release", All: true}, api.Response{Leases: &released}, exitOK},
		{api.Request{Op: "release", All: true}, api.Response{Leases: &releasing}, exitNotUsed},
		{api.Request{Op: "list"}, api.Response{Leases: &[]api.Response{}}, exitOK},
		{api.Request{Op: "fd"}, api.Response{State: "Ready", Mode: "dax"}, exitOK},
		{api.Request{Op: "fd"}, api.Response{State: "Pending", Mode: "dax"}, exitNotUsed},
		{api.Request{Op: "fd"}, api.Response{State: "Ready", Mode: "numa", Error: "lease 1a2b3c4d is not a dax lease; use env"}, exitNotUsed},
		{api.Request{Op: "fd"}, api.Response{Error: "no such lease of this container"}, exitError},
		{api.Request{Op: "env"}, api.Response{State: "Ready", Mode: "numa"}, exitOK},
		{api.Request{Op: "env", Wait: "0s"}, api.Response{State: "Pending"}, exitNotUsed},
	} {
		if got := exitCode(&c.req, &c.resp); got != c.code {
			t.Errorf("%+v %+v: exit %d, want %d", c.req, c.resp, got, c.code)
		}
	}
}

// fakeServer answers one request per connection with resp, and with the
// file as SCM_RIGHTS if fdFile is not empty.
func fakeServer(t *testing.T, resp string, fdFile string) (string, chan api.Request) {
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
	reqs := make(chan api.Request, 10)
	go func() {
		for {
			c, err := l.AcceptUnix()
			if err != nil {
				return
			}
			line, _ := bufio.NewReader(c).ReadBytes('\n')
			var r api.Request
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
	resp := `{"lease":"000000a1","state":"Ready","source":"local","mode":"numa","shared":false,"readOnly":false,"size":268435456,"serial":"0xc100e2e1","node":2}`
	sock, reqs := fakeServer(t, resp, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"request", "--size", "256Mi", "--wait", "30s"}, &out, &errb); code != exitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if strings.TrimSpace(out.String()) != resp {
		t.Fatalf("output %q", out.String())
	}
	if r := <-reqs; r != (api.Request{Op: "request", Size: "256Mi", Wait: "30s"}) {
		t.Fatalf("request %+v", r)
	}

	t.Setenv(api.SocketEnv, "")
	errb.Reset()
	if code := run([]string{"list"}, &out, &errb); code != exitError ||
		!strings.Contains(errb.String(), "CXL_LEASE_SOCKET is not set (the pod needs a claim on DeviceClass cxl-lease)") {
		t.Fatalf("no socket: exit %d %s", code, errb.String())
	}
	if code := run([]string{"bogus"}, &out, &errb); code != exitUsage {
		t.Fatalf("usage: exit %d", code)
	}
	out.Reset()
	if code := run([]string{"help"}, &out, &errb); code != exitOK || !strings.Contains(out.String(), "cxl-request env --lease ID") {
		t.Fatalf("help: exit %d %q", code, out.String())
	}
}

func TestRunEnv(t *testing.T) {
	resp := `{"lease":"1a2b3c4d","state":"Ready","source":"pool","mode":"dax","shared":false,"readOnly":false,"size":536870912,"serial":"0xc1ee0001","dax":"/dev/dax0.0",` +
		`"env":{"CXL_C1EE0001_DAX":"/dev/dax0.0","CXL_C1EE0001_MODE":"dax","CXL_C1EE0001_READONLY":"0","CXL_C1EE0001_SERIAL":"0xc1ee0001","CXL_C1EE0001_SHARED":"0","CXL_C1EE0001_SIZE":"536870912"}}`
	sock, reqs := fakeServer(t, resp, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"env", "--lease", "1a2b3c4d"}, &out, &errb); code != exitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	want := "CXL_C1EE0001_DAX=/dev/dax0.0\nCXL_C1EE0001_MODE=dax\nCXL_C1EE0001_READONLY=0\nCXL_C1EE0001_SERIAL=0xc1ee0001\n" +
		"CXL_C1EE0001_SHARED=0\nCXL_C1EE0001_SIZE=536870912\nCXL_LEASE=1a2b3c4d\n"
	if out.String() != want {
		t.Fatalf("env output\n%s\nwant\n%s", out.String(), want)
	}
	if r := <-reqs; r != (api.Request{Op: "env", Lease: "1a2b3c4d"}) {
		t.Fatalf("request %+v", r)
	}
}

func TestRunEnvNotReady(t *testing.T) {
	sock, _ := fakeServer(t, `{"lease":"1a2b3c4d","state":"Pending","mode":"numa","shared":false,"readOnly":false}`, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"env", "--lease", "1a2b3c4d", "--wait", "0s"}, &out, &errb); code != exitNotUsed || out.Len() != 0 {
		t.Fatalf("exit %d output %q", code, out.String())
	}
}

func TestRunExecDax(t *testing.T) {
	dir := t.TempDir()
	dax := filepath.Join(dir, "dax")
	if err := os.WriteFile(dax, []byte("TAG-1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	resp := `{"lease":"000000a2","state":"Ready","source":"pool","mode":"dax","shared":true,"readOnly":true,"size":268435456,"serial":"0xc1ae0001","dax":"/dev/dax0.0",` +
		`"env":{"CXL_C1AE0001_DAX":"/dev/dax0.0","CXL_C1AE0001_MODE":"dax","CXL_C1AE0001_READONLY":"1","CXL_C1AE0001_SERIAL":"0xc1ae0001","CXL_C1AE0001_SHARED":"1","CXL_C1AE0001_SIZE":"268435456"}}`
	sock, reqs := fakeServer(t, resp, dax)
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	script := `echo "$CXL_C1AE0001_FD $CXL_LEASE ${CXL_C1AE0001_DAX-none} $CXL_C1AE0001_SIZE $CXL_C1AE0001_READONLY $CXL_C1AE0001_SHARED"; head -c 8 <&3; exit 7`
	code := run([]string{"request", "--serial", "0xc1ae0001", "--shared", "--read-only", "--exec", "/bin/sh", "-c", script}, &out, &errb)
	if code != 7 {
		t.Fatalf("exit %d (stderr %s)", code, errb.String())
	}
	if r := <-reqs; r != (api.Request{Op: "request", Serial: "0xc1ae0001", Shared: true, ReadOnly: true}) {
		t.Fatalf("request %+v", r)
	}
	want := "3 000000a2 none 268435456 1 1\nTAG-1234"
	if out.String() != want {
		t.Fatalf("output %q, want %q", out.String(), want)
	}

	// fd: the same environment and fd
	out.Reset()
	if code := run([]string{"fd", "--lease", "000000a2", "--exec", "/bin/sh", "-c", script}, &out, &errb); code != 7 || out.String() != want {
		t.Fatalf("fd: exit %d output %q", code, out.String())
	}
	if r := <-reqs; r != (api.Request{Op: "fd", Lease: "000000a2"}) {
		t.Fatalf("request %+v", r)
	}

	// env --exec: no fd, the device path stays
	out.Reset()
	if code := run([]string{"env", "--lease", "000000a2", "--exec", "/bin/sh", "-c", `echo "${CXL_C1AE0001_FD-nofd} $CXL_C1AE0001_DAX"`}, &out, &errb); code != exitOK ||
		out.String() != "nofd /dev/dax0.0\n" {
		t.Fatalf("env --exec: exit %d output %q", code, out.String())
	}

	// without --exec the JSON is printed and the fd dropped
	out.Reset()
	if code := run([]string{"request", "--serial", "0xc1ae0001", "--shared"}, &out, &errb); code != exitOK || strings.TrimSpace(out.String()) != resp {
		t.Fatalf("exit %d output %q", code, out.String())
	}
}

func TestRunExecNUMA(t *testing.T) {
	resp := `{"lease":"000000a4","state":"Ready","source":"local","mode":"numa","shared":false,"readOnly":false,"size":134217728,"serial":"0xc100e2e1","node":2,` +
		`"env":{"CXL_C100E2E1_MODE":"numa","CXL_C100E2E1_NODE":"2","CXL_C100E2E1_SERIAL":"0xc100e2e1","CXL_C100E2E1_SHARED":"0","CXL_C100E2E1_SIZE":"134217728"}}`
	sock, _ := fakeServer(t, resp, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	script := `echo "$CXL_C100E2E1_NODE $CXL_C100E2E1_SIZE $CXL_LEASE ${CXL_C100E2E1_FD-nofd}"; exec 3<&- 2>/dev/null; exit 0`
	if code := run([]string{"request", "--size", "128Mi", "--exec", "/bin/sh", "-c", script}, &out, &errb); code != exitOK {
		t.Fatalf("exit %d (stderr %s)", code, errb.String())
	}
	if out.String() != "2 134217728 000000a4 nofd\n" {
		t.Fatalf("output %q", out.String())
	}
	out.Reset()
	if code := run([]string{"env", "--lease", "000000a4", "--exec", "/bin/sh", "-c", script}, &out, &errb); code != exitOK || out.String() != "2 134217728 000000a4 nofd\n" {
		t.Fatalf("env --exec: exit %d output %q", code, out.String())
	}
	// a numa lease has no fd
	errb.Reset()
	numa := `{"lease":"000000a4","state":"Ready","mode":"numa","shared":false,"readOnly":false,"error":"lease 000000a4 is not a dax lease; use env"}`
	sock, _ = fakeServer(t, numa, "")
	t.Setenv(api.SocketEnv, sock)
	if code := run([]string{"fd", "--lease", "000000a4", "--exec", "/bin/true"}, &out, &errb); code != exitNotUsed || !strings.Contains(errb.String(), "not a dax lease") {
		t.Fatalf("fd of a numa lease: exit %d %s", code, errb.String())
	}
}

func TestRunExecNotReady(t *testing.T) {
	sock, _ := fakeServer(t, `{"lease":"000000a3","state":"Failed","mode":"dax","shared":true,"readOnly":false,"error":"timeout: not Ready after 2m0s"}`, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"request", "--serial", "0x1", "--shared", "--exec", "/bin/true"}, &out, &errb); code != exitNotUsed {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(errb.String(), "timeout") {
		t.Fatalf("stderr %q", errb.String())
	}
	// asynchronous: Pending is ok without --exec, not with it
	sock, _ = fakeServer(t, `{"lease":"000000a5","state":"Pending","mode":"numa","shared":false,"readOnly":false}`, "")
	t.Setenv(api.SocketEnv, sock)
	if code := run([]string{"request", "--size", "1Gi", "--wait", "0s"}, &out, &errb); code != exitOK {
		t.Fatalf("wait 0s: exit %d", code)
	}
	if code := run([]string{"request", "--size", "1Gi", "--wait", "0s", "--exec", "/bin/true"}, &out, &errb); code != exitNotUsed {
		t.Fatalf("wait 0s --exec: exit %d", code)
	}
}

// A command that a signal ends exits 128+signal.
func TestRunExecSignaled(t *testing.T) {
	sock, _ := fakeServer(t, `{"lease":"000000a6","state":"Ready","mode":"numa","shared":false,"readOnly":false}`, "")
	t.Setenv(api.SocketEnv, sock)
	var out, errb bytes.Buffer
	if code := run([]string{"request", "--size", "1Gi", "--exec", "/bin/sh", "-c", "kill -TERM $$"}, &out, &errb); code != 128+int(syscall.SIGTERM) {
		t.Fatalf("exit %d, expected %d (stderr %s)", code, 128+int(syscall.SIGTERM), errb.String())
	}
}

// A signal to cxl-request is forwarded to the command.
func TestRunExecForwardsSignals(t *testing.T) {
	sock, _ := fakeServer(t, `{"lease":"000000a7","state":"Ready","mode":"numa","shared":false,"readOnly":false}`, "")
	t.Setenv(api.SocketEnv, sock)
	ready := filepath.Join(t.TempDir(), "ready")
	script := `trap 'exit 42' TERM; touch "$1"; while :; do sleep 0.01; done`
	done := make(chan int, 1)
	var out, errb bytes.Buffer
	go func() {
		done <- run([]string{"request", "--size", "1Gi", "--exec", "/bin/sh", "-c", script, "sh", ready}, &out, &errb)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != 42 {
			t.Fatalf("exit %d, expected the trap's 42", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the signal was not forwarded")
	}
}

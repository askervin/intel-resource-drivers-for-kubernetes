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

package api

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/containers/nri-plugins/pkg/cxl/memctl"
)

func TestModesMatchMemctl(t *testing.T) {
	if ModeNUMA != memctl.ModeNUMA || ModeDax != memctl.ModeDax {
		t.Fatalf("api modes %q %q, memctl modes %q %q", ModeNUMA, ModeDax, memctl.ModeNUMA, memctl.ModeDax)
	}
}

func TestRequestResponseRoundTrip(t *testing.T) {
	req := Request{Op: OpRequest, Source: SourcePool, Mode: ModeDax, Shared: true, ReadOnly: true, Size: "256Mi",
		Serial: "0xc1ae0001", Wait: "10s", RequestID: "r1", Lease: "1a2b3c4d", All: true}
	data, err := json.Marshal(&req)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"op":"request","source":"pool","mode":"dax","shared":true,"readOnly":true,"size":"256Mi","serial":"0xc1ae0001","wait":"10s","requestId":"r1","lease":"1a2b3c4d","all":true}`
	if string(data) != want {
		t.Fatalf("request JSON\n%s\nwant\n%s", data, want)
	}
	var back Request
	if err := json.Unmarshal(data, &back); err != nil || back != req {
		t.Fatalf("request round trip %+v %v", back, err)
	}
	if data, _ := json.Marshal(&Request{Op: OpList}); string(data) != `{"op":"list"}` {
		t.Fatalf("minimal request %s", data)
	}

	node := 2
	resp := Response{Lease: "1a2b3c4d", State: StateReady, Source: SourceLocal, Mode: ModeNUMA, Size: 536870912,
		Serial: "0xc100e2e1", Node: &node, Env: map[string]string{"CXL_C100E2E1_MODE": "numa"},
		Leases: &[]Response{{Lease: "00000001", State: StatePending}}}
	data, err = json.Marshal(&resp)
	if err != nil {
		t.Fatal(err)
	}
	want = `{"lease":"1a2b3c4d","state":"Ready","source":"local","mode":"numa","shared":false,"readOnly":false,"size":536870912,"serial":"0xc100e2e1","node":2,"env":{"CXL_C100E2E1_MODE":"numa"},"leases":[{"lease":"00000001","state":"Pending","shared":false,"readOnly":false}]}`
	if string(data) != want {
		t.Fatalf("response JSON\n%s\nwant\n%s", data, want)
	}
	var rback Response
	if err := json.Unmarshal(data, &rback); err != nil || !reflect.DeepEqual(rback, resp) {
		t.Fatalf("response round trip %+v %v", rback, err)
	}
}

func TestValidate(t *testing.T) {
	for _, c := range []struct {
		req Request
		err string
	}{
		{Request{Op: OpRequest, Size: "512Mi"}, ""},
		{Request{Op: OpRequest, Serial: "0xc1ae0001"}, ""},
		{Request{Op: OpRequest, Shared: true}, ""},
		{Request{Op: OpRequest, Shared: true, ReadOnly: true, Serial: "3249405953"}, ""},
		{Request{Op: OpRequest, Source: SourceLocal, Size: "64Mi", Mode: ModeDax, ReadOnly: true}, ""},
		{Request{Op: OpRequest, Size: "1Gi", Wait: "0s", RequestID: "r1"}, ""},
		{Request{Op: OpRelease, Lease: "1a2b3c4d"}, ""},
		{Request{Op: OpRelease, All: true, Wait: "1m"}, ""},
		{Request{Op: OpList}, ""},
		{Request{Op: OpFd, Lease: "1a2b3c4d"}, ""},
		{Request{Op: OpEnv, Lease: "1a2b3c4d", Wait: "60s"}, ""},
		{Request{Op: OpRequest, Size: "1Gi", Source: "remote"}, `invalid source "remote" (local or pool)`},
		{Request{Op: OpRequest, Size: "1Gi", Source: "any"}, `invalid source "any" (local or pool)`},
		{Request{Op: OpRequest, Size: "1Gi", Mode: "devdax"}, `invalid mode "devdax" (numa or dax)`},
		{Request{Op: OpRequest, Shared: true, Mode: ModeNUMA}, "shared memory is never onlined as RAM: use mode dax"},
		{Request{Op: OpRequest, Shared: true, Source: SourceLocal}, "local devices are not shareable; share a ResourceClaim between pods instead"},
		{Request{Op: OpRequest, Size: "1Gi", ReadOnly: true}, "readOnly needs mode dax"},
		{Request{Op: OpRequest, Size: "1Gi", ReadOnly: true, Mode: ModeNUMA}, "readOnly needs mode dax"},
		{Request{Op: OpRequest}, "size or serial is required"},
		{Request{Op: OpRequest, Serial: "c1ae0001"}, `invalid serial "c1ae0001"`},
		{Request{Op: OpRelease}, "release takes --lease ID or --all"},
		{Request{Op: OpRelease, Lease: "1a2b3c4d", All: true}, "release takes --lease ID or --all"},
		{Request{Op: OpRequest, Size: "1Gi", Wait: "soon"}, `invalid wait "soon"`},
		{Request{Op: OpRelease, All: true, Wait: "-1s"}, `invalid wait "-1s"`},
		{Request{Op: OpFd}, "fd needs --lease ID"},
		{Request{Op: OpEnv}, "env needs --lease ID"},
		{Request{Op: "lease"}, `unknown op "lease"`},
	} {
		err := c.req.Validate()
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%+v: %v", c.req, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%+v: expected error %q, got %v", c.req, c.err, err)
		}
	}
}

func TestEffectiveMode(t *testing.T) {
	for _, c := range []struct {
		req  Request
		mode string
	}{
		{Request{}, ModeNUMA},
		{Request{Shared: true}, ModeDax},
		{Request{Mode: ModeDax}, ModeDax},
		{Request{Mode: ModeNUMA}, ModeNUMA},
	} {
		if got := c.req.EffectiveMode(); got != c.mode {
			t.Errorf("%+v: mode %s, want %s", c.req, got, c.mode)
		}
	}
}

func TestKeys(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{Key(0xc100e2e1, 0), "C100E2E1"},
		{Key(0xc100e2e1, 1), "C100E2E1"},
		{Key(0xc100e2e1, 2), "C100E2E1_2"},
		{Key(0x1, 3), "1_3"},
		{RegionKey("region0"), "REGION0"},
		{RegionKey("fake-region12"), "FAKE_REGION12"},
		{Nth(DRAMKey, 2), "DRAM_2"},
		{Nth(RegionKey("region1"), 1), "REGION1"},
	} {
		if c.got != c.want {
			t.Errorf("key %q, want %q", c.got, c.want)
		}
	}
}

func TestEnv(t *testing.T) {
	for _, c := range []struct {
		name string
		mem  Memory
		want map[string]string
	}{
		{"memdev numa", Memory{Key: Key(0xc100e2e1, 1), Serial: "0xc100e2e1", Mode: ModeNUMA, Size: 134217728, Node: 2},
			map[string]string{"CXL_C100E2E1_SERIAL": "0xc100e2e1", "CXL_C100E2E1_SHARED": "0", "CXL_C100E2E1_MODE": "numa",
				"CXL_C100E2E1_SIZE": "134217728", "CXL_C100E2E1_NODE": "2"}},
		{"memdev dax read-only", Memory{Key: Key(0xc100e2e1, 1), Serial: "0xc100e2e1", Mode: ModeDax, Size: 134217728, Dax: "/dev/dax0.1", ReadOnly: true},
			map[string]string{"CXL_C100E2E1_SERIAL": "0xc100e2e1", "CXL_C100E2E1_SHARED": "0", "CXL_C100E2E1_MODE": "dax",
				"CXL_C100E2E1_SIZE": "134217728", "CXL_C100E2E1_DAX": "/dev/dax0.1", "CXL_C100E2E1_READONLY": "1"}},
		{"shared pool device dax", Memory{Key: Key(0xc1ae0001, 1), Serial: "0xc1ae0001", Shared: true, Mode: ModeDax, Size: 268435456, Dax: "/dev/dax0.0"},
			map[string]string{"CXL_C1AE0001_SERIAL": "0xc1ae0001", "CXL_C1AE0001_SHARED": "1", "CXL_C1AE0001_MODE": "dax",
				"CXL_C1AE0001_SIZE": "268435456", "CXL_C1AE0001_DAX": "/dev/dax0.0", "CXL_C1AE0001_READONLY": "0"}},
		{"second slice", Memory{Key: Key(0xc100e2e1, 2), Serial: "0xc100e2e1", Mode: ModeNUMA, Size: 134217728, Node: 0},
			map[string]string{"CXL_C100E2E1_2_SERIAL": "0xc100e2e1", "CXL_C100E2E1_2_SHARED": "0", "CXL_C100E2E1_2_MODE": "numa",
				"CXL_C100E2E1_2_SIZE": "134217728", "CXL_C100E2E1_2_NODE": "0"}},
		{"region", Memory{Key: RegionKey("region0"), Mode: ModeNUMA, Size: 67108864, Node: 2},
			map[string]string{"CXL_REGION0_MODE": "numa", "CXL_REGION0_SIZE": "67108864", "CXL_REGION0_NODE": "2"}},
		{"dram", Memory{Key: DRAMKey, Mode: ModeNUMA, Size: 67108864, Nodes: "0-1"},
			map[string]string{"CXL_DRAM_MODE": "numa", "CXL_DRAM_SIZE": "67108864", "CXL_DRAM_NODES": "0-1"}},
		{"dax lease without a device path", Memory{Key: Key(0xc1ee0001, 1), Serial: "0xc1ee0001", Mode: ModeDax, Size: 1},
			map[string]string{"CXL_C1EE0001_SERIAL": "0xc1ee0001", "CXL_C1EE0001_SHARED": "0", "CXL_C1EE0001_MODE": "dax",
				"CXL_C1EE0001_SIZE": "1", "CXL_C1EE0001_READONLY": "0"}},
	} {
		if got := Env(c.mem); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: env\n%v\nwant\n%v", c.name, got, c.want)
		}
	}
}

func TestExecEnv(t *testing.T) {
	dax := Env(Memory{Key: Key(0xc1ee0001, 1), Serial: "0xc1ee0001", Mode: ModeDax, Size: 536870912, Dax: "/dev/dax0.0"})
	want := []string{"CXL_C1EE0001_FD=3", "CXL_C1EE0001_MODE=dax", "CXL_C1EE0001_READONLY=0", "CXL_C1EE0001_SERIAL=0xc1ee0001",
		"CXL_C1EE0001_SHARED=0", "CXL_C1EE0001_SIZE=536870912", "CXL_LEASE=1a2b3c4d"}
	if got := ExecEnv(dax, "1a2b3c4d", 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("dax exec env\n%v\nwant\n%v", got, want)
	}
	// without an fd the device path stays
	want = []string{"CXL_C1EE0001_DAX=/dev/dax0.0", "CXL_C1EE0001_MODE=dax", "CXL_C1EE0001_READONLY=0", "CXL_C1EE0001_SERIAL=0xc1ee0001",
		"CXL_C1EE0001_SHARED=0", "CXL_C1EE0001_SIZE=536870912", "CXL_LEASE=1a2b3c4d"}
	if got := ExecEnv(dax, "1a2b3c4d", -1); !reflect.DeepEqual(got, want) {
		t.Fatalf("dax exec env without fd\n%v\nwant\n%v", got, want)
	}
	numa := Env(Memory{Key: Key(0xc100e2e1, 1), Serial: "0xc100e2e1", Mode: ModeNUMA, Size: 134217728, Node: 2})
	want = []string{"CXL_C100E2E1_MODE=numa", "CXL_C100E2E1_NODE=2", "CXL_C100E2E1_SERIAL=0xc100e2e1", "CXL_C100E2E1_SHARED=0",
		"CXL_C100E2E1_SIZE=134217728", "CXL_LEASE=00000001"}
	if got := ExecEnv(numa, "00000001", -1); !reflect.DeepEqual(got, want) {
		t.Fatalf("numa exec env\n%v\nwant\n%v", got, want)
	}
	if got := EnvList(map[string]string{"B": "2", "A": "1"}); !reflect.DeepEqual(got, []string{"A=1", "B=2"}) {
		t.Fatalf("env list %v", got)
	}
}

func TestSerials(t *testing.T) {
	for _, c := range []struct {
		in  string
		v   uint64
		err bool
	}{
		{"0xc100e2e1", 0xc100e2e1, false},
		{"0XC100E2E1", 0xc100e2e1, false},
		{"3238060769", 0xc100e2e1, false},
		{"010", 10, false},
		{"0x", 0, true},
		{"c100e2e1", 0, true},
		{"", 0, true},
		{"-1", 0, true},
	} {
		v, err := ParseSerial(c.in)
		if (err != nil) != c.err || v != c.v {
			t.Errorf("ParseSerial(%q) = %v, %v", c.in, v, err)
		}
		if err != nil && err.Error() != `invalid serial "`+c.in+`"` {
			t.Errorf("ParseSerial(%q) error %v", c.in, err)
		}
	}
	if s := FormatSerial(0xc100e2e1); s != "0xc100e2e1" {
		t.Fatalf("FormatSerial %s", s)
	}
}

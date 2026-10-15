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

package device

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	"sigs.k8s.io/yaml"
)

// TestDeviceClassesMatchConstants checks that deployments/cxl/device-classes.yaml
// has the six classes of the Class* constants, each selecting the driver,
// type and sharing that the constant stands for, and no config.
func TestDeviceClassesMatchConstants(t *testing.T) {
	data, err := os.ReadFile("../../../deployments/cxl/device-classes.yaml")
	if err != nil {
		t.Fatal(err)
	}
	classes := map[string]*resourcev1.DeviceClass{}
	for _, doc := range strings.Split(string(data), "\n---\n") {
		c := &resourcev1.DeviceClass{}
		if err := yaml.UnmarshalStrict([]byte(doc), c); err != nil {
			t.Fatalf("%v in\n%s", err, doc)
		}
		if c.Kind != "DeviceClass" || c.APIVersion != "resource.k8s.io/v1" {
			t.Fatalf("not a DeviceClass: %s %s", c.APIVersion, c.Kind)
		}
		classes[c.Name] = c
	}
	selects := func(driver, typ string) string {
		return fmt.Sprintf(`device.driver == %q && device.attributes[%q].%s == %q`, driver, driver, AttrType, typ)
	}
	want := map[string]string{
		ClassLocal:      selects(DriverName, DeviceTypeMemdev),
		ClassPool:       selects(PoolDriverName, DeviceTypeMemdev) + fmt.Sprintf(` && device.attributes[%q].%s == false`, PoolDriverName, AttrShared),
		ClassPoolShared: selects(PoolDriverName, DeviceTypeMemdev) + fmt.Sprintf(` && device.attributes[%q].%s == true`, PoolDriverName, AttrShared),
		ClassRegion:     selects(DriverName, DeviceTypeRegion),
		ClassDRAM:       selects(DriverName, DeviceTypeDRAM),
		ClassLease:      selects(DriverName, DeviceTypeLease),
	}
	if len(classes) != len(want) {
		names := make([]string, 0, len(classes))
		for n := range classes {
			names = append(names, n)
		}
		slices.Sort(names)
		t.Fatalf("classes %v, expected the %d classes of the constants", names, len(want))
	}
	for name, expr := range want {
		c := classes[name]
		switch {
		case c == nil:
			t.Errorf("no class %s", name)
		case len(c.Spec.Selectors) != 1 || c.Spec.Selectors[0].CEL == nil || c.Spec.Selectors[0].CEL.Expression != expr:
			t.Errorf("class %s selectors %+v, expected %s", name, c.Spec.Selectors, expr)
		case len(c.Spec.Config) != 0 || c.Spec.ExtendedResourceName != nil:
			t.Errorf("class %s has a config or an extended resource: %+v", name, c.Spec)
		}
	}
}

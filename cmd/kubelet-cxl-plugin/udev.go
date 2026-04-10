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
	"bufio"
	"encoding/json"
	"os"
	"syscall"
	"time"

	"github.com/containers/nri-plugins/pkg/udev"
	"k8s.io/klog/v2"
)

// UdevEventWatcher monitors udev events for NUMA node changes and
// delivers debounced "stable" notifications. It watches for node
// add/remove events and waits for a quiet period (no new events)
// before signaling that sysfs has stabilized and a rescan is
// warranted.
type UdevEventWatcher struct {
	monitor        *udev.Monitor
	stableDuration time.Duration
	rawEvents      chan *udev.Event
	stable         chan struct{}
	stopCh         chan struct{}
}

// NewUdevEventWatcher creates a watcher that monitors udev events
// for NUMA node additions and removals. The stableDuration parameter
// controls how long the watcher waits after the last event before
// signaling stability.
func NewUdevEventWatcher(stableDuration time.Duration) (*UdevEventWatcher, error) {
	m, err := udev.NewMonitor(
		udev.WithFilters(
			map[string]string{
				udev.PropertySubsystem: "node",
				udev.PropertyAction:    "add",
			},
			map[string]string{
				udev.PropertySubsystem: "node",
				udev.PropertyAction:    "remove",
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return &UdevEventWatcher{
		monitor:        m,
		stableDuration: stableDuration,
		rawEvents:      make(chan *udev.Event, 64),
		stable:         make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}, nil
}

// Start begins monitoring udev events and delivering debounced
// stable notifications. If fifoPath is non-empty, it also reads
// JSON-encoded fake events from that named pipe (created if it
// does not exist).
func (w *UdevEventWatcher) Start(fifoPath string) error {
	w.monitor.Start(w.rawEvents)
	go w.debounce()

	if fifoPath != "" {
		if err := ensureFifo(fifoPath); err != nil {
			return err
		}
		go w.fifoReader(fifoPath)
		klog.Infof("UdevEventWatcher: reading fake events from FIFO %s", fifoPath)
	}

	klog.Infof("UdevEventWatcher: started (stableDuration=%v)", w.stableDuration)
	return nil
}

// Stop stops the udev monitor and signals all goroutines to exit.
func (w *UdevEventWatcher) Stop() {
	close(w.stopCh)
	w.monitor.Stop() //nolint:errcheck
}

// Events returns a read-only channel that receives a signal each
// time sysfs has stabilized after udev node events.
func (w *UdevEventWatcher) Events() <-chan struct{} {
	return w.stable
}

// debounce reads raw udev events and sends a single notification
// on the stable channel after stableDuration of quiet.
func (w *UdevEventWatcher) debounce() {
	var timer *time.Timer
	var timerCh <-chan time.Time

	for {
		select {
		case evt, ok := <-w.rawEvents:
			if !ok {
				// Monitor closed the channel.
				if timer != nil {
					timer.Stop()
				}
				return
			}
			klog.V(5).Infof("UdevEventWatcher: received event action=%s subsystem=%s devpath=%s",
				evt.Action, evt.Subsystem, evt.Devpath)

			if w.stableDuration == 0 {
				// No debounce: signal immediately.
				select {
				case w.stable <- struct{}{}:
				default:
				}
				continue
			}

			if timer == nil {
				timer = time.NewTimer(w.stableDuration)
				timerCh = timer.C
			} else {
				timer.Reset(w.stableDuration)
			}

		case <-timerCh:
			klog.V(3).Info("UdevEventWatcher: sysfs stabilized, signaling rescan")
			select {
			case w.stable <- struct{}{}:
			default:
			}
			timer = nil
			timerCh = nil

		case <-w.stopCh:
			if timer != nil {
				timer.Stop()
			}
			return
		}
	}
}

// fifoReader reads JSON-encoded udev events from a named pipe,
// one JSON object per line. After EOF (writer closed), it re-opens
// the FIFO to accept the next writer. Stops when stopCh is closed.
func (w *UdevEventWatcher) fifoReader(fifoPath string) {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		// Open blocks until a writer opens the other end.
		f, err := os.OpenFile(fifoPath, os.O_RDONLY, 0)
		if err != nil {
			select {
			case <-w.stopCh:
				return
			default:
			}
			klog.Warningf("UdevEventWatcher: failed to open FIFO %s: %v", fifoPath, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			select {
			case <-w.stopCh:
				f.Close()
				return
			default:
			}

			line := scanner.Text()
			if line == "" {
				continue
			}

			evt, err := parseFifoEvent(line)
			if err != nil {
				klog.Warningf("UdevEventWatcher: ignoring malformed FIFO event: %v", err)
				continue
			}

			klog.V(3).Infof("UdevEventWatcher: injected fake event action=%s subsystem=%s devpath=%s",
				evt.Action, evt.Subsystem, evt.Devpath)

			select {
			case w.rawEvents <- evt:
			case <-w.stopCh:
				f.Close()
				return
			}
		}

		f.Close()
		// EOF: writer closed the pipe. Re-open to accept the next writer.
	}
}

// parseFifoEvent parses a JSON line into a udev.Event. The JSON
// object keys and values become the event's Properties map.
func parseFifoEvent(line string) (*udev.Event, error) {
	props := map[string]string{}
	if err := json.Unmarshal([]byte(line), &props); err != nil {
		return nil, err
	}

	return &udev.Event{
		Action:     props[udev.PropertyAction],
		Subsystem:  props[udev.PropertySubsystem],
		Devpath:    props[udev.PropertyDevpath],
		Seqnum:     props[udev.PropertySeqnum],
		Properties: props,
	}, nil
}

// ensureFifo creates a named pipe at path if it does not already exist.
func ensureFifo(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if info.Mode()&os.ModeNamedPipe != 0 {
			return nil
		}
		return os.ErrExist
	}
	if !os.IsNotExist(err) {
		return err
	}
	return syscall.Mkfifo(path, 0666)
}

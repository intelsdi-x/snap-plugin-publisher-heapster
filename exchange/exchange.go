/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package  exchange groups together pieces of data shared between publisher
//components and some common configuration.
package exchange

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	cadv "github.com/google/cadvisor/info/v1"
)

const (
	// StubIncomingDockerData enables generating stub data for development-tests
	StubIncomingDockerData = true
)

type StatsRequest struct {
	// The name of the container for which to request stats.
	// Default: /
	ContainerName string `json:"containerName,omitempty"`

	// Max number of stats to return.
	// If start and end time are specified this limit is ignored.
	// Default: 60
	NumStats int `json:"num_stats,omitempty"`

	// Start time for which to query information.
	// If omitted, the beginning of time is assumed.
	Start time.Time `json:"start,omitempty"`

	// End time for which to query information.
	// If omitted, current time is assumed.
	End time.Time `json:"end,omitempty"`

	// Whether to also include information from subcontainers.
	// Default: false.
	Subcontainers bool `json:"subcontainers,omitempty"`
}

// Type MetricMemory constitutes a metric storage shared between processor
//and server parts of the plugin.
//MetricMemory is equipped with RW lock that should be obtained separately for
//read and update operations.
type MetricMemory struct {
	sync.RWMutex
	ContainerMap   map[string]*cadv.ContainerInfo
	PendingMetrics map[string]map[string][]cadv.MetricVal
}

// Type SystemConfig contains all configuration items extracted from plugin's
//config in task manifest.
type SystemConfig struct {
	// Field  StatsDepth limits the maximum number of stats that should be
	//buffered for a container. Disabled if zero. Evaluated in combination
	//with  StatsSpan.
	StatsDepth int
	// Field  StatsSpan limits the maximum time span of stats that should
	//be buffered for a container. Stats will be limited by this limit and
	//the value of  StatsDepth.
	StatsSpan   time.Duration
	TstampDelta time.Duration
	ServerAddr  string
	ServerPort  int
	// Field  VerboseAt holds a list of logger name prefixes (field 'at')
	//that should be set to verbose (Debug level).
	VerboseAt []string
	// Field  SilentAt holds a list of logger name prefixes (field 'at')
	//that should be set to silent (Warning level).
	SilentAt []string
	// Field  MuteAt holds a list of logger name prefixes (field 'at')
	//that should be set to mute (Error level).
	MuteAt []string
}

// Type LoggerControlType is a poor man's approach to hierarchical control
//of application verbosity. Every logger has to be wired with call to
// WireLogger() then verbosity may be selectively turned on with calls to
// SetLevel().
type LoggerControlType struct {
	sync.Mutex
	loggers  []*logrus.Entry
	updaters []func(log *logrus.Entry)
}

var (
	LoggerControl = &LoggerControlType{}
)

func NewMetricMemory() *MetricMemory {
	return &MetricMemory{
		ContainerMap:   map[string]*cadv.ContainerInfo{},
		PendingMetrics: map[string]map[string][]cadv.MetricVal{},
	}
}

func NewSystemConfig() *SystemConfig {
	return &SystemConfig{}
}

// Function WireLogger() registers logger for verbosity control. Logger
//to be wired should have data field 'at' set to some sort of qualified name.
//If some levels were already turned on with call to SetLevel() then all
//rules will be evaluated on new logger.
func (l *LoggerControlType) WireLogger(log *logrus.Entry) {
	l.Lock()
	defer l.Unlock()
	l.loggers = append(l.loggers, log)
	l.updateLogger(log)
}

// Function SetLevel() registers and runs a rule to control wired loggers.
//Every logger that matches the prefix in  logAt parameter will have
//level set to given one. Every new wired logger will also have this rule
//evaluated.
func (l *LoggerControlType) SetLevel(logAt string, level logrus.Level) {
	l.Lock()
	defer l.Unlock()
	l.updaters = append(l.updaters, func(log *logrus.Entry) {
		if ckLogAt, haveLogAt := log.Data["at"]; !haveLogAt {
			return
		} else {
			if strings.HasPrefix(fmt.Sprint(ckLogAt), logAt) {
				log.Logger.Level = level
			}
		}
	})
	for _, logger := range l.loggers {
		l.updateLogger(logger)
	}
}

// Run all available updaters against given logger instance.
func (l *LoggerControlType) updateLogger(log *logrus.Entry) {
	for _, updater := range l.updaters {
		updater(log)
	}
}

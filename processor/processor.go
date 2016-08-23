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

// Package  processor contains all routines performing processing on
//incoming metrics.
package processor

import (
	"math/rand"
	"path/filepath"
	"time"

	"sync"

	"github.com/Sirupsen/logrus"
	cadv "github.com/google/cadvisor/info/v1"
	"github.com/intelsdi-x/snap-plugin-publisher-heapster/exchange"
	"github.com/intelsdi-x/snap-plugin-publisher-heapster/util"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/satori/go.uuid"
)

const (
	dockerMetricPrefix = "/intel/linux/docker"
)

// Type  ProcessorInstance wires together all data elements required to perform
//metrics processing.
type ProcessorInstance struct {
	config *exchange.SystemConfig
	memory *exchange.MetricMemory
	stats  *processorStats
}

// Type  processorStats holds diagnostic indicators for monitoring load on
//processor part of plugin.
//This type is equipped with RW lock that should be obtained separately for
//read and update operations.
type processorStats struct {
	sync.RWMutex
	MetricsRxTotal       int `json:"metrics_received_total"`
	MetricsRxLast        int `json:"metrics_received_last"`
	ContainersRxLast     int `json:"containers_received_last"`
	ContainersRxMax      int `json:"containers_received_max"`
	CustomMetricsRxTotal int `json:"custom_metric_values_received_total"`
	CustomMetricsRxLast  int `json:"custom_metric_values_received_last"`
	CustomMetricsDdTotal int `json:"custom_metric_values_discarded_total"`
	CustomMetricsDdLast  int `json:"custom_metric_values_discarded_last"`
	CustomSpecsRxTotal   int `json:"custom_metric_specs_received_total"`
	CustomSpecsRxLast    int `json:"custom_metric_specs_received_last"`
}

// Type  processingContext holds data required for a processing run on a single
//batch of metrics.
type processingContext struct {
	*ProcessorInstance
	stats_containersPcsdMap map[string]bool
	// number of custom metrics (values) that were stored in exposed container stats
	stats_numCustomMetricsRx int
	// number of custom metric specs that were added to containers
	stats_numCustomSpecsRx int
	// number of custom metric values that were discarded for being too old
	stats_numStaleMetricsDd int
}

var (
	log *logrus.Entry
)

func init() {
	var l = logrus.New()
	log = l.WithField("at", "/processor/main")
	exchange.LoggerControl.WireLogger(log)
}

func NewProcessor(config *exchange.SystemConfig, memory *exchange.MetricMemory) (*ProcessorInstance, error) {
	processor := ProcessorInstance{
		config: config,
		memory: memory,
		stats:  &processorStats{},
	}
	return &processor, nil
}

// Function  ProcessMetrics initiates a processing run on a batch of metrics.
//This function engages following locks:
//- Lock on metric memory (ProcessorInstance.memory),
//- Lock on processor stats (ProcessorInstance.stats).
func (p *ProcessorInstance) ProcessMetrics(metrics []plugin.MetricType) {
	p.memory.Lock()
	defer p.memory.Unlock()
	p.stats.Lock()
	defer p.stats.Unlock()

	ctx := processingContext{
		ProcessorInstance:       p,
		stats_containersPcsdMap: map[string]bool{},
	}
	ctx.processMetrics(metrics)
}

// Function  DeliverStatus provides a data structure reflecting state of
//processor part for diagnostic purposes.
func (p *ProcessorInstance) DeliverStatus() interface{} {
	p.stats.RLock()
	defer p.stats.RUnlock()
	statsSnapshot := *p.stats
	return statsSnapshot
}

func (p *processingContext) processMetrics(metrics []plugin.MetricType) {
	mtree := util.RebuildObjectFromMetrics(metrics,
		func(path []string, m plugin.MetricType) interface{} {
			return m
		})
	var containerPaths []string
	if dockerTree, err := util.NewObjWalker(mtree).Seek(dockerMetricPrefix); err != util.NotFound {
		// remove docker metrics from tree, remaining ones are custom metrics
		outerTree, _ := util.NewObjWalker(mtree).Seek(filepath.Dir(dockerMetricPrefix))
		delete(outerTree.(map[string]interface{}), dockerMetricPrefix)
		util.PruneEmptySubtrees(mtree)
		containerPaths = p.ingestDockerMetrics(dockerTree.(map[string]interface{}))
	}
	p.ingestCustomMetrics(mtree)
	if len(containerPaths) > 0 {
		for _, containerPath := range containerPaths {
			p.mergeCustomMetricsFor(containerPath)
			p.discardTooOldCustomValuesFor(containerPath)
		}
	}
	// update diagnostic info
	p.stats.MetricsRxLast = len(metrics)
	p.stats.MetricsRxTotal += len(metrics)
	if len(p.stats_containersPcsdMap) > p.stats.ContainersRxMax {
		p.stats.ContainersRxMax = len(p.stats_containersPcsdMap)
	}
	p.stats.ContainersRxLast = len(p.stats_containersPcsdMap)
	p.stats.CustomMetricsRxLast = p.stats_numCustomMetricsRx
	p.stats.CustomMetricsRxTotal += p.stats_numCustomMetricsRx
	p.stats.CustomSpecsRxLast = p.stats_numCustomSpecsRx
	p.stats.CustomSpecsRxTotal += p.stats_numCustomSpecsRx
	p.stats.CustomMetricsDdLast = p.stats_numStaleMetricsDd
	p.stats.CustomMetricsDdTotal += p.stats_numStaleMetricsDd
}

// Function  ingestDockerMetrics processes a tree of metrics generated by
//docker collector plugin.
func (p *processingContext) ingestDockerMetrics(mtree map[string]interface{}) (containerPaths []string) {
	makeDummyContainerInfo := func(id, name string) *cadv.ContainerInfo {
		log.Warn("Stub! Method needs to be removed")
		makeImageNameIfNeeded := func() string {
			if id != "/" {
				return uuid.NewV4().String()
			} else {
				return ""
			}
		}
		i := cadv.ContainerInfo{
			ContainerReference: cadv.ContainerReference{
				Id:   id,
				Name: name},
			Spec: cadv.ContainerSpec{
				CreationTime: time.Now(),
				Labels:       map[string]string{},
				Envs:         map[string]string{},
				HasCpu:       true,
				Cpu: cadv.CpuSpec{
					Limit:    uint64(rand.Int63()),
					MaxLimit: uint64(rand.Int63()),
					Mask:     uuid.NewV4().String(),
					Quota:    uint64(rand.Int63()),
					Period:   uint64(rand.Int63()),
				},
				Image: makeImageNameIfNeeded(),
			}}
		return &i
	}
	//dummy root container - inject for development tests
	if exchange.StubIncomingDockerData {
		mtree["root"] = map[string]interface{}{}
	}
	for dockerID, dockerMetrics := range mtree {
		id := dockerID
		path := dockerID
		p.stats_containersPcsdMap[path] = true
		if id == "root" {
			id = "/'"
			path = "/"
		}
		if _, haveContainer := p.memory.ContainerMap[path]; !haveContainer {
			log.WithField("id", id).Debug("building info structures for new container")
			var container *cadv.ContainerInfo
			if exchange.StubIncomingDockerData {
				container = makeDummyContainerInfo(id, path)
			} else {
				log.Warn("Real data not yet supported for  ContainerInfo")
			}
			p.memory.ContainerMap[path] = container
		}
		container := p.memory.ContainerMap[path]
		p.updateContainerStats(container, dockerMetrics.(map[string]interface{}))
		containerPaths = append(containerPaths, path)
	}
	return containerPaths
}

// Function  updateContainerStats fills  Stats structure of  ContainerInfo
//with values extracted from container metrics.
func (p *processingContext) updateContainerStats(container *cadv.ContainerInfo, metrics map[string]interface{}) {
	log.Warn("Real data not yet supported for  ContainerStats")
	stats := cadv.ContainerStats{}
	stats.Timestamp = time.Now()
	stats.Cpu.LoadAverage = rand.Int31() % 1024
	stats.Cpu.Usage.System = uint64(rand.Int63())
	stats.Cpu.Usage.User = uint64(rand.Int63())
	stats.Cpu.Usage.Total = stats.Cpu.Usage.User + stats.Cpu.Usage.System
	p.makeRoomForStats(&container.Stats, &stats)
	container.Stats = append(container.Stats, &stats)
}

// Function  makeRoomForStats performs filtering and truncation on list of
//container stats so that incoming  Stats element fits within configured range
//of stats (stats_depth and stats_span).
func (p *ProcessorInstance) makeRoomForStats(destList *[]*cadv.ContainerStats, stats *cadv.ContainerStats) {
	validOfs := 0
	statsList := *destList
	if p.config.StatsDepth > 0 && len(statsList) == p.config.StatsDepth {
		validOfs++
	}
	if p.config.StatsSpan <= 0 {
		if validOfs > 0 {
			statsList = statsList[:copy(statsList, statsList[validOfs:])]
			*destList = statsList
		}
		return
	}
	nuStamp := stats.Timestamp
	for validOfs < len(statsList) {
		ckStamp := statsList[validOfs].Timestamp
		span := nuStamp.Sub(ckStamp)
		if span <= p.config.StatsSpan {
			break
		}
		validOfs++
	}
	if validOfs > 0 {
		statsList = statsList[:copy(statsList, statsList[validOfs:])]
		*destList = statsList
	}
}

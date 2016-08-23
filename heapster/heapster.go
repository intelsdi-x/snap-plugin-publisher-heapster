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

package heapster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"strings"

	"github.com/intelsdi-x/snap-plugin-publisher-heapster/exchange"
	_ "github.com/intelsdi-x/snap-plugin-publisher-heapster/exchange"
	"github.com/intelsdi-x/snap-plugin-publisher-heapster/processor"
	"github.com/intelsdi-x/snap-plugin-publisher-heapster/server"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	PluginName    = "heapster"
	PluginVersion = 115
	PluginType    = plugin.PublisherPluginType
)

const (
	defStatsDepth     = 10
	defServerAddr     = ""
	defServerPort     = 8777
	defStatsSpanStr   = "10m"
	defStatsSpan      = 10 * time.Minute
	defTstampDeltaStr = "0"
	defTstampDelta    = 0
	cfgStatsDepth     = "stats_depth"
	cfgServerAddr     = "server_addr"
	cfgServerPort     = "server_port"
	cfgStatsSpan      = "stats_span"
	cfgTstampDelta    = "timestamp_delta"
	cfgVerboseAt      = "verbose_at"
	cfgSilentAt       = "silent_at"
	cfgMuteAt         = "mute_at"
)

type instance struct {
	initOnce     *sync.Once
	config       *exchange.SystemConfig
	metricMemory *exchange.MetricMemory
	processor    *processor.ProcessorInstance
}

type publishContext struct {
	config    *exchange.SystemConfig
	processor *processor.ProcessorInstance
}

type ConfigMap map[string]ctypes.ConfigValue

var (
	log *logrus.Entry
)

func init() {
	gob.Register(map[string]float64{})
	log = logrus.New().WithField("at", "/publisher")
	exchange.LoggerControl.WireLogger(log)
}

func NewPublisher() *instance {
	return &instance{initOnce: &sync.Once{},
		config:       exchange.NewSystemConfig(),
		metricMemory: exchange.NewMetricMemory()}
}

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(PluginName, PluginVersion, PluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType}, plugin.ConcurrencyCount(99))
}

func newPublishContext(config *exchange.SystemConfig, processor *processor.ProcessorInstance) *publishContext {
	return &publishContext{
		config:    config,
		processor: processor,
	}
}

func (p *instance) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	if initErr := p.ensureInitialized(ConfigMap(config)); initErr != nil {
		log.Errorf("Publisher failed to initialize, error=%v\n", initErr)
		return initErr
	}
	ctx := newPublishContext(p.config, p.processor)

	var mts []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&mts); err != nil {
			log.Printf("Error decoding: error=%v content=%v", err, content)
			return fmt.Errorf("Error decoding %v", err)
		}
	default:
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	return ctx.publish(mts)
}

func (p *instance) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	pn := cpolicy.NewPolicyNode()
	rule1, _ := cpolicy.NewStringRule(cfgServerAddr, false, defServerAddr)
	rule2, _ := cpolicy.NewIntegerRule(cfgServerPort, false, defServerPort)
	rule3, _ := cpolicy.NewIntegerRule(cfgStatsDepth, false, defStatsDepth)
	rule4, _ := cpolicy.NewStringRule(cfgStatsSpan, false, defStatsSpanStr)
	rule5, _ := cpolicy.NewStringRule(cfgTstampDelta, false, defTstampDeltaStr)
	rule6, _ := cpolicy.NewStringRule(cfgVerboseAt, false, "")
	rule7, _ := cpolicy.NewStringRule(cfgSilentAt, false, "")
	rule8, _ := cpolicy.NewStringRule(cfgMuteAt, false, "")
	pn.Add(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8)
	cp.Add([]string{}, pn)
	return cp, nil
}

func (p *instance) ensureInitialized(configMap ConfigMap) error {
	var serr error
	p.initOnce.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Caught an error: %s", r)
			}
		}()
		if serr = initSystemConfig(p.config, configMap); serr != nil {
			return
		}
		if p.processor, serr = processor.NewProcessor(p.config, p.metricMemory); serr != nil {
			return
		}
		var serverCtx *server.ServerContext
		if serverCtx, serr = server.InitServer(p.config, p.metricMemory); serr != nil {
			return
		}
		if serr = serverCtx.AddStatusPublisher("processor", p.processor.DeliverStatus); serr != nil {
			return
		}
		if serr = serverCtx.Start(); serr != nil {
			return
		}
	})
	return serr
}

func (ctx *publishContext) publish(metrics []plugin.MetricType) error {
	log.WithField("num_metrics", len(metrics)).Debug("received metrics to process")
	ctx.processor.ProcessMetrics(metrics)
	return nil
}

func (m ConfigMap) GetInt(key string, defValue int) int {
	if value, gotIt := m[key]; gotIt {
		return value.(ctypes.ConfigValueInt).Value
	} else {
		return defValue
	}
}

func (m ConfigMap) GetStr(key string, defValue string) string {
	if value, gotIt := m[key]; gotIt {
		return value.(ctypes.ConfigValueStr).Value
	} else {
		return defValue
	}
}

func initSystemConfig(systemConfig *exchange.SystemConfig, configMap ConfigMap) error {
	systemConfig.StatsDepth = configMap.GetInt(cfgStatsDepth, defStatsDepth)
	systemConfig.ServerAddr = configMap.GetStr(cfgServerAddr, defServerAddr)
	systemConfig.ServerPort = configMap.GetInt(cfgServerPort, defServerPort)
	systemConfig.VerboseAt = strings.Split(configMap.GetStr(cfgVerboseAt, ""), " ")
	systemConfig.SilentAt = strings.Split(configMap.GetStr(cfgSilentAt, ""), " ")
	systemConfig.MuteAt = strings.Split(configMap.GetStr(cfgMuteAt, ""), " ")
	for _, verboseAt := range systemConfig.VerboseAt {
		exchange.LoggerControl.SetLevel(verboseAt, logrus.DebugLevel)
	}
	for _, silentAt := range systemConfig.SilentAt {
		exchange.LoggerControl.SetLevel(silentAt, logrus.WarnLevel)
	}
	for _, muteAt := range systemConfig.MuteAt {
		exchange.LoggerControl.SetLevel(muteAt, logrus.ErrorLevel)
	}
	statsSpanStr := configMap.GetStr(cfgStatsSpan, defStatsSpanStr)
	if statsSpan, err := time.ParseDuration(statsSpanStr); err != nil {
		systemConfig.StatsSpan = defStatsSpan
	} else {
		systemConfig.StatsSpan = statsSpan
	}
	tstampDeltaStr := configMap.GetStr(cfgTstampDelta, defTstampDeltaStr)
	tstampDelta, err := time.ParseDuration(tstampDeltaStr)
	if err != nil {
		systemConfig.TstampDelta = defTstampDelta
	} else {
		systemConfig.TstampDelta = tstampDelta
	}
	return nil
}

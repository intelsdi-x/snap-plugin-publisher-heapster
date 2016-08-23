/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"
	"time"

	cadv "github.com/google/cadvisor/info/v1"

	"github.com/Sirupsen/logrus"

	"github.com/gorilla/mux"

	"github.com/intelsdi-x/snap-plugin-publisher-heapster/exchange"
)

var (
	log *logrus.Entry
)

// Type  StatusPublisherFunc describes a function that intends to publish
//diagnostic status available below  /_status/ branch.
//Status publisher should return a simple object valid for JSON marshalling.
type StatusPublisherFunc func() interface{}

// Type  ServerContext wires data structures and configuration necessary for
//managing a stats server.
type ServerContext struct {
	// Field  config refers to plugin's config.
	config *exchange.SystemConfig
	// Field  memory refers to stored metrics available for report.
	memory *exchange.MetricMemory
	// Field  stats refers to internal diagnostic stats of server part.
	stats  *serverStats
	router *mux.Router
}

type serverStats struct {
	sync.RWMutex
	StatsTxMax   int `json:"stats_tx_max"`
	StatsTxTotal int `json:"stats_tx_total"`
	StatsTxLast  int `json:"stats_tx_last"`
	StatsDdMax   int `json:"stats_dd_max"`
	StatsDdTotal int `json:"stats_dd_total"`
	StatsDdLast  int `json:"stats_dd_last"`
}

// Type  statsListType is a wrapper around slice of  ContainerStats to
//facilitate stats sorting.
type statsListType []*cadv.ContainerStats

func (s statsListType) Len() int {
	return len(s)
}

func (s statsListType) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s statsListType) Less(i, j int) bool {
	l, r := s[i], s[j]
	return !l.Timestamp.Before(r.Timestamp)
}

func init() {
	var l = logrus.New()
	log = l.WithField("at", "/server")
	exchange.LoggerControl.WireLogger(log)
}

func newServerContext(config *exchange.SystemConfig, memory *exchange.MetricMemory) ServerContext {
	ctx := ServerContext{
		config: config,
		memory: memory,
		stats:  &serverStats{},
	}
	return ctx
}

// Function  InitServer builds a server instance.
func InitServer(config *exchange.SystemConfig, memory *exchange.MetricMemory) (*ServerContext, error) {
	server := newServerContext(config, memory)
	if err := server.setup(); err != nil {
		log.WithField("error", err).Error("Server setup failed")
		return nil, err
	}
	return &server, nil
}

// Function  AddStatusPublisher registers a function indented to report
//diagnostic status for some part of the plugin.
//Function will be invoked in response to request for /_status/[name].
func (s *ServerContext) AddStatusPublisher(name string, statusPublisher StatusPublisherFunc) error {
	s.router.Methods("GET").Path(fmt.Sprintf("/_status/%s", name)).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.serveStatusWrapper(name, statusPublisher, w, r)
	})
	return nil
}

// Function Start starts the server's listening thread (separate goroutine).
func (s *ServerContext) Start() error {
	go func() {
		if err := s.listen(); err != nil {
			log.WithField("error", err).Error("Server routine exited with error")
			return
		}
	}()
	return nil
}

// Function  setup performs wiring of http router for the server part.
func (s *ServerContext) setup() error {
	log.Debug("Setting up http server")
	s.router = mux.NewRouter().StrictSlash(true)
	s.router.Methods("POST").Path("/stats/container/").HandlerFunc(s.containerStats)
	s.AddStatusPublisher("server", func() interface{} {
		s.stats.RLock()
		defer s.stats.RUnlock()
		statsCopy := *s.stats
		return statsCopy
	})
	return nil
}

// Function  listen runs a blocking call to http server's listening function.
func (s *ServerContext) listen() error {
	listenAddr := fmt.Sprintf("%s:%d", s.config.ServerAddr, s.config.ServerPort)
	log.WithField("address", listenAddr).Info("Starting server")
	err := http.ListenAndServe(listenAddr, s.router)
	return err
}

// Function  containerStats delivers diagnostic indicators on work performed by
//server part.
func (s *ServerContext) containerStats(w http.ResponseWriter, r *http.Request) {
	log.Info("/stats/container was invoked")
	buffer := &bytes.Buffer{}

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err == nil {
		err = r.Body.Close()
	}
	if err != nil {
		log.WithField("error", err).Error("Failed to read request body")
		s.reportError(w, err, http.StatusInternalServerError)
		return
	}
	var statsJson map[string]interface{}
	if err := json.Unmarshal(body, &statsJson); err != nil {
		log.WithField("error", err).Error("Failed to decode request")
		s.reportError(w, err, 422)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
		return
	}
	var stats exchange.StatsRequest
	json.Unmarshal(body, &stats)
	if _, gotStart := statsJson["start"]; !gotStart {
		stats.Start = time.Time{}
	}
	if _, gotEnd := statsJson["end"]; !gotEnd {
		stats.End = time.Now()
	}

	res := s.buildStatsResponse(&stats)

	if err := json.NewEncoder(buffer).Encode(res); err != nil {
		log.WithField("error", err).Error("Failed to encode response")
		s.reportError(w, err, http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, buffer); err != nil {
		log.WithField("error", err).Error("Failed to deliver response")
		s.reportError(w, err, http.StatusInternalServerError)
	}
}

// Function  serveStatusWrapper invokes and publishes status object provided by
//statusPublisher function.
func (s *ServerContext) serveStatusWrapper(name string, statusPublisher StatusPublisherFunc, w http.ResponseWriter, r *http.Request) {
	log.WithField("publisher_name", name).Info("serving status from registered publisher")
	statusObject := statusPublisher()
	buffer := &bytes.Buffer{}
	if err := json.NewEncoder(buffer).Encode(statusObject); err != nil {
		log.WithFields(logrus.Fields{"publisher_name": name, "error": err}).Error("failed to encode status from registered publisher")
		s.reportError(w, err, http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, buffer)
}

func (s *ServerContext) reportError(w http.ResponseWriter, err error, statusCode int) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(err); err != nil {
		http.Error(w, err.Error(), statusCode)
		return
	}
}

// Function  copyForUpdate builds a copy of ContainerInfo structure that can be
//processed independently of data in metric memory. Some internal fields are
//copied to avoid overwriting metric memory.
func copyForUpdate(container *cadv.ContainerInfo) *cadv.ContainerInfo {
	res := *container
	res.Stats = append(make([]*cadv.ContainerStats, 0, len(res.Stats)), res.Stats...)
	return &res
}

// Function  buildStatsResponse builds a response to request for container
//stats. This function engages following locks:
//- RLock on metric storage (ServerContext.memory),
//- Lock on server stats (ServerContext.stats).
func (s *ServerContext) buildStatsResponse(request *exchange.StatsRequest) interface{} {
	s.memory.RLock()
	defer s.memory.RUnlock()
	s.stats.Lock()
	defer s.stats.Unlock()
	res := map[string]*cadv.ContainerInfo{}
	stats_statsTx := 0
	stats_statsDd := 0
	for name, info := range s.memory.ContainerMap {
		info2 := copyForUpdate(info)
		sortedStats := statsListType(info2.Stats)
		sort.Sort(sortedStats)
		filteredStats := make(statsListType, 0, len(sortedStats))
		for i, statsItem := range sortedStats {
			if statsItem.Timestamp.Before(request.Start) ||
				statsItem.Timestamp.After(request.End) {
				stats_statsDd++
				continue
			}
			filteredStats = append(filteredStats, statsItem)
			stats_statsTx++
			if request.NumStats > 0 && len(filteredStats) >= request.NumStats {
				stats_statsDd += (len(sortedStats) - i - 1)
				break
			}
		}
		info2.Stats = filteredStats
		res[name] = info2
	}
	// update the statistics
	if stats_statsDd > s.stats.StatsDdMax {
		s.stats.StatsDdMax = stats_statsDd
	}
	s.stats.StatsDdLast = stats_statsDd
	s.stats.StatsDdTotal += stats_statsDd
	if stats_statsTx > s.stats.StatsTxMax {
		s.stats.StatsTxMax = stats_statsTx
	}
	s.stats.StatsTxLast = stats_statsTx
	s.stats.StatsTxTotal += stats_statsTx

	return res
}

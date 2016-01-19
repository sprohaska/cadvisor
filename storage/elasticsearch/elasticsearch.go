// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	info "github.com/google/cadvisor/info/v1"
	storage "github.com/google/cadvisor/storage"

	"gopkg.in/olivere/elastic.v2"
)

func init() {
	storage.RegisterStorageDriver("elasticsearch", new)
}

type elasticStorage struct {
	client      *elastic.Client
	machineName string
	indexName   string
	typeName    string
	lock        sync.Mutex
}

type flatStats struct {
	Timestamp     time.Time       `json:"timestamp"`
	MachineName   string          `json:"machine_name,omitempty"`
	ContainerName string          `json:"container_name,omitempty"`
	Cpu           *info.CpuStats  `json:"cpu,omitempty"`
	TaskStats     *info.LoadStats `json:"tasks,omitempty"`
	// Non-array fields from Network inlined.
	Tcp  *info.TcpStat `json:"tcp,omitempty"`
	Tcp6 *info.TcpStat `json:"tcp6,omitempty"`
	// Fields from info.DiskIoStats inlined.
	IoServiceBytes *info.PerDiskStats `json:"io_service_bytes,omitempty"`
	IoServiced     *info.PerDiskStats `json:"io_serviced,omitempty"`
	IoQueued       *info.PerDiskStats `json:"io_queued,omitempty"`
	Sectors        *info.PerDiskStats `json:"sectors,omitempty"`
	IoServiceTime  *info.PerDiskStats `json:"io_service_time,omitempty"`
	IoWaitTime     *info.PerDiskStats `json:"io_wait_time,omitempty"`
	IoMerged       *info.PerDiskStats `json:"io_merged,omitempty"`
	IoTime         *info.PerDiskStats `json:"io_time,omitempty"`
	// MemoryStats is flat enough; it does not contain arrays.
	Memory *info.MemoryStats `json:"memory,omitempty"`
	// Array elements from Network are flattened into Interface.
	Interface  *info.InterfaceStats `json:"interface,omitempty"`
	Filesystem *info.FsStats        `json:"filesystem,omitempty"`
}

var (
	argElasticHost   = flag.String("storage_driver_es_host", "http://localhost:9200", "ElasticSearch host:port")
	argIndexName     = flag.String("storage_driver_es_index", "", "ElasticSearch index name; leave empty to use \"cadvisor-YYYY.MM.DD\"")
	argTypeName      = flag.String("storage_driver_es_type", "stats", "ElasticSearch type name")
	argEnableSniffer = flag.Bool("storage_driver_es_enable_sniffer", false, "ElasticSearch uses a sniffing process to find all nodes of your cluster by default, automatically")
)

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return newStorage(
		hostname,
		*argIndexName,
		*argTypeName,
		*argElasticHost,
		*argEnableSniffer,
	)
}

func (self *elasticStorage) flatContainerStats(
	ref info.ContainerReference, stats *info.ContainerStats) []*flatStats {
	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}

	base := flatStats{
		Timestamp:     stats.Timestamp,
		MachineName:   self.machineName,
		ContainerName: containerName,
	}
	flats := make([]*flatStats, 0)

	// Base stats.
	{
		f := base
		f.Cpu = &stats.Cpu
		f.Memory = &stats.Memory
		f.TaskStats = &stats.TaskStats
		f.Tcp = &stats.Network.Tcp
		f.Tcp6 = &stats.Network.Tcp6
		flats = append(flats, &f)
	}

	// Fields from info.DiskIoStats.
	for _, s := range stats.DiskIo.IoServiceBytes {
		f := base
		f.IoServiceBytes = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoServiced {
		f := base
		f.IoServiced = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoQueued {
		f := base
		f.IoQueued = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.Sectors {
		f := base
		f.Sectors = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoServiceTime {
		f := base
		f.IoServiceTime = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoWaitTime {
		f := base
		f.IoWaitTime = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoMerged {
		f := base
		f.IoMerged = &s
		flats = append(flats, &f)
	}
	for _, s := range stats.DiskIo.IoTime {
		f := base
		f.IoTime = &s
		flats = append(flats, &f)
	}

	for _, s := range stats.Network.Interfaces {
		f := base
		f.Interface = &s
		flats = append(flats, &f)
	}

	for _, s := range stats.Filesystem {
		f := base
		f.Filesystem = &s
		flats = append(flats, &f)
	}

	return flats
}

func (self *elasticStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		self.lock.Lock()
		defer self.lock.Unlock()
		// Flatten the stats into multiple entries to avoid arrays of
		// objects, which Kibana 4 cannot handle.
		flats := self.flatContainerStats(ref, stats)

		// Use the default index name if the arg is empty.
		indexName := self.indexName
		if len(indexName) == 0 {
			indexName = "cadvisor-" + stats.Timestamp.Format("2006.01.02")
		}

		for _, f := range flats {
			_, err := self.client.Index().
				Index(indexName).
				Type(self.typeName).
				BodyJson(f).
				Do()
			if err != nil {
				// Handle error
				fmt.Printf("failed to write stats to ElasticSearch - %s", err)
				return
			}
		}
	}()
	return nil
}

func (self *elasticStorage) Close() error {
	self.client = nil
	return nil
}

// machineName: A unique identifier to identify the host that current cAdvisor
// instance is running on.
// ElasticHost: The host which runs ElasticSearch.
func newStorage(
	machineName,
	indexName,
	typeName,
	elasticHost string,
	enableSniffer bool,
) (storage.StorageDriver, error) {
	// Obtain a client and connect to the default Elasticsearch installation
	// on 127.0.0.1:9200. Of course you can configure your client to connect
	// to other hosts and configure it in various other ways.
	client, err := elastic.NewClient(
		elastic.SetHealthcheck(true),
		elastic.SetSniff(enableSniffer),
		elastic.SetHealthcheckInterval(30*time.Second),
		elastic.SetURL(elasticHost),
	)
	if err != nil {
		// Handle error
		return nil, fmt.Errorf("failed to create the elasticsearch client - %s", err)
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping().URL(elasticHost).Do()
	if err != nil {
		// Handle error
		return nil, fmt.Errorf("failed to ping the elasticsearch - %s", err)

	}
	fmt.Printf("Elasticsearch returned with code %d and version %s", code, info.Version.Number)

	ret := &elasticStorage{
		client:      client,
		machineName: machineName,
		indexName:   indexName,
		typeName:    typeName,
	}
	return ret, nil
}

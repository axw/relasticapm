// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/elastic/go-hdrhistogram"
	"go.elastic.co/apm/model"
	"go.elastic.co/fastjson"
)

var (
	shell       = getenvDefault("SHELL", "/bin/bash")
	serverURL   = getenvDefault("ELASTIC_APM_SERVER_URL", "http://localhost:8200")
	curlCommand = fmt.Sprintf("curl -f -H 'Content-Type: application/x-ndjson' %s/intake/v2/events --data-binary @-", serverURL)
)

var (
	esURL       = flag.String("es", getenvDefault("ELASTICSEARCH_URL", "http://localhost:9200"), "Elasticsearch URL")
	since       = flag.String("since", "now-1h", "return data from this date/time")
	searchSize  = flag.Int("search_size", 1000, "search size")
	execCommand = flag.String("exec", curlCommand, "command to execute for each stream")
)

func Main(ctx context.Context) error {
	flag.Parse()
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{*esURL}})
	if err != nil {
		return err
	}
	pit, err := openPIT(ctx, es, "apm-*")
	if err != nil {
		return err
	}
	defer closePIT(ctx, es, pit)

	var g errgroup.Group
	events := make(chan event, *searchSize)
	g.Go(func() error { return processEvents(ctx, events) })
	g.Go(func() error {
		defer close(events)
		return searchEvents(ctx, es, pit, events)
	})
	return g.Wait()
}

func processEvents(ctx context.Context, events <-chan event) error {
	var w fastjson.Writer
	durationHist := hdrhistogram.New(0, int64(time.Hour), 2)
	beforeAll := time.Now()
	for event := range events {
		w.Reset()

		w.RawString(`{"metadata":{`)
		w.RawString(`"service":`)
		if err := event.service.MarshalFastJSON(&w); err != nil {
			return err
		}
		w.RawString(`,"system":`)
		if err := event.system.MarshalFastJSON(&w); err != nil {
			return err
		}
		w.RawString("}}\n")

		w.RawString(`{"transaction":`)
		switch {
		case event.tx != nil:
			if err := event.tx.MarshalFastJSON(&w); err != nil {
				return err
			}
		}
		w.RawString("}\n")

		before := time.Now()
		cmd := exec.CommandContext(ctx, shell, "-c", *execCommand)
		cmd.Stdin = bytes.NewReader(w.Bytes())
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return err
		}
		taken := time.Since(before)
		durationHist.RecordValue(int64(taken))
	}
	takenAll := time.Since(beforeAll)
	log.Printf("processed %d events in %s", durationHist.TotalCount(), takenAll)
	log.Printf("- p95 executation time: %s", time.Duration(durationHist.ValueAtQuantile(95)))
	return nil
}

type event struct {
	service *model.Service
	system  *model.System
	tx      *model.Transaction
}

func makeEvent(doc eventDoc, sampleRate float64) event {
	service := &model.Service{
		Name:        doc.Service.Name,
		Version:     doc.Service.Version,
		Environment: doc.Service.Environment,
		Agent: &model.Agent{
			Name:    doc.Agent.Name,
			Version: doc.Agent.Version,
		},
	}
	system := &model.System{
		Architecture: doc.Host.Architecture,
		Hostname:     doc.Host.Hostname,
		Platform:     doc.Host.OS.Platform,
	}
	if doc.Container.ID != "" {
		system.Container = &model.Container{ID: doc.Container.ID}
	}
	if doc.Kubernetes.Pod.Name != "" {
		system.Kubernetes = &model.Kubernetes{Pod: &model.KubernetesPod{Name: doc.Kubernetes.Pod.Name}}
	}
	return event{
		service: service,
		system:  system,
		tx:      makeTransaction(doc, sampleRate),
	}
}

// NOTE(axw) we currently only capture base fields which are present in both sampled
// and non-sampled transactions, used for calculating transaction metrics, and which
// are required fields (e.g. trace ID).
func makeTransaction(doc eventDoc, sampleRate float64) *model.Transaction {
	tx := &model.Transaction{
		Timestamp:  model.Time(doc.Timestamp),
		ID:         makeSpanID(doc.Transaction.ID),
		ParentID:   makeSpanID(doc.Parent.ID),
		TraceID:    makeTraceID(doc.Trace.ID),
		Name:       doc.Transaction.Name,
		Type:       doc.Transaction.Type,
		Duration:   float64(doc.Transaction.Duration.Micros / 1000),
		Result:     doc.Transaction.Result,
		Outcome:    doc.Event.Outcome,
		SampleRate: &sampleRate,
	}
	if !doc.Transaction.Sampled {
		tx.Sampled = &doc.Transaction.Sampled
	}
	return tx
}

func makeTraceID(in string) model.TraceID {
	var out model.TraceID
	if in == "" {
		return out
	}
	n, err := hex.Decode(out[:], []byte(in))
	if err != nil {
		panic(err)
	}
	if n != len(out) {
		panic(fmt.Errorf("invalid trace ID %q", in))
	}
	return out
}

func makeSpanID(in string) model.SpanID {
	var out model.SpanID
	if in == "" {
		return out
	}
	n, err := hex.Decode(out[:], []byte(in))
	if err != nil {
		panic(err)
	}
	if n != len(out) {
		panic(fmt.Errorf("invalid span ID %q", in))
	}
	return out
}

type eventDoc struct {
	Timestamp time.Time `json:"@timestamp"`

	Agent struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"agent"`

	Container struct {
		ID string `json:"id"`
	} `json:"container"`

	Event struct {
		Outcome string `json:"outcome"`
	} `json:"event"`

	Host struct {
		Architecture string `json:"architecture"`
		Hostname     string `json:"hostname"`
		Name         string `json:"name"`
		OS           struct {
			Platform string `json:"platform"`
		} `json:"os"`
	} `json:"host"`

	Kubernetes struct {
		Pod struct {
			Name string `json:"name"`
		} `json:"pod"`
	} `json:"kubernetes"`

	Parent struct {
		ID string `json:"id"`
	} `json:"parent"`

	Service struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Environment string `json:"environment"`
	} `json:"service"`

	Transaction struct {
		Name     string `json:"name"`
		Result   string `json:"result"`
		Type     string `json:"type"`
		ID       string `json:"id"`
		Sampled  bool   `json:"sampled"`
		Duration struct {
			Micros int64 `json:"us"`
		} `json:"duration"`
	} `json:"transaction"`

	Trace struct {
		ID string `json:"id"`
	} `json:"trace"`
}

func searchEvents(ctx context.Context, es *elasticsearch.Client, pit string, out chan<- event) error {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	var body struct {
		PIT            PointInTime              `json:"pit"`
		Size           int                      `json:"size,omitempty"`
		TrackTotalHits bool                     `json:"track_total_hits"`
		Query          interface{}              `json:"query,omitempty"`
		Sort           []map[string]interface{} `json:"sort,omitempty"`
		Aggs           map[string]interface{}   `json:"aggs,omitempty"`
		SearchAfter    []interface{}            `json:"search_after,omitempty"`
	}
	body.PIT.ID = pit
	body.PIT.KeepAlive = "1m"
	body.Size = *searchSize
	body.Sort = []map[string]interface{}{{"@timestamp": "asc"}, {"_shard_doc": "asc"}}
	body.Query = map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": []map[string]interface{}{{
				"range": map[string]interface{}{
					"@timestamp": map[string]interface{}{
						"gte":    *since,
						"lte":    now,
						"format": "epoch_millis",
					},
				},
			}, {
				"terms": map[string]interface{}{
					"processor.event": []string{"transaction"},
				},
			}},
		},
	}
	body.Aggs = map[string]interface{}{
		// Calculate sample rate from the percentage of sampled transaction docs
		// for each service.name/service.environment pair for the search period.
		"services": map[string]interface{}{
			"multi_terms": map[string]interface{}{
				"terms": []map[string]interface{}{
					{"field": "service.name"},
					{"field": "service.environment", "missing": ""},
				},
				"size": 10000,
			},
			"aggs": map[string]interface{}{
				"sampled": map[string]interface{}{
					"terms": map[string]interface{}{
						"field":   "transaction.sampled",
						"size":    2,
						"missing": true,
					},
				},
			},
		},
	}

	type serviceKey struct {
		name        string
		environment string
	}
	sampleRates := make(map[serviceKey]float64)
	for {
		resp, err := es.Search(
			es.Search.WithContext(ctx),
			es.Search.WithBody(esutil.NewJSONReader(body)),
		)
		if err != nil {
			return err
		}
		if resp.IsError() {
			defer resp.Body.Close()
			return makeRespError(resp)
		}

		type searchHit struct {
			Source eventDoc      `json:"_source"`
			Sort   []interface{} `json:"sort"`
		}
		var result struct {
			PointInTimeID string `json:"pit_id"`
			Hits          struct {
				Hits []searchHit `json:"hits"`
			} `json:"hits"`
			Aggregations struct {
				Services struct {
					Buckets []struct {
						Key     []string `json:"key"`
						Sampled struct {
							Buckets []struct {
								Key      int   `json:"key"`
								DocCount int64 `json:"doc_count"`
							} `json:"buckets"`
						} `json:"sampled"`
					} `json:"buckets"`
				} `json:"services"`
			} `json:"aggregations"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return err
		}
		resp.Body.Close()
		if len(result.Hits.Hits) == 0 {
			break
		}
		if body.Aggs != nil {
			body.Aggs = nil
			for _, serviceBucket := range result.Aggregations.Services.Buckets {
				var sampled, unsampled int64
				serviceName := serviceBucket.Key[0]
				serviceEnvironment := serviceBucket.Key[1]
				for _, sampledBucket := range serviceBucket.Sampled.Buckets {
					switch sampledBucket.Key {
					case 0:
						unsampled += sampledBucket.DocCount
					case 1:
						sampled += sampledBucket.DocCount
					}
				}
				total := sampled + unsampled
				if total > 0 {
					sampleRate := float64(sampled) / float64(total)
					sampleRates[serviceKey{
						name:        serviceName,
						environment: serviceEnvironment,
					}] = sampleRate
				}
			}
			log.Println("sample rates:", sampleRates)
		}
		body.PIT.ID = result.PointInTimeID
		for _, hit := range result.Hits.Hits {
			body.SearchAfter = hit.Sort
			if !hit.Source.Transaction.Sampled {
				continue
			}
			sampleRate, ok := sampleRates[serviceKey{
				name:        hit.Source.Service.Name,
				environment: hit.Source.Service.Environment,
			}]
			if !ok {
				return fmt.Errorf(
					"sample rate not found for %s/%s",
					hit.Source.Service.Name, hit.Source.Service.Environment,
				)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- makeEvent(hit.Source, sampleRate):
			}
		}
	}
	return nil
}

func openPIT(ctx context.Context, es *elasticsearch.Client, index string) (string, error) {
	resp, err := es.OpenPointInTime(
		es.OpenPointInTime.WithContext(ctx),
		es.OpenPointInTime.WithIndex("apm-*"),
		es.OpenPointInTime.WithKeepAlive("1m"),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return "", makeRespError(resp)
	}

	var pit PointInTime
	if err := json.NewDecoder(resp.Body).Decode(&pit); err != nil {
		return "", err
	}
	return pit.ID, nil
}

func closePIT(ctx context.Context, es *elasticsearch.Client, id string) error {
	resp, err := es.ClosePointInTime(
		es.ClosePointInTime.WithContext(ctx),
		es.ClosePointInTime.WithBody(esutil.NewJSONReader(PointInTime{ID: id})),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return makeRespError(resp)
	}
	return nil
}

func makeRespError(resp *esapi.Response) error {
	body, _ := ioutil.ReadAll(resp.Body)
	return fmt.Errorf("%s (%s)", resp.Status(), strings.TrimSpace(string(body)))
}

type PointInTime struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive"`
}

func getenvDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		value = defaultValue
	}
	return value
}

func main() {
	if err := Main(context.Background()); err != nil {
		log.Fatal(err)
	}
}

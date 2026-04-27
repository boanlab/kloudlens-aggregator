// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PromCollector exposes Aggregator.Stats() as Prometheus metrics for the
// kloudlens-aggregator binary.
type PromCollector struct {
	agg *Aggregator

	received    *prometheus.Desc
	written     *prometheus.Desc
	dropped     *prometheus.Desc
	errors      *prometheus.Desc
	walAppends  *prometheus.Desc
	walErrors   *prometheus.Desc
	walLastSeq  *prometheus.Desc
	subDrops    *prometheus.Desc
	subscribers *prometheus.Desc
}

// NewPromCollector returns a Collector ready to be handed to
// prometheus.MustRegister. All series values are sampled from agg.Stats()
// on each scrape, so the Collector adds zero background cost.
func NewPromCollector(agg *Aggregator) *PromCollector {
	return &PromCollector{
		agg: agg,
		received: prometheus.NewDesc(
			"kloudlens_aggregator_received_total",
			"Envelopes pulled off upstream agent Subscribe streams since startup.",
			nil, nil,
		),
		written: prometheus.NewDesc(
			"kloudlens_aggregator_written_total",
			"Envelopes successfully NDJSON-encoded to the output sink.",
			nil, nil,
		),
		dropped: prometheus.NewDesc(
			"kloudlens_aggregator_dropped_total",
			"Envelopes dropped because the internal fan-in channel was full.",
			nil, nil,
		),
		errors: prometheus.NewDesc(
			"kloudlens_aggregator_errors_total",
			"Per-stream Subscribe recv errors observed. Reconnects are transparent; a non-zero rate means an agent is flapping.",
			nil, nil,
		),
		walAppends: prometheus.NewDesc(
			"kloudlens_aggregator_wal_appends_total",
			"Envelopes appended to the aggregator WAL.",
			nil, nil,
		),
		walErrors: prometheus.NewDesc(
			"kloudlens_aggregator_wal_errors_total",
			"WAL append failures. NDJSON emit still attempts; rate() > 0 means WAL replay will be lossy.",
			nil, nil,
		),
		walLastSeq: prometheus.NewDesc(
			"kloudlens_aggregator_wal_last_seq",
			"Highest sequence number assigned by the aggregator WAL (0 when no WAL is configured).",
			nil, nil,
		),
		subDrops: prometheus.NewDesc(
			"kloudlens_aggregator_subscriber_dropped_total",
			"Envelopes dropped by the re-export fan-out because a downstream consumer's per-subscription queue was full. Monotonic across subscriber reconnects.",
			nil, nil,
		),
		subscribers: prometheus.NewDesc(
			"kloudlens_aggregator_subscribers_active",
			"Downstream re-export clients currently connected to the aggregator. Correlate with subscriber_dropped_total to distinguish fleet churn from sustained backpressure.",
			nil, nil,
		),
	}
}

func (c *PromCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.received
	ch <- c.written
	ch <- c.dropped
	ch <- c.errors
	ch <- c.walAppends
	ch <- c.walErrors
	ch <- c.walLastSeq
	ch <- c.subDrops
	ch <- c.subscribers
}

func (c *PromCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.agg.Stats()
	ch <- prometheus.MustNewConstMetric(c.received, prometheus.CounterValue, float64(s.Received))
	ch <- prometheus.MustNewConstMetric(c.written, prometheus.CounterValue, float64(s.Written))
	ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, float64(s.Dropped))
	ch <- prometheus.MustNewConstMetric(c.errors, prometheus.CounterValue, float64(s.Errors))
	ch <- prometheus.MustNewConstMetric(c.walAppends, prometheus.CounterValue, float64(s.WALAppends))
	ch <- prometheus.MustNewConstMetric(c.walErrors, prometheus.CounterValue, float64(s.WALErrors))
	ch <- prometheus.MustNewConstMetric(c.walLastSeq, prometheus.GaugeValue, float64(s.WALLastSeq))
	ch <- prometheus.MustNewConstMetric(c.subDrops, prometheus.CounterValue, float64(s.SubscriberDropped))
	ch <- prometheus.MustNewConstMetric(c.subscribers, prometheus.GaugeValue, float64(s.Subscribers))
}

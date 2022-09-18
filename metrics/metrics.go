package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var HttpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_http_requests_total",
}, []string{"host", "action", "method"})
var InvalidHttpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_invalid_http_requests_total",
}, []string{"action", "method"})
var HttpResponses = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_http_responses_total",
}, []string{"host", "action", "method", "statusCode"})
var CacheHits = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_cache_hits_total",
}, []string{"cache"})
var CacheMisses = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_cache_misses_total",
}, []string{"cache"})
var CacheEvictions = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_cache_evictions_total",
}, []string{"cache", "reason"})
var CacheNumItems = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "media_cache_num_items",
}, []string{"cache"})
var CacheNumLiveItems = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "media_cache_num_live_items",
}, []string{"cache"})
var CacheNumBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "media_cache_num_bytes_used",
}, []string{"cache"})
var CacheLiveNumBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "media_cache_num_live_bytes_used",
}, []string{"cache"})
var ThumbnailsGenerated = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_thumbnails_generated_total",
}, []string{"width", "height", "method", "animated", "origin"})
var MediaDownloaded = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_downloaded_total",
}, []string{"origin"})
var UrlPreviewsGenerated = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "media_url_previews_generated_total",
}, []string{"type"})
var URLPreviewHTTPClientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "url_preview_client_request_duration_seconds",
	Help:    "how long the URL preview took for each URL. Only recorded for domains of interest.",
	Buckets: prometheus.ExponentialBucketsRange(0.1, 10, 20),
}, []string{"domain"})
var MediaUploadDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "media_upload_duration_seconds",
	Help:    "how long it takes to upload media to the media store",
	Buckets: prometheus.ExponentialBucketsRange(0.1, 15, 20),
}, []string{"worker"})
var MediaUploadBytes = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "media_upload_bytes_total",
	Help: "how many bytes have been uploaded to the media store",
})

func init() {
	prometheus.MustRegister(HttpRequests)
	prometheus.MustRegister(InvalidHttpRequests)
	prometheus.MustRegister(HttpResponses)
	prometheus.MustRegister(CacheHits)
	prometheus.MustRegister(CacheMisses)
	prometheus.MustRegister(CacheEvictions)
	prometheus.MustRegister(CacheNumItems)
	prometheus.MustRegister(CacheNumLiveItems)
	prometheus.MustRegister(CacheNumBytes)
	prometheus.MustRegister(CacheLiveNumBytes)
	prometheus.MustRegister(ThumbnailsGenerated)
	prometheus.MustRegister(MediaDownloaded)
	prometheus.MustRegister(UrlPreviewsGenerated)
	prometheus.MustRegister(URLPreviewHTTPClientRequestDuration)
	prometheus.MustRegister(MediaUploadDuration)
	prometheus.MustRegister(MediaUploadBytes)
}

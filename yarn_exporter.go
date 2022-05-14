package main

import (
    "config"
    "exporter"
    "log"
    "net/http"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

const RootPageHtml = `<html>
<head><title>Yarn Exporter</title></head>
  <body>
    <h1>Yarn Exporter</h1>
    <p><a href="/metrics">Metrics</a></p>
  </body>
</html>
`

func HandleRootPage(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte(RootPageHtml))
}

func main() {
    c := config.NewConfig()
    e := exporter.NewExporter(c)

    prometheus.MustRegister(e)

    http.Handle("/metrics", promhttp.Handler())
    http.HandleFunc("/", HandleRootPage)
    log.Fatal(http.ListenAndServe(c.Listen, nil))
}

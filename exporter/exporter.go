package exporter

import (
    "collector"
    "config"
    "strconv"
    "time"

    "github.com/prometheus/client_golang/prometheus"
)

const timeFormat = "2006-01-02T15:04:05.000GMT"

var (
    yarnAppAttempt = prometheus.NewDesc(prometheus.BuildFQName("yarn", "app", "attempts"),
        "yarn app attempts",
        []string{"endpoint", "id", "name", "start_time", "end_time", "spark_user", "completed", "app_spark_version",
            "start_time_epoch", "end_time_epoch"},
        nil,
    )
    yarnAppsStatus = prometheus.NewDesc(prometheus.BuildFQName("yarn", "apps", "status"),
        "yarn apps status",
        []string{"endpoint", "status"},
        nil,
    )
    yarnMemoryStatus = prometheus.NewDesc(prometheus.BuildFQName("yarn", "memory", "status"),
        "yarn memory status (mb)",
        []string{"endpoint", "status"},
        nil,
    )
    yarnVCoresStatus = prometheus.NewDesc(prometheus.BuildFQName("yarn", "vcores", "status"),
        "yarn virtual cores status",
        []string{"endpoint", "status"},
        nil,
    )
    yarnContainersStatus = prometheus.NewDesc(prometheus.BuildFQName("yarn", "containers", "status"),
        "yarn containers status",
        []string{"endpoint", "status"},
        nil,
    )
    yarnNodesStatus = prometheus.NewDesc(prometheus.BuildFQName("yarn", "nodes", "status"),
        "yarn nodes status",
        []string{"endpoint", "status"},
        nil,
    )
)

type Exporter struct {
    Config *config.Config
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
    ch <- yarnAppAttempt
    ch <- yarnAppsStatus
    ch <- yarnMemoryStatus
    ch <- yarnVCoresStatus
    ch <- yarnContainersStatus
    ch <- yarnNodesStatus

}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
    scheme := e.Config.YarnEndpointScheme
    host := e.Config.YarnEndpointHost
    clusterMetricsPort := e.Config.YarnEndpointClusterMetricsPort
    clusterMetricsPath := e.Config.YarnEndpointClusterMetricsPath
    appEndpointPort := e.Config.YarnEndpointApplicationPort
    appEndpointPath := e.Config.YarnEndpointApplicationPath

    yarnClusterEndpoint := scheme + host + ":" + clusterMetricsPort + clusterMetricsPath
    yarnAppEndpoint := scheme + host + ":" + appEndpointPort + appEndpointPath

    c := collector.NewCollector()

    // cluster metrics
    cs := c.FetchClusterMetrics(yarnClusterEndpoint)

    appsStatKey := []string{"submitted", "completed", "pending", "running", "failed", "killed"}
    appsStatValue := []float64{cs.AppsCompleted, cs.AppsCompleted, cs.AppsPending, cs.AppsRunning, cs.AppsFailed, cs.AppsKilled}
    for i, value := range appsStatValue {
        ch <- prometheus.MustNewConstMetric(yarnAppsStatus, prometheus.GaugeValue,
            value,
            yarnClusterEndpoint, appsStatKey[i],
        )
    }

    memoryStatKey := []string{"reserved", "available", "allocated", "total"}
    memoryStatValue := []float64{cs.ReservedMB, cs.AvailableMB, cs.AllocatedMB, cs.TotalMB}
    for i, value := range memoryStatValue {
        ch <- prometheus.MustNewConstMetric(yarnMemoryStatus, prometheus.GaugeValue,
            value,
            yarnClusterEndpoint, memoryStatKey[i],
        )
    }

    vCoresStatKey := []string{"reserved", "available", "allocated", "total"}
    vCoresStatValue := []float64{cs.ReservedVirtualCores, cs.AvailableVirtualCores, cs.AllocatedVirtualCores,
        cs.TotalVirtualCores}
    for i, value := range vCoresStatValue {
        ch <- prometheus.MustNewConstMetric(yarnVCoresStatus, prometheus.GaugeValue,
            value,
            yarnClusterEndpoint, vCoresStatKey[i],
        )
    }

    containersStatKey := []string{"reserved", "allocated", "pending"}
    containersStatValue := []float64{cs.ContainersReserved, cs.ContainersAllocated, cs.ContainersPending}

    for i, value := range containersStatValue {
        ch <- prometheus.MustNewConstMetric(yarnContainersStatus, prometheus.GaugeValue,
            value,
            yarnClusterEndpoint, containersStatKey[i],
        )
    }

    nodeStatKey := []string{"total", "lost", "unhealthy", "decommissioning", "decommissioned", "reboot", "active", "shutdown"}
    nodeStatValue := []float64{cs.TotalNodes, cs.LostNodes, cs.UnhealthyNodes, cs.DecommissioningNodes, cs.DecommissionedNodes,
        cs.RebootedNodes, cs.ActiveNodes, cs.ShutdownNodes}

    for i, value := range nodeStatValue {
        ch <- prometheus.MustNewConstMetric(yarnNodesStatus, prometheus.GaugeValue,
            value,
            yarnClusterEndpoint, nodeStatKey[i],
        )
    }

    sevenDaysBefore, _ := time.ParseDuration("-168h")
    minDate := time.Now().Add(sevenDaysBefore).Format(timeFormat)
    apps := c.FetchAppAttempts(yarnAppEndpoint + "?minDate=" + minDate)
    for _, app := range apps {
        if len(app.Attempts) == 0 {
            continue
        }
        attempt := app.Attempts[0]
        // []string{"id", "name", "start_time", "end_time", "spark_user", "completed", "app_spark_version", "start_time_epoch", "end_time_epoch"},
        ch <- prometheus.MustNewConstMetric(yarnAppAttempt, prometheus.GaugeValue,
            attempt.Duration,
            yarnAppEndpoint,
            app.Id,
            app.Name,
            attempt.StartTime,
            attempt.EndTime,
            attempt.SparkUser,
            strconv.FormatBool(attempt.Completed),
            attempt.AppSparkVersion,
            strconv.FormatInt(int64(attempt.StartTimeEpoch), 10),
            strconv.FormatInt(int64(attempt.EndTimeEpoch), 10),
        )
    }
}

func NewExporter(c *config.Config) *Exporter {
    return &Exporter{
        Config: c,
    }
}

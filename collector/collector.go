package collector

import (
    "encoding/json"
    "io/ioutil"
    "log"
    "math"
    "net/http"
    "time"
)

const timeFormat = "2006-01-02 15:04:05"

type YarnCluster struct {
    ClusterMetrics *ClusterMetrics `json:"clusterMetrics"`
}

type ClusterMetrics struct {
    AppsSubmitted         float64 `json:"appsSubmitted"`
    AppsCompleted         float64 `json:"appsCompleted"`
    AppsPending           float64 `json:"appsPending"`
    AppsRunning           float64 `json:"appsRunning"`
    AppsFailed            float64 `json:"appsFailed"`
    AppsKilled            float64 `json:"appsKilled"`
    ReservedMB            float64 `json:"reservedMB"`
    AvailableMB           float64 `json:"availableMB"`
    AllocatedMB           float64 `json:"allocatedMB"`
    ReservedVirtualCores  float64 `json:"reservedVirtualCores"`
    AvailableVirtualCores float64 `json:"availableVirtualCores"`
    AllocatedVirtualCores float64 `json:"allocatedVirtualCores"`
    ContainersAllocated   float64 `json:"containersAllocated"`
    ContainersReserved    float64 `json:"containersReserved"`
    ContainersPending     float64 `json:"containersPending"`
    TotalMB               float64 `json:"totalMB"`
    TotalVirtualCores     float64 `json:"totalVirtualCores"`
    TotalNodes            float64 `json:"totalNodes"`
    LostNodes             float64 `json:"lostNodes"`
    UnhealthyNodes        float64 `json:"unhealthyNodes"`
    DecommissioningNodes  float64 `json:"decommissioningNodes"`
    DecommissionedNodes   float64 `json:"decommissionedNodes"`
    RebootedNodes         float64 `json:"rebootedNodes"`
    ActiveNodes           float64 `json:"activeNodes"`
    ShutdownNodes         float64 `json:"shutdownNodes"`
}

type Application struct {
    Id       string     `json:"id"`
    Name     string     `json:"name"`
    Attempts []*Attempt `json:"attempts"`
}

type Attempt struct {
    StartTime        string  `json:"startTime"`
    EndTime          string  `json:"endTime"`
    LastUpdated      string  `json:"lastUpdated"`
    Duration         float64 `json:"duration"`
    SparkUser        string  `json:"sparkUser"`
    Completed        bool    `json:"completed"`
    AppSparkVersion  string  `json:"appSparkVersion"`
    StartTimeEpoch   int64   `json:"startTimeEpoch"`
    EndTimeEpoch     int64   `json:"endTimeEpoch"`
    LastUpdatedEpoch int64   `json:"lastUpdatedEpoch"`
}

func deepCopy(attempt *Attempt) *Attempt {
    attemptCopy := &Attempt{}
    attemptCopy.StartTime = attempt.StartTime
    attemptCopy.EndTime = attempt.EndTime
    attemptCopy.LastUpdated = attempt.LastUpdated
    attemptCopy.Duration = attempt.Duration
    attemptCopy.SparkUser = attempt.SparkUser
    attemptCopy.Completed = attempt.Completed
    attemptCopy.AppSparkVersion = attempt.AppSparkVersion
    attemptCopy.StartTimeEpoch = attempt.StartTimeEpoch
    attemptCopy.EndTimeEpoch = attempt.EndTimeEpoch
    attemptCopy.LastUpdatedEpoch = attempt.LastUpdatedEpoch
    return attemptCopy
}

func Assert(err error, action string) {
    if err != nil && action == "panic" {
        panic(err)
    } else if err != nil {
        log.Println(err)
    }
}

type Collector struct{}

func (c *Collector) fetch(url string) []byte {
    response, err := http.Get(url)
    Assert(err, "panic")
    defer response.Body.Close()
    body, err := ioutil.ReadAll(response.Body)
    Assert(err, "panic")
    return body
}

func (c *Collector) FetchClusterMetrics(clusterMetricsEndpoint string) *ClusterMetrics {
    body := c.fetch(clusterMetricsEndpoint)

    var yarnCluster YarnCluster
    err := json.Unmarshal(body, &yarnCluster)
    Assert(err, "panic")
    return yarnCluster.ClusterMetrics
}

func (c *Collector) FetchAppAttempts(appEndpoint string) (singleAppAttempts []*Application) {
    body := c.fetch(appEndpoint)

    var apps []Application
    err := json.Unmarshal(body, &apps)
    Assert(err, "panic")
    now := time.Now()
    for _, app := range apps {
        for _, singleApp := range app.Attempts {
            startTime := time.UnixMilli(singleApp.StartTimeEpoch)
            subTime := now.Sub(startTime)
            if int64(subTime)-int64(time.Second*time.Duration(7*24*60*60)) >= 0 {
                continue
            }
            if singleApp.EndTime == "1969-12-31T23:59:59.999GMT" {
                singleApp.EndTime = ""
            }
            if singleApp.Duration == 0 {
                singleApp.Duration = math.Floor(float64(subTime) / 1000 / 1000)
            }
            var appAttempt Application
            appAttempt.Id = app.Id
            appAttempt.Name = app.Name
            appAttempt.Attempts = append(appAttempt.Attempts, deepCopy(singleApp))
            singleAppAttempts = append(singleAppAttempts, &appAttempt)
        }
    }
    return
}

func NewCollector() *Collector {
    return &Collector{}
}

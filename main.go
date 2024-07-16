package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	REFRESH_INTERVAL_SEC = 10
	DAQS_DIR             = "/mnt/ffs/data/daqs"
	MAX_FAIL_COUNT       = 35
	DEFAULT_BIND_IP      = "0.0.0.0"
	DEFAULT_BIND_PORT    = 9980
)

var (
	modulePower = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tigo_module_power",
			Help: "Module power value in W",
		},
		[]string{"name"},
	)
	moduleVolts = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tigo_module_volts",
			Help: "Module volt value in V",
		},
		[]string{"name"},
	)
	moduleRSSI = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tigo_module_rssi",
			Help: "Tigo signal strength value",
		},
		[]string{"name"},
	)
	moduleTemp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tigo_module_temp",
			Help: "Tigo module temperature value in celsius",
		},
		[]string{"name"},
	)
	tigoTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tigo_timestamp",
			Help: "Timestamp of the dataset",
		},
		[]string{"source", "location"},
	)
)

func init() {
	prometheus.MustRegister(modulePower)
	prometheus.MustRegister(moduleVolts)
	prometheus.MustRegister(moduleRSSI)
	prometheus.MustRegister(moduleTemp)
	prometheus.MustRegister(tigoTimestamp)
}

type Config struct {
	TigoDAQSDataDir string `arg:"positional"`
	BindIP          string `arg:"--bind-ip,help:bind ip: default(0.0.0.0)"`
	BindPort        uint16 `arg:"--bind-port,help:bind port: default(9980)"`
	Verbose         bool   `arg:"--verbose,help:verbose output"`
}

func getNewestCSVFile(dataDir string) (string, error) {
	var newestFile string
	var newestModTime time.Time

	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(info.Name()) == ".csv" {
			if info.ModTime().After(newestModTime) {
				newestFile = path
				newestModTime = info.ModTime()
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return newestFile, nil
}

func getFieldValue(field string) (float64, error) {
	if field == "" {
		return 0, fmt.Errorf("empty field")
	}
	return strconv.ParseFloat(field, 64)
}

func updateGauge(gauge *prometheus.GaugeVec, moduleIndex int, value float64, failCount int) {
	label := prometheus.Labels{"name": fmt.Sprintf("A%d", moduleIndex)}
	gauge.With(label).Set(value)
}

func main() {
	var cfg Config
	arg.MustParse(&cfg)

	// Check if TIGODAQSDATADIR is empty and use the default DAQS_DIR if it is
	if cfg.TigoDAQSDataDir == "" {
		cfg.TigoDAQSDataDir = DAQS_DIR
	}

	// Set default values for BindIP and BindPort if not provided
	if cfg.BindIP == "" {
		cfg.BindIP = DEFAULT_BIND_IP
	}
	if cfg.BindPort == 0 {
		cfg.BindPort = DEFAULT_BIND_PORT
	}

	http.Handle("/metrics", promhttp.Handler())

	bindAddress := fmt.Sprintf("%s:%d", cfg.BindIP, cfg.BindPort)
	server := &http.Server{Addr: bindAddress}

	var lastCSVTime time.Time
	failCounterMap := make(map[int]int)
	var mu sync.Mutex

	go func() {
		for {
			csvFile, err := getNewestCSVFile(cfg.TigoDAQSDataDir)
			if err != nil {
				log.Printf("Error getting newest CSV file: %v", err)
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			fileInfo, err := os.Stat(csvFile)
			if err != nil {
				log.Printf("Error stating CSV file: %v", err)
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			curCSVModified := fileInfo.ModTime()
			if !lastCSVTime.IsZero() && lastCSVTime == curCSVModified {
				if time.Since(lastCSVTime) > 10*time.Minute {
					modulePower.Reset()
					moduleRSSI.Reset()
					moduleTemp.Reset()
					moduleVolts.Reset()
				}
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			lastCSVTime = curCSVModified
			file, err := os.Open(csvFile)
			if err != nil {
				log.Printf("Unable to open CSV file: %v", err)
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			rdr := csv.NewReader(file)
			headers, err := rdr.Read()
			if err != nil {
				log.Printf("Error reading CSV headers: %v", err)
				file.Close()
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			moduleCount := (len(headers) - 3) / 12
			records, err := rdr.ReadAll()
			if err != nil {
				log.Printf("Error reading CSV records: %v", err)
				file.Close()
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}
			file.Close()

			if len(records) == 0 {
				time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
				continue
			}

			lastRecord := records[len(records)-1]

			mu.Lock()
			for i := 0; i < moduleCount; i++ {
				startIndex := 3 + i*12
				moduleIndex := i + 1

				vin, err := getFieldValue(lastRecord[startIndex+0])
				if err != nil {
					failCounterMap[startIndex+0]++
				} else {
					failCounterMap[startIndex+0] = 0
				}

				rssi, err := getFieldValue(lastRecord[startIndex+6])
				if err != nil {
					failCounterMap[startIndex+6]++
				} else {
					failCounterMap[startIndex+6] = 0
				}

				pin, err := getFieldValue(lastRecord[startIndex+11])
				if err != nil {
					failCounterMap[startIndex+11]++
				} else {
					failCounterMap[startIndex+11] = 0
				}

				temp, err := getFieldValue(lastRecord[startIndex+2])
				if err != nil {
					failCounterMap[startIndex+2]++
				} else {
					failCounterMap[startIndex+2] = 0
				}

				updateGauge(moduleVolts, moduleIndex, vin, failCounterMap[startIndex+0])
				updateGauge(moduleRSSI, moduleIndex, rssi, failCounterMap[startIndex+6])
				updateGauge(modulePower, moduleIndex, pin, failCounterMap[startIndex+11])
				updateGauge(moduleTemp, moduleIndex, temp, failCounterMap[startIndex+2])
			}

			lastTimestamp, _ := getFieldValue(lastRecord[1])
			tigoTimestamp.WithLabelValues("local", "cca").Set(lastTimestamp)

			mu.Unlock()
			time.Sleep(REFRESH_INTERVAL_SEC * time.Second)
		}
	}()

	fmt.Println("Now listening on", bindAddress)
	log.Fatal(server.ListenAndServe())
}


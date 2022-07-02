package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/pingcap/tidb-binlog/drainer"
	"github.com/spf13/pflag"
)

const (
	flagPrintHistoryInfo = "print-history-info"
	flagPDUrls           = "pd-urls"
	flagOutputFile       = "output"

	defaultEtcdURLs = "http://127.0.0.1:2379"
)

// ddldumper program is used to read the ddl history from tikv, it lower the tidb load.
func main() {
	printHistoryInfo := false
	pdUrls := ""
	outputFile := ""
	pflag.BoolVar(&printHistoryInfo, flagPrintHistoryInfo, true, "print jobs history info")
	pflag.StringVar(&pdUrls, flagPDUrls, defaultEtcdURLs, "a comma separated list of PD endpoints")
	pflag.StringVar(&outputFile, flagOutputFile, "./ddlJobs", "input file")
	pflag.Parse()

	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	jobs, err := drainer.GetHistoryJobs(pdUrls)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for len(jobs) > 0 {
		job := jobs[0]
		if printHistoryInfo {
			jobBytes, err := json.Marshal(job)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			_, err = file.WriteString(string(jobBytes) + "\n")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		} else {
			_, err = file.WriteString(job.String() + "\n")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
		jobs = jobs[1:]
	}
}

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pingcap/parser/model"
	"github.com/spf13/pflag"
)

const (
	flagInputFile  = "input"
	flagOutputFile = "output"
	targetJobId    = 2820055
)

func main() {
	inputFile := ""
	outputFile := ""
	pflag.StringVar(&inputFile, flagInputFile, "./ddlJobs", "input file")
	pflag.StringVar(&outputFile, flagOutputFile, "./ddlSpecificJobs", "output file")
	pflag.Parse()

	inFile, err := os.Open(inputFile)
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer inFile.Close()
	defer outFile.Close()

	scan := bufio.NewScanner(inFile)

	for scan.Scan() {
		line := scan.Text()
		job := model.Job{}
		err = json.Unmarshal([]byte(line), &job)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if job.ID == targetJobId {
			outFile.WriteString(line + "\n")
		}
	}
}

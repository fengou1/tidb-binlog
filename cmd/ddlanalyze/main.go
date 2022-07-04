package main

import (
   "bufio"
   "bytes"
   "encoding/json"
   "fmt"
   "io"
   "math"
   "os"

   "github.com/pingcap/parser/model"
   "github.com/pingcap/tidb-binlog/drainer"
)

const INF = math.MaxInt64

func run() error {
   schema, err := drainer.NewSchema(nil, false)
   if err != nil {
      return err
   }
   file, err := os.Open("/code/ddlJobs")
   if err != nil {
      return err
   }
   defer file.Close()

   bfReader := bufio.NewReaderSize(file, 1<<20)
   cnt := 0
   defer func() {
      fmt.Println(cnt)
   }()
   for {
      var byt []byte
      byt, _, err = bfReader.ReadLine()
      if err != nil {
         if err == io.EOF {
            break
         }
         return err
      }
      byt = bytes.Trim(byt, "\n")
      job := &model.Job{}
      err = json.Unmarshal(byt, job)
      if err != nil {
         return err
      }
      schema.AddJob(job)
      err = schema.HandlePreviousDDLJobIfNeed(INF)
      if err != nil {
         return err
      }
      cnt++
   }
   return nil
}

func main() {
   if err := run(); err != nil {
      fmt.Println(err)
      os.Exit(1)
   }
}
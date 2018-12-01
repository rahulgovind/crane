package main

import (
	"fmt"
	"github.com/rahulgovind/crane"
	"time"
)

func main() {
	var topology = []crane.Node{
		{
			Name:         "Spout",
			BlockSize:    1000000,
			InputFormat:  "line",
			OutputFormat: "json",
			Output:       []string{"Bolt"},
		},
		{
			Name:         "Bolt",
			InputFormat:  "json",
			OutputFormat: "json",
			Output:       []string{"Sink"},
		},
		{
			Name:        "Sink",
			InputFormat: "json",
			Sink:        true,
		},
	}

	var program = crane.Program{Source: `
							package main
							import "strconv"

							func SpoutInput() interface{} {
								return ""
							}
	
							func SpoutOutput() interface{} {
								return int64(0)
							}

							func Spout(s string) []int64 { 
								i, _ := strconv.ParseInt(s, 10, 64)
								return []int64{i}
							}

							func BoltInput() interface{} {
								return int64(0)
							}

							func BoltOutput() interface{} {
								return int64(0)
							}

							func Bolt(v int64) []int64 {
								return []int64{2 * v}
							}

							func SinkInput() interface{} {
								return int64(0)
							}
							`}
	c := crane.NewCrane("127.0.0.1:4101", topology, program)
	c.UploadProgram()
	for {
		status, _ := c.GetTopologyStatus()
		completed := false
		for _, summary := range status {
			fmt.Printf("%v: Completed %v/%v.\t%v Left\n", summary.Name, summary.Completed, summary.Total,
				summary.Total-summary.Completed)
			if summary.Name == "Sink" && summary.Total >= 1 {
				completed = true
			}
		}
		if completed {
			break
		}
		<-time.NewTimer(time.Second).C
	}
	count, _ := c.Count("Sink")
	fmt.Println("Count: ", count)
}

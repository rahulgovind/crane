package crane

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"reflect"
)

type Tuple []interface{}

type Topology []Node

type Node struct {
	Name         string
	Output       []string
	BlockSize    int
	InputFormat  string
	OutputFormat string
	P            Program
	f            reflect.Value
	input        reflect.Value
	output       reflect.Value
}

type SubmitJobArgs struct {
	Topology Topology
	Program  Program
}

type SubmitJobResponse struct {
	Topology Topology
}

func (n *Node) Load() {
	functions := n.P.Load(n.Name, n.Name+"Input", n.Name+"Output")
	n.f = reflect.ValueOf(functions[0])
	n.input = reflect.ValueOf(functions[1].(func() interface{})())
	n.output = reflect.ValueOf(functions[2].(func() interface{})())
}

func (n *Node) GetInput() reflect.Value {
	return n.input
}

func (n *Node) GetOutput() reflect.Value {
	return n.output
}

func (n *Node) GetFunction() reflect.Value {
	return n.f
}

func (t *Topology) getNodeNames() (result []string) {
	for _, n := range *t {
		result = append(result, n.Name)
	}
	return
}

func CreateNodeConfiguration(node Node) io.Reader {
	// Program not initialized
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(node)
	if err != nil {
		log.Fatal("Unable to create node configuration: ", err)
	}

	dec := gob.NewDecoder(bytes.NewReader(buf.Bytes()))
	n := new(Node)
	dec.Decode(n)

	return bytes.NewReader(buf.Bytes())
}

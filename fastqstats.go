package main

// fastq stats with go

import (
	"fmt"
	"os"
	"compress/gzip"
	"bufio"
	"regexp"
	"strings"
	"log"
	"encoding/json"
	"strconv"
	"runtime"
)


func main(){
	if len(os.Args) < 3 {
		log.Fatal("\n\t#usage:\n\t\t " + os.Args[0] + " <nProcesses> <fastqs>... ")		
	}   

	nproc, err := strconv.Atoi(os.Args[1])
	if err != nil { log.Fatal(err) }

	runtime.GOMAXPROCS(nproc)
	stats_channels := make([]chan Stats, len(os.Args)-2)
	stats :=  make([]Stats, len(os.Args)-2)

	fmt.Println(os.Args[1:])

	for i, f := range(os.Args[2:]) {
		stats_channels[i] = make(chan Stats)
		fmt.Println("starting: " + f)
		go GZIPFastq(f, stats_channels[i])
	}

	for i, _ := range(os.Args[2:]){
		stats[i] = <- stats_channels[i]
		fmt.Println(stats[i].Json())
	}
}


type Stats struct {
	f string
	counts map[string]uint64
	index map[string]uint64
	keys map[string]string

}

func (stats *Stats) Json()(string){
	counts, _ := json.Marshal(stats.counts)
	keys, _ := json.Marshal(stats.keys)
	index, _ := json.Marshal(stats.index)
	return fmt.Sprintf("[%s, %s, %s, %s]", stats.f, string(counts), string(keys), string(index))
}

func machine_name_fields(machine_name *string)(string, string, string){
	fields := strings.Split(*machine_name, "/")
	if len(fields) != 2 {panic("machine_name? " + *machine_name)}
	machine_read := fields[1]
	fields = strings.Split(fields[0], ":")
	if len(fields) != 5 {panic("machine_name? " + *machine_name)}
	machine, lane := fields[0], fields[1]
	return machine, lane, machine_read 
}

func file_data_fields(file_data *string)(string, string, string){
	fields := strings.Split(*file_data, ":")
	if len(fields) != 4 {panic("file_data? " + *file_data)}
	file_read, fail, index := fields[0], fields[1], fields[3]
	if fail != "Y" && fail != "N" {panic("file_data? " + *file_data)}
	return file_read, fail, index
}

func GZIPFastq(filename string, out chan Stats){
	var line_count uint64 = 0
	var read_count uint64 = 0
	var qc_fails uint64 = 0
	var qc_ok uint64 = 0

	var words []string
	machine, lane, machine_read, file_read, fail, index, original_lane, original_read, original_machine := "", "", "", "", "", "", "", "", ""  

	var matched bool
	var machine_name_regex = "^@[^:/]+:[^:/]+:[^:/]+:[^:/]+:[^:/]+/[1-2]{1}$"

	file, gz := openFastq(filename)
	scanner := bufio.NewScanner(gz)

	defer func(){
	        gz.Close()
        	file.Close()
	}()

	var stats Stats
	stats.f = filename
	stats.counts = make(map[string]uint64)
	stats.index = make(map[string]uint64)
	stats.keys = make(map[string]string)

	for scanner.Scan() {
		line_count += 1
		if line_count % 1000000 == 0 {fmt.Printf("line: %d\n", line_count) }

		words = strings.Split(strings.TrimSpace(scanner.Text()), " ")
		if len(words) == 2 {
    		matched, _ = regexp.MatchString(machine_name_regex, words[0])
    		if matched {
    			machine, lane, machine_read = machine_name_fields(&words[0])
    			file_read, fail, index = file_data_fields(&words[1])

   				if read_count == 0 { original_lane = lane; original_read = machine_read; original_machine = machine }
   				if lane != original_lane || file_read != machine_read || file_read != original_read || machine != original_machine { 
   					log.Fatalf(" ambigous lane: %s, %s expected", lane, original_lane)
   				}

   				if fail == "Y" {qc_fails += 1} else if fail == "N" {qc_ok += 1}

   				stats.index[index] = stats.index[index] + 1
   				read_count += 1
	   		}
	   		if read_count == 10000000 { break }
    	}
	}

	stats.counts["lines"] = line_count
	stats.counts["reads"] = read_count
	stats.counts["qc_fail"] = qc_fails
	stats.counts["qc_ok"] = qc_ok
	stats.keys["lane"] = original_lane
	stats.keys["read"] = original_read
	stats.keys["machine"] = original_machine
	out <- stats
}

func openFastq(filename string)(*os.File, *gzip.Reader) {
	file_handle, _ := os.Open(filename)
	gzip_handle, _ := gzip.NewReader(file_handle)
	return file_handle, gzip_handle
}

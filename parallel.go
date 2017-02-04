package main


/*
  like xargs, but with nicer logging
 */

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"bufio"
	"strconv"
	"strings"
)


func Contents(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

type ExitStatus struct {
	Id string
	Return int
}

type Pipes struct {
	cmd *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
	stdin io.WriteCloser
	bufOut *bufio.Reader
	bufErr *bufio.Reader
	bufIn *bufio.Writer
}


func Run(program string, args ...string) error {
    cmd := exec.Command(program, args...)
    cmd.Stdin = os.Stdin;
    cmd.Stdout = os.Stdout;
    cmd.Stderr = os.Stderr;
    err := cmd.Run() 
    if err != nil {
        log.Printf("%v\n", err)
    }
    return err
}


func (run *Pipes) Execute(program string, args ...string) error {
    var err error
    (*run).cmd = exec.Command(program, args...)
    run.stdout, err = (*run).cmd.StdoutPipe()
    if err != nil {panic(err)}   
    (*run).stderr, err = (*run).cmd.StderrPipe()
    if err != nil {panic(err)}
    (*run).stdin, err = (*run).cmd.StdinPipe()
    if err != nil {panic(err)}
    (*run).bufOut = bufio.NewReader((*run).stdout)
    (*run).bufErr = bufio.NewReader((*run).stderr)
    (*run).bufIn = bufio.NewWriter((*run).stdin)
    err = (*run).cmd.Start()
    if err != nil {
    	log.Println(fmt.Sprintf("#PROBLEM.. %s", program))
    }
    return err
}

func (run *Pipes) ReadOut() (string, error) {
	line, err := (*run).bufOut.ReadString('\n')
	return line, err
}

func (run *Pipes) Wait() error {
	return (*run).cmd.Wait()
}

func (run *Pipes) ReadErr() (string, error) {
	line, err := (*run).bufErr.ReadString('\n')
	return line, err
}

func (run *Pipes) Process(status_channel chan ExitStatus, id string, program string, args ...string) {
		var start_status = run.Execute(program, args...)
		var err_status = ExitStatus{id, 1}
		if start_status != nil {
			fmt.Println(fmt.Sprintf("#ERR.. failed@start:%s", id))
			fmt.Println(fmt.Sprintf("#ERR.. return:1 [%s status:%s]", id, start_status.Error())) 
			status_channel <- err_status
		} 

		fmt.Println(fmt.Sprintf("#MSG.. started:ok [%s]", id))
		var line_o, line_e string
		var err_o, err_e error
		for {
			line_o, err_o = run.ReadOut()
			line_e, err_e = run.ReadErr()

			if err_o != nil && err_e != nil { 
				break 
			}
			if err_o == nil {
				fmt.Print(fmt.Sprintf("#!%s %s" , id, line_o))
			}
			if err_e == nil { 
				fmt.Print(fmt.Sprintf("#ERR.. %s" , id, line_e))
			}
		}
		exit_status := run.Wait()

		if err_o != io.EOF { fmt.Println(fmt.Sprintf("#MSG.. failed[%s]:%s", id, err_o)) }
		if err_e != io.EOF { fmt.Println(fmt.Sprintf("#MSG.. failed[%s]:%s", id, err_e)) }
		flush_stdin_buffer := (*run).bufIn.Flush() // wait closes everything else, so this better be ok
		if(flush_stdin_buffer != nil){
			fmt.Println(fmt.Sprintf("#WARN[%s].. %s", id, flush_stdin_buffer.Error()))
		}

		if exit_status == nil {
			fmt.Println(fmt.Sprintf("#%s return:0 status:ok\n", id))
			ok_status := ExitStatus{id, 0}
			status_channel <- ok_status
		} else {
			fmt.Println(fmt.Sprintf("#%s return:1 status:%s\n", id, exit_status.Error())) 
			status_channel <- err_status
		}
		
}

func main(){
	var args = os.Args[1:]
	if len(args) < 1 {
		log.Fatal("missing tasklist")
	}
	var tasks, err = Contents(args[0])
	if err != nil {
		log.Fatal("invalid tasklist")
	}
	var nParallel = 5
	var n = len(tasks);
	var statusChannel = make(chan ExitStatus) 
	var run = make([]Pipes, n, n)
	var started = 0
	if nParallel >= n {
		nParallel = n
	}
	for started = 0; started < nParallel; started++ {
		var task = strings.Split(tasks[started], " "); // replace with shlex, https://github.com/google/shlex
		go (&run[started]).Process(statusChannel, strconv.Itoa(started), task[0], task[1:]...)
	}
	var running = started
	for processed := 0; processed < n; {
		status_code :=  <- statusChannel
		fmt.Println(fmt.Sprintf("#%s_RETURN:%d\n", status_code.Id, status_code.Return))
		running--
		processed++
		if running < nParallel && started < n {
	                var task = strings.Split(tasks[started], " "); // replace with shlex, https://github.com/google/shlex
        	        go (&run[started]).Process(statusChannel, strconv.Itoa(started), task[0], task[1:]...)
			started++
			running++
		}
	}
	fmt.Println("...ok")
}

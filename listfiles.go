package main

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const MAXCALLS = 128

func handleError(err error, callback func(error)) error {
	if err != nil {
		if callback == nil {
			log.Output(1, err.Error())
		} else {
			callback(err)
		}
	}
	return err
}

type Listing struct {
	files *list.List
	dirs  *list.List
}

func NewListing() Listing {
	return Listing{files: list.New(), dirs: list.New()}
}

type ThrottledListDir struct {
	count int32
	mtx   sync.Mutex
}

func newThrottledListDir() ThrottledListDir {
	return ThrottledListDir{count: 0, mtx: sync.Mutex{}}
}

type Mutexed struct {
	mtx sync.Mutex
}

func (m *Mutexed) Apply(f func()) {
	defer m.mtx.Unlock()
	m.mtx.Lock()
	f()
}

// keep ReadDir from spawing off too much
func (th *ThrottledListDir) ReadDir(dirname string) ([]os.FileInfo, error) {
	defer th.mtx.Unlock()
	var count int32
	for {
		th.mtx.Lock()
		count = atomic.LoadInt32(&th.count)
		if count < MAXCALLS {
			atomic.AddInt32(&th.count, 1)
			files, err := ioutil.ReadDir(dirname)
			atomic.AddInt32(&th.count, -1)
			return files, err
		}
		th.mtx.Unlock()
		time.Sleep(10 * time.Microsecond)
	}
}

func listDir(dirname string, listingChannel chan Listing, throttled *ThrottledListDir, sharedLock *Mutexed, callback func([]string)) {
	os.Stderr.WriteString(fmt.Sprintf("on:%s\n", dirname))
	var flist = make([]string, 0)
	var dirlist = make([]string, 0)
	files, err := throttled.ReadDir(dirname)
	handleError(err, nil)
	listing := NewListing()
	if err == nil {
		var fullpath string
		var syncChannels = make([]chan Listing, 0)
		for _, file := range files {
			fullpath = dirname + "/" + file.Name()
			switch mode := file.Mode(); {
			case mode.IsRegular():
				flist = append(flist, fullpath)
			case mode.IsDir():
				dirlist = append(dirlist, fullpath)
				var newChannel = make(chan Listing)
				syncChannels = append(syncChannels, newChannel)
				go listDir(fullpath, newChannel, throttled, sharedLock, callback)
			}
		}
		sharedLock.Apply(func() { callback(flist) })
		listing.files.PushBack(flist)
		listing.dirs.PushBack(dirlist)
		for _, channel := range syncChannels {
			subdirectory := <-channel
			listing.files.PushBackList(subdirectory.files)
			listing.dirs.PushBackList(subdirectory.dirs)
			close(channel)
		}
	}
	// writing to channel is a shallow copy, so this is okay
	listingChannel <- listing
}

func printStrSlice(s []string) {
	for _, e := range s {
		fmt.Println(e)
	}
}

func ls(dirname string) {
	var lister = newThrottledListDir()
	var listingChannel = make(chan Listing)
	var sharedLock = Mutexed{mtx: sync.Mutex{}}
	go listDir(dirname, listingChannel, &lister, &sharedLock, printStrSlice)
	listing := <-listingChannel
	close(listingChannel)
	// each subdirectory gets its own slice of filenames in the list
	os.Stderr.WriteString(fmt.Sprintf("#subdirectories:%d\n", listing.files.Len()))
	/*for e := listing.files.Front(); e != nil; e = e.Next() {
		sliced := e.Value.([]string)
		fmt.Println(sliced)
	}*/

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 2)
	var args = os.Args
	for _, d := range args[1:] {
		ls(d)
	}
}

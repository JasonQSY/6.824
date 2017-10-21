package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	debug("phase: %v\n", phase)
	debug("ntasks: %v\n", ntasks)
	debug("n_other: %v\n", n_other)

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(num int) { // pass num since it will be changed in the external loop
			args := DoTaskArgs{jobName, mapFiles[num], phase, num, n_other}
			for { // query new worker until one worker finishes the task.
				addr := <-registerChan
				ok := call(addr, "Worker.DoTask", args, nil)
				if ok {
					// Task finished
					// wg.done() must be called earlier than registerChan<-addr,
					// otherwise it will be blocked for the last job.
					wg.Done()
					registerChan <- addr
					break
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

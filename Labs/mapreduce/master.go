package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	online bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// Create channel used for synchronizing goroutine calls
	//   See comments in map and reduce sections below
	waitChannel := make(chan int)

	// Create args for calling worker jobs
	args := new(DoJobArgs)
	args.File = mr.file

	// Listen on registerChannel to discover new workers
	var newWorker string
	go func() {
		for {
			// Wait for new worker to register
			newWorker = <-mr.registerChannel
			// Record worker information and mark as available
			//   Note this will override previous info
			//   if a worker comes back online after failing
			mr.Workers[newWorker] = new(WorkerInfo)
			mr.Workers[newWorker].address = newWorker
			mr.Workers[newWorker].online = true
			mr.availableChannel <- newWorker
			mr.onlineWorkers++
			DPrintf("*** (registerChan goroutine) Got new worker! ***\n")
		}
	}()

	// Use loop to do map and reduce phases
	// instead of copying all the code
	//   (easier for editing, etc)
	numThisPhase := 0
	for phase := 0; phase < 2; phase++ {
		// Set up phase-specific variables
		if phase == 0 {
			// MAP PHASE
			DPrintf("\n\n\nMAP PHASE\n")
			args.Operation = Map
			args.NumOtherPhase = mr.nReduce
			numThisPhase = mr.nMap
		}
		if phase == 1 {
			// REDUCE PHASE
			DPrintf("\n\n\nREDUCE PHASE\n")
			args.Operation = Reduce
			args.NumOtherPhase = mr.nMap
			numThisPhase = mr.nReduce
		}

		// Create list of file numbers that need processing
		//  This will allow easy handling of worker failures
		filesToProcess := list.New()
		for f := 0; f < numThisPhase; f++ {
			filesToProcess.PushBack(f)
		}

		var curFileNum int
		var nextWorker string
		// Process all map files
		// Double loop is for case where the last few workers
		// fail (they fail after filesToProcess has been emptied)
		//  This only happens if a worker fails during a job, which
		//  shouldn't happen for us but it is here for completeness
		//  See comments at end of loop for more details
		for filesToProcess.Len() > 0 {
			for filesToProcess.Len() > 0 {
				curFileNum = filesToProcess.Front().Value.(int)
				filesToProcess.Remove(filesToProcess.Front())
				args.JobNumber = curFileNum
				// Get an available worker (wait if none currently free)
				if mr.onlineWorkers == 0 {
					DPrintf("*** NO WORKERS ARE ONLINE ***\n")
				}
				DPrintf("Waiting for available worker\n")
				nextWorker = <-mr.availableChannel
				// Send the next file to that worker
				go func(inArgs *DoJobArgs, inNextWorker *WorkerInfo) {
					// Create local clones of input arguments
					// so that they are not shared over all running goroutines
					workerArgs := new(DoJobArgs)
					workerArgs.File = inArgs.File
					workerArgs.JobNumber = inArgs.JobNumber
					workerArgs.Operation = inArgs.Operation
					workerArgs.NumOtherPhase = inArgs.NumOtherPhase
					worker := new(WorkerInfo)
					worker.address = inNextWorker.address

					DPrintf("\t(worker goroutine) starting job on worker %s", worker.address)
					DPrintf("\tfilenum %v\n", workerArgs.JobNumber)

					// Signal that goroutine is started and arguments are cloned
					waitChannel <- 1
					// Start the job on the worker!
					reply := new(DoJobReply)
					workerSuccess := call(worker.address, "Worker.DoJob", workerArgs, &reply) // wait for job to finish
					DPrintf("\t(worker goroutine) done with job on worker, %s", worker.address)
					DPrintf("\tfilenum %v\n", workerArgs.JobNumber)

					// If job failed, mark it for redo
					if !workerSuccess || !reply.OK {
						DPrintf("\t*** JOB FAILED *** filenum %v\n", workerArgs.JobNumber)
						filesToProcess.PushBack(workerArgs.JobNumber)
					}
					// Signal worker is available again if worker didn't fail
					//   note registerChannel goroutine is still running
					//   so if it comes back online it will be marked as such
					if workerSuccess {
						mr.availableChannel <- worker.address
					} else {
						mr.Workers[worker.address].online = false
						mr.onlineWorkers--
						DPrintf("\t*** WORKER FAILED *** worker %s\n", worker.address)
					}
				}(args, mr.Workers[nextWorker])

				// Wait for goroutine to clone arguments
				// to ensure that proper values are used
				<-waitChannel
			} // while filesToProcess.Len() > 0

			// Wait for any outstanding goroutines to finish (either success or failure)
			//	All map jobs must be completed before starting reduce
			DPrintf("\nWaiting for workers to finish\n")
			numWorkersAvailable := 0
			for numWorkersAvailable < mr.onlineWorkers {
				<-mr.availableChannel // Read from buffer or wait for workers to finish jobs
				numWorkersAvailable++
			}
			// Put all online workers back in available queue for next phase
			//   Put in goroutine since will block when channel buffer fills
			DPrintf("Putting available workers back in queue\n")
			go func() {
				for worker, workerInfo := range mr.Workers {
					if workerInfo.online {
						mr.availableChannel <- worker
					}
				}
			}()
			// If filesToProcess is still empty, then this phase is done
			// If one of the last workers that we waited for above
			// failed, then their jobs have been re-added to filesToProcess
			// so we now go back to finish them up
		} // while filesToProcess.Len() > 0
	} // switch from map to reduce

	return mr.KillWorkers()
}

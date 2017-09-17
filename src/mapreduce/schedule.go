package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//  hand out the map and reduce tasks to workers, and return only when all the tasks have finished
	/*
		1. 从channel获取worker
		2. 通过worker进行rpc调用, `Worker.DoTask`,
		3. 若rpc调用执行失败, 则将任务重新塞入registerChannel执行
		ps: 使用WaitGroup保证线程同步
		若不加Wait等待所有goroutine结束在返回, 则会导致一些结果文件并未生成, 测试挂掉
	 */

	var wg sync.WaitGroup  //
	//doneChannel := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		wg.Add(1)  // 增加WaitGroup的计数


		go func(taskNum int, nios int, phase jobPhase) {
			debug("DEBUG: current taskNum: %v, nios: %v, phase: %v\n", taskNum, nios, phase)
			for  {
				worker := <-mr.registerChannel  // 获取工作rpc服务器, worker == address
				debug("DEBUG: current worker port: %v\n", worker)

				var args DoTaskArgs
				if phase == mapPhase{
					args=DoTaskArgs{JobName:mr.jobName,File:mr.files[taskNum],
						Phase:phase,TaskNumber:taskNum,
						NumOtherPhase:nios}
				} else {
					args=DoTaskArgs{JobName:mr.jobName,File:"",
						Phase:phase,TaskNumber:taskNum,
						NumOtherPhase:nios}
				}

				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					wg.Done()
					mr.registerChannel <- worker
					break
				}  // else 表示失败, 使用新的worker 则会进入下一次for循环重试
			}
		}(i, nios, phase)


	}
	wg.Wait()  // 等待所有的任务完成

	fmt.Printf("Schedule: %v phase done\n", phase)
}

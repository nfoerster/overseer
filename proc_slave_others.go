// +build !windows

package overseer

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

func (sp *slave) watchParent() error {
	sp.masterPid = os.Getppid()
	proc, err := os.FindProcess(sp.masterPid)
	if err != nil {
		return fmt.Errorf("master process: %s", err)
	}
	sp.masterProc = proc
	go func() {
		//send signal 0 to master process forever
		for {
			//should not error as long as the process is alive
			if err := sp.masterProc.Signal(syscall.Signal(0)); err != nil {
				log.Printf("signalling to master failed, exit code 1, err:%v", err.Error())
				os.Exit(1)
			}
			time.Sleep(2 * time.Second)
		}
	}()
	return nil
}

func overwrite(dst, src string) error {
	return move(dst, src)
}

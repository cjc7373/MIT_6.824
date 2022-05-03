package mr

import (
	"fmt"
	"log"
	"os"
)

// Debugging
const Debug = false

var logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		logger.Output(2, fmt.Sprintf(format, a...))
	}
	return
}

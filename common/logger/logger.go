package logger

import (
	"log"
	"os"
)

var (
	// Info logger
	Info *log.Logger
	// Warning logger
	Warning *log.Logger
	// Error logger
	Error *log.Logger
)

func init() {
	Info = log.New(os.Stdout, "<< Info >>: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "<< Warning >>: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "<< Error >>: ", log.Ldate|log.Ltime|log.Lshortfile)
}

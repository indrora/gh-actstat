package main

import (
	"fmt"
	"os"
	"time"

	"github.com/theckman/yacspin"
)

// runWithSpinner runs a spinFunc with a yacspin spinner, updating the spinner message with progress from the channel
func runWithSpinner(startMessage string, spinFunc func(chan<- string) error) error {

	cfg := yacspin.Config{
		Frequency: 150 * time.Millisecond,
		Writer:    os.Stdout,
		CharSet:   yacspin.CharSets[14],
		Message:   startMessage,
	}
	s, serr := yacspin.New(cfg)
	if serr != nil {
		panic(serr)
	}

	progress := make(chan string)
	done := make(chan struct{})

	s.Start()
	go func() {
		s.Message(startMessage)
		for {
			select {
			case msg := <-progress:
				s.Message(msg)
			case <-done:
				s.Stop()
				return
			}
		}
	}()
	funcerror := spinFunc(progress)
	close(done)
	fmt.Print("\r\033[K") // Clear the spinner line
	return funcerror
}

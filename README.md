# go-firempq-connector

This is quick implementation of FireMPQ client in Go. It doesn't do much yet.
It can only establish a connection to the hardcored IP address and start pushing
lots of messages using 500 goroutines.

The whole goal of it at the moment is to profile FireMPQ server performance.
It seems at the moment, that client runs slower than server on Mac OS :-|

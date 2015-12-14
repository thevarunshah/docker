// Package cloudlogs provides an interface for the logdriver to forward container logs to any cloud provider
package cloudlogs

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
)

const (
	name                  = "cloudlogs"
	batchPublishFrequency = 5 * time.Second
)

type logStream struct {
	logStreamName string
	logGroupName  string
	client        api
	messages      chan *logger.Message
	lock          sync.RWMutex
	closed        bool
	sequenceToken *string
}

type api interface {
	CreateLogStream(*cloudlogs.CreateLogStreamInput) (*cloudlogs.CreateLogStreamOutput, error)
	PutLogEvents(*cloudlogs.PutLogEventsInput) (*cloudlogs.PutLogEventsOutput, error)
}

type byTimestamp []*cloudlogs.InputLogEvent

// init registers the cloud log driver
func init() { }

// New creates an cloud-specific logger using the configuration passed 
// in on the context. 
func New(ctx logger.Context) (logger.Logger, error) {
	logStreamName := ctx.ContainerID
	containerStream := &logStream{
		logStreamName: logStreamName,
		client:        cloudlogs.New(config),
		messages:      make(chan *logger.Message, 4096),
	}
	err := containerStream.create()
	if err != nil {
		return nil, err
	}
	go containerStream.collectBatch()

	return containerStream, nil
}

// Name returns the name of the cloud logging driver
func (l *logStream) Name() string {
	return name
}

// Log submits messages for logging by an instance of the cloud logging driver
func (l *logStream) Log(msg *logger.Message) error {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if !l.closed {
		l.messages <- msg
	}
	return nil
}

// Close closes the instance of the cloud logging driver
func (l *logStream) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if !l.closed {
		close(l.messages)
	}
	l.closed = true
	return nil
}

// create creates a log stream for the instance of the cloud logging driver
func (l *logStream) create() error {
	input := &cloudlogs.CreateLogStreamInput{ }

	_, err := l.client.CreateLogStream(input)

	return err
}

// newTicker is used for time-based batching.  newTicker is a variable such
// that the implementation can be swapped out for unit tests.
var newTicker = func(freq time.Duration) *time.Ticker {
	return time.NewTicker(freq)
}

// collectBatch executes as a goroutine to perform batching of log events for
// submission to the log stream.  Batching is performed on time- and can also 
// be performed on size-bases.  Time-based batching occurs at a 5 second 
// interval (defined in the batchPublishFrequency const).  
func (l *logStream) collectBatch() {
	timer := newTicker(batchPublishFrequency)

	//implement cloud-provider specific solution here
}

// publishBatch calls PutLogEvents for a given set of InputLogEvents,
// accounting for sequencing requirements (each request must reference the
// sequence token returned by the previous request).
func (l *logStream) publishBatch(events []*cloudlogs.InputLogEvent) {
	if len(events) == 0 {
		return
	}

	sort.Sort(byTimestamp(events))

	nextSequenceToken, err := l.putLogEvents(events, l.sequenceToken)

	if err != nil {
		logrus.Error(err)
	} else {
		l.sequenceToken = nextSequenceToken
	}
}

// putLogEvents wraps the PutLogEvents API
func (l *logStream) putLogEvents(events []*cloudlogs.InputLogEvent, sequenceToken *string) (*string, error) {
	input := &cloudlogs.PutLogEventsInput{
		LogEvents:     events,
		SequenceToken: sequenceToken,
	}
	resp, err := l.client.PutLogEvents(input)
	if err != nil {
		return nil, err
	}
	return resp.NextSequenceToken, nil
}

// Len returns the length of a byTimestamp slice.  Len is required by the
// sort.Interface interface.
func (slice byTimestamp) Len() int {
	return len(slice)
}

// Less compares two values in a byTimestamp slice by Timestamp.  Less is
// required by the sort.Interface interface.
func (slice byTimestamp) Less(i, j int) bool {
	iTimestamp, jTimestamp := int64(0), int64(0)
	if slice != nil && slice[i].Timestamp != nil {
		iTimestamp = *slice[i].Timestamp
	}
	if slice != nil && slice[j].Timestamp != nil {
		jTimestamp = *slice[j].Timestamp
	}
	return iTimestamp < jTimestamp
}

// Swap swaps two values in a byTimestamp slice with each other.  Swap is
// required by the sort.Interface interface.
func (slice byTimestamp) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

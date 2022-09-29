package util

import (
	"sync"
	"time"
)

type mediaSet map[chan struct{}]struct{}

var waiterLock = &sync.Mutex{}
var waiters = map[string]mediaSet{}

func StartWaitForUpload(origin string, mediaID string) chan struct{} {
	key := origin + mediaID
	ch := make(chan struct{})

	waiterLock.Lock()
	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		set = make(mediaSet)
		waiters[key] = set
	}
	set[ch] = struct{}{}
	waiterLock.Unlock()

	return ch
}

func CancelWaitForUpload(ch chan struct{}, origin string, mediaID string) {
	key := origin + mediaID
	waiterLock.Lock()

	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		set = make(mediaSet)
		waiters[key] = set
	}

	delete(set, ch)
	close(ch)

	if len(set) == 0 {
		delete(waiters, key)
	}

	waiterLock.Unlock()
}

func WaitForUpload(ch chan struct{}, origin string, mediaID string, timeout time.Duration) bool {
	key := origin + mediaID
	waiterLock.Lock()

	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		set = make(mediaSet)
		waiters[key] = set
	}
	waiterLock.Unlock()

	defer func() {
		waiterLock.Lock()

		delete(set, ch)
		close(ch)

		if len(set) == 0 {
			delete(waiters, key)
		}

		waiterLock.Unlock()
	}()

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

func NotifyUpload(origin string, mediaId string) {
	waiterLock.Lock()
	defer waiterLock.Unlock()

	set := waiters[origin+mediaId]

	if set == nil {
		return
	}

	for channel := range set {
		channel <- struct{}{}
	}
}

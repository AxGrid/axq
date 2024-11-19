/*
 * Created by Zed 06.12.2023, 10:14
 */

package service

import (
	"errors"
	"sync"
	"time"
)

type FidLock struct {
	fid           uint64
	fidMu         sync.Mutex
	fidLockChan   map[uint64]chan bool
	fidLockChanMu sync.RWMutex
}

func NewFidLock(startFid uint64) *FidLock {
	return &FidLock{
		fid:         startFid,
		fidLockChan: make(map[uint64]chan bool),
	}
}

func (f *FidLock) WaitDo(fid uint64, do func() error) error {
	f.fidMu.Lock()
	if f.fid+1 == fid {
		err := do()
		if err == nil {
			f.fid++
			if ch, ok := f.fidLockChan[fid]; ok {
				ch <- true
			}
		}
		f.fidMu.Unlock()
		return err
	}
	c := make(chan bool, 1)
	f.fidLockChan[fid-1] = c
	f.fidMu.Unlock()
	select {
	case <-c:
		err := do()
		f.fidMu.Lock()
		defer f.fidMu.Unlock()
		delete(f.fidLockChan, fid-1)
		if err == nil {
			f.fid++
			if ch, ok := f.fidLockChan[fid]; ok {
				ch <- true
			}
		}
		return err
	case <-time.After(2 * time.Second):
		f.fidMu.Lock()
		defer f.fidMu.Unlock()
		delete(f.fidLockChan, fid-1)
		return errors.New("timeout")
	}
}

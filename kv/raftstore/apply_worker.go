package raftstore

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

type applyArg struct {
	d       *peerMsgHandler
	entries []pb.Entry
}

type applyWorker struct {
	applyCh chan applyArg
	closeCh chan struct{}
}

func NewApplyWorker() *applyWorker {
	return &applyWorker{
		applyCh: make(chan applyArg, 4096),
	}
}

func (aw *applyWorker) run() {
	go func() {
		for {
			select {
			case <-aw.closeCh:
				return
			case arg := <-aw.applyCh:
				arg.d.ApplyEntries(arg.entries)
			}
		}
	}()
}

func (aw *applyWorker) Add(arg applyArg) {
	aw.applyCh <- arg
}

func (aw *applyWorker) Stop() {
	aw.closeCh <- struct{}{}
}

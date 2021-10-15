// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"errors"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 主要是节点运行状态，包括Leader是谁，自己是什么角色。
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 主要是集群运行状态，包括届值、提交索引和Leader。如何
    // 更好理解软、硬状态？一句话，硬状态需要使用者持久化，而软状态不需要，就好像一个是内存
    // 值一个是硬盘值，这样就比较好理解了。
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	// ReadState是包含索引和rctx（就是ReadIndex()函数的参数）的结构，意义是某一时刻的集群
    // 最大提交索引。至于这个时刻使用者用于实现linearizable read就是另一回事了，其中rctx就是
    // 某一时刻的唯一标识。一句话概括：这个参数就是Node.ReadIndex()的结果回调。
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 需要存入可靠存储的日志，还记得log那片文章里面提到的unstable么，这些日志就是从哪里获取的
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 需要存入可靠存储的快照，它也是来自unstable
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// 已经提交的日志，用于使用者应用这些日志，需要注意的是，CommittedEntries可能与Entries有
    // 重叠的日志，这是因为Leader确认一半以上的节点接收就可以提交。而节点接收到新的提交索引的消息
    // 的时候，一些日志可能还存储在unstable中。
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// Messages是需要发送给其他节点的消息，raft负责封装消息但不负责发送消息，消息发送需要使用者
    // 来实现。
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	// MustSync指示了硬状态和不可靠日志是否必须同步的写入磁盘还是异步写入，也就是使用者必须把
    // 数据同步到磁盘后才能调用Advance()
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
// 获得applied段最高位index
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	// raft内部有两个计时：心跳和选举。raft内部没有设计定时器，计时就是由这个接口驱动的，每调用一次
    // 内部计数一次，这就是raft的计时原理。所以raft的计时粒度取决于调用者，这样的设计使得raft的
    // 适配能力更强。
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	// 这个是raft对外提供的非常核心的接口，前面关于提议的定义可知，该接口把使用者的data通过日志广播
    // 到所有节点，当然这个广播操作需要Leader执行。如果当前节点不是Leader，那么会把日志转发给Leader。
    // 需要注意：该函数虽然返回错误代码，但是返回nil不代表data已经被超过半数节点接收了。因为提议
    // 是一个异步的过程，该接口的返回值只能表示提议这个操作是否被允许，比如在没有选出Leader的情况下
    // 是无法提议的。而提议的数据被超过半数的节点接受是通过下面的Ready()获取的。
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	// 与Propose功能类似，数据不是使用者的业务数据，而是与raft配置相关的数据。配置数据对于raft
    // 需要保证所有节点是相同的，所以raft也采用了提议的方法保障配置数据的一致性。
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	// raft本身作为一种算法的实现并没有网络传输相关的实现，这些需要使用者自己实现。这也让raft
    // 本身的适配能力更强。在以前的文章中，笔者提到了大量的“消息”，消息就是raft节点间通信的协议，
    // 所以使用者在实现网络部分会收到其他节点发来的消息，所以把其他节点发来的消息需要通过Step()函数
    // 送到raft内部处理。此处多说一句，笔者喜欢etcd实现的raft原因之一就是不实现定时器，不实现网络
    // 传输，只专注与raft算法实现本身。
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	// 关于Ready笔者后面会有更加详细的说明，此处只举一个例子来说明Ready是个什么？在关于log文章
    // 中笔者提到过日志会从提交状态转到应用状态，这个转换过程就是通过Ready()函数实现的，使用者
    // 通过该函数获取chan然后从chan获取Ready数据，使用者再把数据应用到系统中。Ready中的数据
    // 不只有已提交的日志，还有其他内容，后面再详细说明。熟悉C++的同学可以把这个理解为回调，使用者
    // 通过Propose()提议的数据通过回调的方式在通知使用者
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	// Advance()函数是和Ready()函数配合使用的，当使用者处理完Ready数据后调用Advance()接口
    // 告知raft，这样raft才能向chan中推新的Ready数据。也就是使用者通过<-chan Ready获取的数据
    // 处理完后必须调用Advance()接口，否则将会阻塞在<-chan Ready上，因为raft此时也通过另一个
    // chan等待Advance()的信号。
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	// 在了解该接口之前，读者需要了解linearizable read的概念，就是读请求需要读到最新的已经
    // commit的数据。我们知道etcd是可以在任何节点读取数据的，如果该节点不是leader，那么数据
    // 很有可能不是最新的，会造成stale read。即便是leader也有可能网络分区，leader还自认为
    // 自己是leader，这里就会涉及到一致性模型，感兴趣的读者可以阅读笔者的《一致性模型》。
    // 书归正传，ReadIndex就是使用者获取当前集群的最大的提交索引，此时使用者只要应用的最大
    // 索引大于该值，使用者就可以实现读取是最新的数据。说白了就是读之前获取集群的最大提交索引，
    // 然后等节点应用到该索引时再把系统中对应的值返回给用户。其中rctx唯一的标识了此次读操作，
    // 索引会通过Ready()返回给使用者，获取最大提交索引是一个异步过程。
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	// 源码注释还是比较详细的，笔者总结为：Leader向其他节点发送的快照如果使用者发送失败了就需要通过
    // 这个接口汇报给raft。在tracker的文章中可知，节点的Progress处于快照状态是暂停的，直到节点回复
    // 快照被应用才能触发状态更新，如果此时节点异常接收快照失败，那么Leader会以为该节点的Progress
    // 一直处于暂停状态而不再向该节点发送任何日志。
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
func StartNode(c *Config, peers []Peer) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warningf("error occurred during starting a new node: %v", err)
	}

	n := newNode(rn)

	go n.run()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return &n
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan msgWithResult
	recvc      chan pb.Message
	confc      chan pb.ConfChangeV2
	confstatec chan pb.ConfState
	readyc     chan Ready
	advancec   chan struct{}
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status

	rn *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *node) run() {
	var propc chan msgWithResult 	// 上层应用传进来的消息
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.rn.raft

	// 初始状态不知道谁是leader，需要通过Ready获取
	lead := None

	for {
		if advancec != nil {
			readyc = nil
		} else if n.rn.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}

		// leader发生了变化，因为初始化lead=None，所以这个变化也可能是刚刚获取leader ID。
		if lead != r.lead {
			// 都是一些日志的东西，核心内容就是知道leader是谁后就可以通过propc读取数据，否则
            // 不处理。这也说明propc里的数据都需要通过leader发起提议才能处理的，前面总结了
            // proc里的数据都是需要有返回值的，这两点一结合，可以总结出：通过proc输入给node
            // 处理的数据都是需要同步到整个集群的(自己不是leader就把数据转发给leader实现)，
            // 而这类的数据处理必须有返回值告知用户是否成功，其他的数据通过recvc输入到node，
            // 这类数据只需要节点自己处理，不是集群性的，此时再看这两个chan的命名感觉很深刻了。
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		// 处理proc中的数据，当然是通过raft处理的，然后把raft的返回值在返回给调用者
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:	// 上层应用传进来的消息
			// filter out response message from unknown From.
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				// 交给step处理
				r.Step(m)
			}
		case cc := <-n.confc:
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
			outer:
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
							break outer
						}
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc:
			n.rn.Tick()
		case readyc <- rd: // 将ready数据push channel, 等待应用层pop进行处理
			n.rn.acceptReady(rd)
			advancec = n.advancec
		case <-advancec:
			// 生成ready数据
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

// 给自己发消息进入竞选状态
func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

// 包装confChange成message
func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	// 所有的非pb.MsgProp消息通过recvc送给node处理，此时是否wait根本不关心，因为通过recvc
    // 提交给node处理的消息可以理解为没有返回值的调用
	if m.Type != pb.MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select {
	// 如果流程没有走到这里，result的写入会堵塞。因此上面make的时候设置了一个缓冲区位
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

// 应用层通过chan拿到ready之后
// 1.将HardState, Entries, Snapshot持久化到storage
// 2.将Messages广播给其他节点
// 3.将CommittedEntries（已经commit还没有apply）应用到状态机
// 4.如果发现CommittedEntries中有成员变更类型的entry，调用node.ApplyConfChange()方法让node知道
// 5.最后再调用node.Advance()告诉raft，这批状态更新处理完了，状态已经演进了，可以给我下一批Ready让我处理
func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	// 把配置调整发送到confc
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	// 再通过confstatec把调整后的结果读出来
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	// 通过status把c送给node，让node通过c把Status输出
	case n.status <- c:
		// 此时再从c中把Status读出来
		return <-c
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}
	// 以下变量，设置等于输出
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	// 有不可靠日志、leader更换以及换届选举都需要设置同步标记，也就是说当有不可靠日志或者
    // 新一轮选举发生时必须等到这些数据同步到可靠存储后才能继续执行，这还算是比较好理解，毕竟
    // 这些状态是全局性的，需要leader统计超过半数可靠可靠以后确认为可靠的数据。如果此时采用
    // 异步实现，就会出现不一致的可能性。
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

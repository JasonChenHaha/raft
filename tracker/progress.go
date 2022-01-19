// Copyright 2019 The etcd Authors
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

package tracker

import (
	"fmt"
	"sort"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
type Progress struct {
	// Leader与Follower之间的状态同步是异步的，Leader将日志发给Follower，Follower再回复接收
    // 到了哪些日志。出于效率考虑，Leader不会每条日志以类似同步调用的方式发送给Follower，而是
    // 只要Leader有新的日志就发送，Next就是用来记录下一次发送日志起始索引。换句话说就是发送给Peer
    // 的最大日志索引是Next-1，而Match的就是经过Follower确认接收的最大日志索引，Next-Match-1
    // 就是还在飞行中或者还在路上的日志数量（Inflights）。Inflights还是比较形象的，下面会有详细
    // 说明。
	// 已发送成功和下一条要发送的提案index
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	// 此处顺便把StateType这个类型详细说明一下，StateType的代码在go.etcd.io/etcd/raft/tracker/state.go
    // Progress一共有三种状态，分别为探测（StateProbe）、复制（StateReplicate）、快照（StateSnapshot）
    // 探测：一般是系统选举完成后，Leader不知道所有Follower都是什么进度，所以需要发消息探测一下，从
    //    Follower的回复消息获取进度。在还没有收到回消息前都还是探测状态。因为不确定Follower是
    //    否活跃，所以发送太多的探测消息意义不大，只发送一个探测消息即可。
    // 复制：当Peer回复探测消息后，消息中有该节点接收的最大日志索引，如果回复的最大索引大于Match，
    //    以此索引更新Match，Progress就进入了复制状态，开启高速复制模式。复制制状态不同于
    //    探测状态，Leader会发送更多的日志消息来提升IO效率，就是上面提到的异步发送。这里就要引入
    //    Inflight概念了，飞行中的日志，意思就是已经发送给Follower还没有被确认接收的日志数据，
    //    后面会有inflight介绍。
    // 快照：快照状态说明Follower正在复制Leader的快照
	State StateType

	// PendingSnapshot is used in StateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	// 在快照状态时，快照的索引值
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//
	// TODO(tbg): the leader should always have this set to true.
	// 变量名字就能看出来，表示Follower最近是否活跃，只要Leader收到任何一个消息就表示节点是最近
    // 是活跃的。如果新一轮的选举，那么新的Leader默认为都是不活跃的。
	RecentActive bool

	// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
	// true, raft should pause sending replication message to this peer until
	// ProbeSent is reset. See ProbeAcked() and IsPaused().
	// 探测状态时才有用，表示探测消息是否已经发送了，如果发送了就不会再发了，避免不必要的IO。
	// 在发探测消息时为true, 如果探测消息得到回复，就会变为false
	ProbeSent bool

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	// 发送数据的滑动窗口
	// Inflight前面提到了，在复制状态有作用，后面有他的代码解析，此处只需要知道他是个限流的作用即可。
    // Leader不能无休止的向Follower发送日志，飞行中的日志量太大对网络和节点都是负担。而且一个日志
    // 丢失其后面的日志都要重发，所以过大的飞行中的日志失败后的重发成本也很大。
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
// 探测消息被peer确认
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
// 当leader刚刚当选时，或当follower拒绝了leader复制的日志时，该follower的进度状态会变为StateProbe类型。
// 在该状态下，leader每次心跳期间仅为follower发送一条MsgApp消息，且leader会根据follower发送的相应的MsgAppResp消息调整该follower的进度
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	// 探测附加到下一次的发送索引中(可能是snapshot或者普通消息)
	if pr.State == StateSnapshot {
		// peer接收完snapshot之后需要再探测一次才能继续发日志
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		// 在peer拒绝了日志, 或者消息丢失的情况下
		// leader会将peer从复制状态转为探测状态
		// 然后从已经明确的index(match)的下一条开始发送
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
// 该状态下的follower处于稳定状态，leader会优化为其复制日志的速度，每次可能发送多条MsgApp消息（受Progress的流控限制)
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
// 当follower所需的日志已被压缩无法访问时，leader会将该follower的进度置为StateSnapshot状态，并向该follower发送快照。
// leader不会为处于StateSnapshot状态的follower发送任何的MsgApp消息，直到其成功收到快照。
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	// 此处为什么不需要调整Next？因为这个状态无需在发日志给peer，直到快照完成后才能继续
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
// 收到MsgAppResp确认之后, 向前推进
// 更新Progress的状态，为什么有个maybe呢？因为不确定是否更新，raft代码中有很多maybeXxx系列函数，
// 大多是尝试性操作，毕竟是分布式系统，消息重发、网络分区可能会让某些操作失败。这个函数是在节点回复
// 追加日志消息时被调用的，在反馈消息中节点告知Leader日志消息中最后一条日志的索引已经被接收，就是
// 下面的参数n，Leader尝试更新节点的Progress的状态。
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		// 这个函数就是把ProbeSent设置为false，试问为什么在这个条件下才算是确认收到探测包？
        // 这就要从探测消息说起了，raft可以把日志消息、心跳消息当做探测消息，此处是把日志消息
        // 当做探测消息的处理逻辑。新的日志肯定会让Match更新，只有收到了比Match更大的回复才
        // 能算是这个节点收到了新日志消息，其他的反馈都可以视为过时消息。比如Match=9，新的日志
        // 索引是10，只有收到了>=10的反馈才能确定节点收到了当做探测消息的日志。
		pr.ProbeAcked()
	}
	// 什么时候next < n？Next是Leader认为发送给Peer最大的日志索引了，Peer怎么可能会回复一个
    // 比Next更大的日志索引呢？这个其实是在系统初始化的时候亦或是每轮选举完成后，新的Leader
    // 还不知道peer的接收的最大日志索引，所以此时的Next还是个初识值。
	pr.Next = max(pr.Next, n+1)
	return updated
}

// OptimisticUpdate signals that appends all the way up to and including index n
// are in-flight. As a result, Next is increased to n+1.
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index of the append message rejected by the follower, and
// the hint that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
// matchHint其实就是最后一条日志的索引(即MsgAppResp的RejectHint)
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	// 复制状态下Match是有效的，可以通过Match判断拒绝的日志是否已经无效了
	if pr.State == StateReplicate {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use matchHint if it's larger?
		// 源码注释：直接把Next调整到Match+1。源码注释还有一句是如果matchHint更大为什么不用他？
        // matchHint有可能比Match大么？让我们分析一下，因为在复制状态下Leader会发送多个日志信息
        // 给Peer再等待Peer的回复，例如：Match+1，Match+2，Match+3，Match+4，此时如果
        // Match+3丢了，那么Match+4肯定会被拒绝，此时matchHint应该是Match+2，Next=matchHint+1
        // 应该更合理。但是从peer的角度看，如果收到了Match+2的日志就会给leader一次回复，这个
        // 回复理论上是早于当前这个拒绝消息的，所以当Leader收到Match+4拒绝消息，此时的Match
        // 已经更新到Match+2，如果Peer回复的消息也丢包了Match可能也没有更新。所以Match+1
        // 大概率和matchHint相同，少数情况可能matchHint更好，但是用Match+1做可能更保险一点。
		pr.Next = pr.Match + 1
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	// 源码注释翻译：如果拒绝日志索引不是Next-1，肯定是陈旧消息。这是因为非复制状态探测消息一次只
    // 发送一条日志。这句话是什么意思呢，读者需要注意，代码执行到这里说明Progress不是复制状态，
    // 应该是探测状态。正常情况下，为了效率考虑，Leader向Peer发送日志消息一次会带多条日志，比如一个日志消息
    // 会带有10条日志。上面Match+1，Match+2，Match+3，Match+4的例子是为了理解方便假设每个
    // 日志消息一条日志。真实的情况是Message[Match,Match+9]，Message[Match+10,Match+15]，
    // 一个日志消息如果带有多条日志，Peer拒绝的是其中一条日志。此时用什么判断拒绝索引日志就在刚刚
    // 发送的探测消息中呢？所以探测消息一次只发送一条日志就能做到了，因为这个日志的索引肯定是Next-1。
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, matchHint+1), 1)
	// 因为节点拒绝了日志，如果这个日志是探测消息，那就再探测一次，ProbeSent=true的话，Leader
    // 就不会再发消息了
	pr.ProbeSent = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		// 探测状态下如果已经发送了探测消息Progress就暂停了，意味着不能再发探测消息了，前一个消息
        // 还没回复呢，如果节点真的不活跃，发再多也没用。
		return pr.ProbeSent
	case StateReplicate:
		return pr.Inflights.Full()
	case StateSnapshot:
		// 快照状态Progress就是暂停的，Peer正在复制Leader发送的快照，这个过程是一个相对较大
        // 而且重要的事情，因为所有的日志都是基于某一快照基础上的增量，所以快照不完成其他的都是徒劳。
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}

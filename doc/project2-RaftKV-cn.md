# Project2 RaftKV

Raft是一种共识算法，其设计意图即为易于理解。您可以在[The Raft site](https://raft.github.io/)上阅读有关Raft本身的材料，对Raft进行交互式可视化，以及其他资源，包括[扩展的Raft论文](https://raft.github.io/raft.pdf)。

在这个项目中，您将实现一个基于Raft的高可用kv服务器，该服务器不仅需要实现Raft算法，而且还需要实际使用它，并且给您带来更多挑战，例如使用`badger`管理Raft的持久状态，为快照消息添加流控制等。

这个项目中你要完成三件任务，包括：

- 实现基本的Raft算法
- 在Raft的基础上构建容错的KV服务器
- 添加对raftlog GC和快照的支持

## Part A

### 代码

在这一部分中，您将实现基本的Raft算法。 您需要实现的代码在`raft/`下。
在`raft/`中，有一些框架代码和测试用例在等着你去实现。
你需要在此处实现的Raft算法具有与上层应用程序一起精心设计的接口。
此外，它使用逻辑时钟（在此处命名为tick）来进行选举和心跳超时，而不是物理时钟。
也就是说，不要在Raft模块本身中设置计时器，并且上层应用程序要负责会通过调用`RawNode.Tick()`来使逻辑时钟前进。
除此之外，消息的发送和接收与其他东西以异步方式进行处理，上层应用程序决定了何时实际执行这些（请参见下文以获取更多详细信息）。
例如，Raft不会阻塞在任何请求消息的响应上。

动手实现之前，请先查看此部分的提示。
另外，你应该大致了解一下proto文件`proto/proto/eraftpb.proto`。
Raft发送和接收的消息以及相关结构的定义都在其中，您将使用它们进行实现。
请注意，与Raft论文不同，它将Heartbeat和AppendEntries划分为不同的消息，以使逻辑更加清晰。

这部分可分为3个步骤，包括：

- Leader 选举
- 日志复制
- Raw node 接口

### 实现Raft算法

`raft/raft.go`中的`raft.Raft`提供了Raft算法的核心，包括消息处理，驱动逻辑时钟等。
有关更多实现指南，请查看`raft/doc.go`，其中包含设计概述以及这些`MessageTypes`分别负责什么。

#### Leader 选举

要实现Leader选举，您可能要从`raft.Raft.tick()`开始，该命令用于让内部逻辑时钟前进一步，从而驱动选举超时或心跳超时。
您现在无需关心消息发送和接收逻辑。
如果您需要发送一条消息，只需将其推送到`raft.Raft.msgs`即可，所有Raft收到的信息将作为参数传递到`raft.Raft.Step()`。
测试代码将从`raft.Raft.msgs`获取消息，并通过`raft.Raft.Step()`传递响应。
`raft.Raft.Step()`是消息处理的入口，您应该处理诸如`MsgRequestVote`，`MsgHeartbeat`及其响应之类的消息。
并且还请实现测试用的桩函数，并正确调用它们，例如`raft.Raft.becomeXXX`，当Raft的角色发生变化时，该函数用于更新Raft的内部状态。

你可以运行 `make project2aa` 来测试你的实现，还可以看一看这个部分末尾处的提示。

#### Log replication

To implement log replication, you may want to start with handling `MsgAppend` and `MsgAppendResponse` on both the sender and receiver side. Checkout `raft.RaftLog` in `raft/log.go` which is a helper struct that help you manage the raft log, in here you also need to interact with the upper application by the `Storage` interface defined in `raft/storage.go` to get the persisted data like log entries and snapshot.

You can run `make project2ab` to test the implementation, and see some hints at the end of this part.

### Implement the raw node interface

`raft.RawNode` in `raft/rawnode.go` is the interface we interact with the upper application, `raft.RawNode` contains `raft.Raft` and provide some wrapper functions like `RawNode.Tick()`and `RawNode.Step()`. It also provides `RawNode.Propose()` to let the upper application propose new raft logs.

Another important struct `Ready` is also defined here. When handling messages or advancing the logical clock, the `raft.Raft` may need to interact with the upper application, like:

- send messages to other peer
- save log entries to stable storage
- save hard state like term, commit index and vote to stable storage
- apply committed log entries to the state machine
- etc

But these interactions do not happen immediately, instead they are encapsulated in `Ready` and returned by `RawNode.Ready()` to the upper application. It is up to the upper application when to call `RawNode.Ready()` and handle it.  After handling the returned `Ready`, the upper application also needs to call some functions like `RawNode.Advance()` to update `raft.Raft`'s internal state like the applied index, stabled log index, etc.

You can run `make project2ac` to test the implementation and run `make project2a` to test the whole part A.

> Hints:
>
> - Add any state you need to `raft.Raft`, `raft.RaftLog`, `raft.RawNode` and message on `eraftpb.proto`
> - The tests assume that the first time start raft should have term 0
> - The tests assume that the new elected leader should append a noop entry on its term
> - The tests doesn’t set term for the local messages, `MessageType_MsgHup`, `MessageType_MsgBeat` and `MessageType_MsgPropose`.
> - The log entries append are quite different between leader and non-leader, there are different sources, checking and handling, be careful with that.
> - Don’t forget the election timeout should be different between peers.
> - Some wrapper functions in `rawnode.go` can implement with `raft.Step(local message)`
> - When start a new raft, get the last stabled state from `Storage` to initialize `raft.Raft` and `raft.RaftLog`

## Part B

In this part you will build a fault-tolerant key-value storage service using the Raft module implemented in part A.  Your key/value service will be a replicated state machine, consisting of several key/value servers that use Raft for replication. Your key/value service should continue to process client requests as long as a majority of the servers are alive and can communicate, in spite of other failures or network partitions.

In project1 you have implemented a standalone kv server, so you should already be familiar with the kv server API and `Storage` interface.  

Before introducing the code, you need to understand three terms first: `Store`, `Peer` and `Region` which are defined in `proto/proto/metapb.proto`.

- Store stands for an instance of tinykv-server
- Peer stands for a Raft node which is running on a Store
- Region is a collection of Peers, also called Raft group

![region](imgs/region.png)

For simplicity, there would be only one Peer on a Store and one Region in a cluster for project2. So you don’t need to consider the range of Region now. Multiple regions will be further introduced in project3.

### The Code

First, the code that you should take a look at is  `RaftStorage` located in `kv/storage/raft_storage/raft_server.go` which also implements the `Storage` interface. Unlike `StandaloneStorage` which directly writes or reads from the underlying engine, it first sends every write or read request to Raft, and then does actual write and read to the underlying engine after Raft commits the request. Through this way, it can keep the consistency between multiple Stores.

`RaftStorage` mainly creates a `Raftstore` to drive Raft.  When calling the `Reader` or `Write` function,  it actually sends a `RaftCmdRequest` defined in `proto/proto/raft_cmdpb.proto` with four basic command types(Get/Put/Delete/Snap) to raftstore by channel(the receiver of the channel is `raftCh` of  `raftWorker`) and returns the response after Raft commits and applies the command. And the `kvrpc.Context` parameter of `Reader` and `Write` function is useful now, it carries the Region information from the perspective of the client and is passed as the header of  `RaftCmdRequest`. Maybe the information is wrong or stale, so raftstore needs to check them and decide whether to propose the request.

Then, here comes the core of TinyKV — raftstore. The structure is a little complicated, you can read the reference of TiKV to give you a better understanding of the design:

- <https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore>  (Chinese Version)
- <https://pingcap.com/blog/2017-08-15-multi-raft/#raftstore> (English Version)

The entrance of raftstore is `Raftstore`, see `kv/raftstore/raftstore.go`.  It starts some workers to handle specific tasks asynchronously,  and most of them aren’t used now so you can just ignore them. All you need to focus on is `raftWorker`.(kv/raftstore/raft_worker.go)

The whole process is divided into two parts: raft worker polls `raftCh` to get the messages, the messages includes the base tick to drive Raft module and Raft commands to be proposed as Raft entries; it gets and handles ready from Raft module, including send raft messages, persist the state, apply the committed entries to the state machine. Once applied, return the response to clients.

### Implement peer storage

Peer storage is what you interact with through the `Storage` interface in part A, but in addition to the raft log, peer storage also manages other persisted metadata which is very important to restore the consistent state machine after restart. Moreover, there are three important states defined in `proto/proto/raft_serverpb.proto`:

- RaftLocalState: Used to store HardState of the current Raft and the last Log Index.
- RaftApplyState: Used to store the last Log index that Raft applies and some truncated Log information.
- RegionLocalState: Used to store Region information and the corresponding Peer state on this Store. Normal indicates that this Peer is normal, Applying means this Peer hasn’t finished the apply snapshot operation and Tombstone shows that this Peer has been removed from Region and cannot join in Raft Group.

These states are stored in two badger instances: raftdb and kvdb:

- raftdb stores raft log and `RaftLocalState`
- kvdb stores key-value data in different column families, `RegionLocalState` and `RaftApplyState`. You can regard kvdb as the state machine mentioned in Raft paper

The format is as below and some helper functions are provided in `kv/raftstore/meta`, and set them to badger with `writebatch.SetMeta()`.

| Key            | KeyFormat                      | Value          | DB |
|:----           |:----                           |:----           |:---|
|raft_log_key    |0x01 0x02 region_id 0x01 log_idx|Entry           |raft|
|raft_state_key  |0x01 0x02 region_id 0x02        |RaftLocalState  |raft|
|apply_state_key |0x01 0x02 region_id 0x03        |RaftApplyState  |kv  |
|region_state_key|0x01 0x03 region_id 0x01        |RegionLocalState|kv  |

> You may wonder why TinyKV needs two badger instances. Actually, it can use only one badger to store both raft log and state machine data. Separating into two instances is just to be consistent with TiKV design.

These metadatas should be created and updated in `PeerStorage`. When creating PeerStorage, see `kv/raftstore/peer_storager.go`. It initializes RaftLocalState, RaftApplyState of this Peer or gets the previous value from the underlying engine in the case of restart. Note that the value of both RAFT_INIT_LOG_TERM and RAFT_INIT_LOG_INDEX is 5 (as long as it's larger than 1) but not 0. The reason why not set it to 0 is to distinguish with the case that peer created passively after conf change. You may not quite understand it now, so just keep it in mind and the detail will be described in project3b when you are implementing conf change.

The code you need to implement in this part is only one function:  `PeerStorage.SaveReadyState`, what this function does is to save the data in `raft.Ready` to badger, including append log entries and save the Raft hard state.

To append log entries, simply save all log entries at `raft.Ready.Entries` to raftdb and delete any previously appended log entries which will never be committed. Also update the peer storage’s `RaftLocalState` and save it to raftdb.

To save the hard state is also very easy, just update peer storage’s `RaftLocalState.HardState` and save it to raftdb.

> Hints:
>
> - Use `WriteBatch` to save these states at once.
> - See other functions at `peer_storage.go` for how to read and write these states.

### Implement Raft ready process

In project2 part A, you have built a tick based Raft module. Now you need to write the outer process to drive it. Most of the code is already implemented under `kv/raftstore/peer_msg_handler.go` and `kv/raftstore/peer.go`.  So you need to learn the code and finish the logic of `proposeRaftCommand` and `HandleRaftReady`. Here are some interpretations of the framework.

The Raft `RawNode` is already created with `PeerStorage` and stored in `peer`. In the raft worker, you can see that it takes the `peer` and wraps it by `peerMsgHandler`.  The `peerMsgHandler` mainly has two functions: one is `HandleMsgs` and the other is `HandleRaftReady`.

`HandleMsgs` processes all the messages received from raftCh, including `MsgTypeTick` which calls `RawNode.Tick()`  to drive the Raft, `MsgTypeRaftCmd` which wraps the request from clients and `MsyTypeRaftMessage` which is the message transported between Raft peers. All the message types are defined in `kv/raftstore/message/msg.go`. You can check it for detail and some of them will be used in the following parts.

After the message is processed, the Raft node should have some state updates. So `HandleRaftReady` should get the ready from Raft module and do corresponding actions like persisting log entries, applying committed entries
and sending raft messages to other peers through the network.

In a pseudocode, the raftstore uses Raft like:

``` go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```

After this the whole process of a read or write would be like this:

- Clients calls RPC RawGet/RawPut/RawDelete/RawScan
- RPC handler calls `RaftStorage` related method
- `RaftStorage` sends a Raft command request to raftstore, and waits for the response
- `RaftStore` proposes the Raft command request as a Raft log
- Raft module appends the log, and persist by `PeerStorage`
- Raft module commits the log
- Raft worker executes the Raft command when handing Raft ready, and returns the response by callback
- `RaftStorage` receive the response from callback and returns to RPC handler
- RPC handler does some actions and returns the RPC response to clients.

You should run `make project2b` to pass all the tests. The whole test is running a mock cluster including multiple TinyKV instances with a mock network. It performs some read and write operations and checks whether the return values are as expected.

To be noted, error handling is an important part of passing the test. You may have already noticed that there are some errors defined in `proto/proto/errorpb.proto` and the error is a field of the gRPC response. Also, the corresponding errors which implements `error` interface are defined in `kv/raftstore/util/error.go`, so you can use them as a return value of functions.

These error are mainly related with Region. So it is also a member of `RaftResponseHeader` of `RaftCmdResponse`. When proposaling a request or applying a command, there may be some errors. If that, you should return the raft command response with the error, then the error will be further passed to gRPC response. You can use `BindErrResp` provided in `kv/raftstore/cmd_resp.go` to convert these errors to errors defined in `errorpb.proto` when returning the response with error.

In this stage, you may consider these errors, and others will be processed in project3:

- ErrNotLeader: the raft command is proposed on a follower. so use it to let client try other peers.
- ErrStaleCommand: It may due to leader changes that some logs are not committed and overrided with new leaders’ logs. But client doesn’t know that and is still waiting for the response. So you should return this to let client knows and retries the command again.

> Hints:
>
> - `PeerStorage` implements the `Storage` interface of  Raft module, you should use the provided method  `SaveRaftReady()` to persist the Raft related states.
> - Use `WriteBatch` in `engine_util` to make multiple writes atomically, for example, you need to make sure to apply the committed entries and update the applied index in one write batch.
> - Use `Transport` to send raft messages to other peers, it’s in the `GlobalContext`,
> - The server should not complete a get RPC if it is not part of a majority and do not has up-to-date data. You can just put the get operation into the raft log, or implement the optimization for read-only operations that is described in Section 8 in the Raft paper.
> - Do not forget to update and persist the apply state when applying the log entries.
> - You can apply the committed Raft log entries in an asynchronous way just like TiKV does. It’s not necessary, though a big challenge to improve performance.
> - Record the callback of the command when proposing, and return the callback after applying.
> - For the snap command response, should set badger Txn to callback explicitly.

## Part C

As things stand now with your code, it's not practical for a long-running server to remember the complete Raft log forever. Instead, the server will check the number of Raft log, and discard log entries exceeding the threshold from time to time.

In this part, you will implement the Snapshot handling based on the above two part implementation. Generally, Snapshot is just a raft message like AppendEntries used to replicate data to follower, what make it different is its size, Snapshot contains the whole state machine data in some point of time, and to build and send such a big message at once will consume many resource and time, which may block the handling of other raft message, to amortize this problem, Snapshot message will use an independent connect, and split the data into chunks to transport. That’s the reason why there is a snapshot RPC API for TinyKV service. If you are interested in the detail of sending and receiving, check `snapRunner` and the reference <https://pingcap.com/blog-cn/tikv-source-code-reading-10/>

### The Code

All you need to change is based on the code written in part A and part B.

### Implement in Raft

Although we need some different handling for Snapshot messages, in the perspective of raft algorithm there should be no difference. See the definition of `eraftpb.Snapshot` in the proto file, the `data` field on `eraftpb.Snapshot` does not represent the actual state machine data but some metadata used for the upper application you can ignore it for now. When the leader needs to send a Snapshot message to a follower, it can call `Storage.Snapshot()` to get a `eraftpb.Snapshot`, then send the snapshot message like other raft messages. How the state machine data is actually built and sent are implemented by the raftstore, it will be introduced in the next step. You can assume that once `Storage.Snapshot()` returns successfully, it’s safe for Raft leader to the snapshot message to the follower, and follower should call `handleSnapshot` to handle it, which namely just restore the raft internal state like term, commit index and membership information, ect, from the
`eraftpb.SnapshotMetadata` in the message, after that, the procedure of snapshot handling is finish.

#### Implement in raftstore

In this step, you need to learn two more workers of raftstore — raftlog-gc worker and region worker.

Raftstore checks whether it needs to gc log from time to time based on the config `RaftLogGcCountLimit`, see `onRaftGcLogTick()`. If yes, it will propose a raft admin command `CompactLogRequest` which is wrapped in `RaftCmdRequest` just like four basic command types(Get/Put/Delete/Snap) implemented in project2 part B. Then you need to  process this admin command when it’s committed by Raft. But unlike Get/Put/Delete/Snap commands write or read state machine data, `CompactLogRequest` modifies metadata, namely updates the `RaftTruncatedState` which is in the `RaftApplyState`. After that, you should schedule a task to raftlog-gc worker by `ScheduleCompactLog`. Raftlog-gc worker will do the actual log deletion work asynchronously.

Then due to the log compaction, Raft module maybe needs to send a snapshot. `PeerStorage` implements `Storage.Snapshot()`. TinyKV generates snapshots and applies snapshots in the region worker. When calling `Snapshot()`, it actually sends a task `RegionTaskGen` to the region worker. The message handler of the region worker is located in `kv/raftstore/runner/region_task.go`. It scans the underlying engines to generate a snapshot, and sends snapshot metadata by channel. At the next time of Raft calling `Snapshot`, it checks whether the snapshot generating is finished. If yes, Raft should send the snapshot message to other peers, and the snapshot sending and receiving work is handled by `kv/storage/raft_storage/snap_runner.go`. You don’t need to dive into the details, only should know the snapshot message will be handled by `onRaftMsg` after the snapshot is received.

Then the snapshot will reflect in the next Raft ready, so the task you should do is to modify the raft ready process to handle the case of snapshot. When you are sure to apply the snapshot, you can update the peer storage’s memory state like `RaftLocalState`, `RaftApplyState` and `RegionLocalState`. Also don’t forget to persist these states to kvdb and raftdb and remove stale state from kvdb and raftdb. Besides, you also need to update
`PeerStorage.snapState` to `snap.SnapState_Applying` and send `runner.RegionTaskApply` task to region worker through `PeerStorage.regionSched` and wait until region worker finish.

You should run `make project2c` to pass all the tests.

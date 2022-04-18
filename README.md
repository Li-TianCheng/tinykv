# 整体结构
![](https://github.com/Li-TianCheng/tinykv/blob/main/doc/imgs/%E6%95%B4%E4%BD%93%E7%BB%93%E6%9E%84.png)
## raftStorage
* raftStorage中interface注册到grpcServer中,详见```tinykvpb.RegisterTinyKvServer```
* 网络消息通过grpcServer路由,分别调用Raft(),Snapshot()接口
* snap worker负责snapshot的发送和接收,当snapshot接收完毕后,将SendRaftMessage到下层的raftStore进行处理
* resolve worker负责解析消息地址
* Raft()接口将SendRaftMessage到下层的raftStore进行处理
* Snapshot()接口将向snap worker发送一个recvSnapTask,对snapshot进行异步接收
## node
* node是对raftStore的封装,并添加了一些其他信息
## raftStore
* tick driver负责定时发送MsgTypeTick消息驱动raft
* split check worker负责检测是否需要split
* raft log gc worker负责接收RaftLogGCTask,并对日志进行清理
* region worker负责异步生成snapshot,以及snapshot的应用
* scheduler worker负责与Scheduler进行交换
* store worker负责生成新的peer
* raft worker负责处理从上层接收到的消息
* 上层raftStorage将消息通过router路由到相应的peer,如果peer不存在,则发送消息到store worker来生成相应peer,否则发送消息到raft worker进行处理
* raft worker收到消息,生成相应的peerMsgHandler对消息进行处理,并驱动下层RawNode
* peerMsgHandler对RawNode的Ready进行相应处理,持久化到raftDB和kvDB中,并将消息发送(如果是snapshot则向snap worker发送sendSnapTask)
* raftDB中保存raft log以及RaftLocalState(hard state(Term,Vote,Commit),LastIndex,LastTerm)
* kvDB中保存kv数据,RegionLocalState以及RaftApplyState(AppliedIndex,RaftTruncatedState)
# Raft算法层
![](https://github.com/Li-TianCheng/tinykv/blob/main/doc/imgs/raft.png)
* RawNode为上层peerMsgHandler提供几个调用接口
* RawNode利用Ready向上层peerMsgHandler传递信息,peerMsgHandler拿到Ready后负责对消息的发送及log的持久化等操作
* RawNode被上层调用后,调用Raft的Step()来向Raft传递消息,驱动Raft
* Raft拿到消息后,进行处理,并更新状态,以及RaftLog

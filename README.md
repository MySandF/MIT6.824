# MIT6.824 实验进度记录
## lab1 4/27~4/29
- BUG：卡死，原因：定时器到时间没唤醒条件变量，导致协程一直等待
- BUG:   data race，work_state没加锁
## lab2A 5/5~5/8
- BUG:定时器reset后仍然触发，由于定时器特性，Stop()函数可能返回false  
解决：自定义定时器，sleep50ms记一次数，超过timeout时间触发事件
- 处理事件时首先判断raft是否被kill
- 可以PASS测试，但依然存在BUG：两个election timeout在同一50ms内会出问题，会同时收到另一server的投票  
已解决：Raft.votefor变量需要加锁
## lab2B 5/14~5/19
- follower能正确接收entry，但是测试不通过  
解决：  
   1. 没有向applyCh发送ApplyMsg  
   2. 且leader没有发送
- 5/16: 重写RequestVote逻辑，通过前三小节
- 5/17: 完善AppendEntries，通过前五小节
- 5/18: 完善commit机制(只有当前Term的entry被commit之后，之前的entry才能被commit)，通过前六小节，5、6留有bug  
   有时会多出几个server，
- 5/19: 完善AppendEntries，检查最后一个Entry的Index和Term是否匹配

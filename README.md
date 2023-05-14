# MIT6.824 实验进度记录
## lab1 4/27~4/29
BUG：卡死，原因：定时器到时间没唤醒条件变量，导致协程一直等待
BUG:   data race，work_state没加锁
## lab2A 5/5~5/8
BUG:定时器reset后仍然触发，由于定时器特性，Stop()函数可能返回false
自定义定时器，sleep50ms记一次数，超过timeout时间触发事件
处理事件时首先判断raft是否被kill
可以PASS测试，但依然存在BUG：两个election timeout在同一50ms内会出问题，会同时收到另一server的投票  （已解决：Raft.votefor变量需要加锁）
## lab2B

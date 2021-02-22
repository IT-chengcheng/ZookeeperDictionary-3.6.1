# 基于 Zookeeper 3.6.1 版本的“源码”分析

## 涉及内容,包括但不限于：
+ 1、zookeeper服务端，客户端启动流程
+ 2、IO模型，线程模型，以及各种RequestProcessor的分析
+ 3、单个命令的完整执行过程分析，比如create del  ping
+ 4、内存存储的数据结构
+ 5、session机制，session创建，管理，激活，清理，重连等源码分析
+ 6、领导选举 源码分析
+ 7、leader  follower 两阶段提交 源码分析
+ 8、watch机制 客户端，服务端源码分析
+ 9、······等等
## 研究Zookeeper源码前提
+ 1、精通JAVA-NIO编程
+ 2、熟读Zookeeper官网，熟练使用Zookeeper各种API，只有熟练使用，研究源码才有具体的方向！！
+ 3、本部作品就是一部Zookeeper**源码字典**，目的就是帮助想要精通Zookeeper源码的朋友尽快精通Zookeeper。

  
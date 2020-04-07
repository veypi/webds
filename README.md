# webds
Distributed System with websocket

## 设计

- 整体结构

![](doc/webds.png)

    Server层为核心服务层，通过选举产生一个中心节点.
    SubServer层实例不可被选举为中心节点，但可以跨网段部署，其余与server节点等效
    client层节点可以连接任意server节点
    每个server节点都有维护 superior/lateral master 列表, 用以记录上级和平级的节点.
    每个节点最多只有一个实际往出连通的平级或上级的连接,用以维护数据广播的最终一致性
    平级连接之间共享 superior/lateral master 列表

- 通信方式

        websocket

- 通信协议

        主要以多级topic设计
        参考iris  序列化成字节流，格式: prefix;topic;type;msg
        json or protobuf
        保留下列1级topic，其余topic用于分发
            /sys  用于系统指令
            /inner 用于client 连接的Server直接处理，不进行广播, 响应函数由 conn.On 函数指定

- 命令行工具设计

        参照ros命令行设计：形如
            - webds topic list/pub/sub
            - webds node list/stop

## TODO

- client/go/py/js

- command tools

- 分布式，选举产生中心通信节点

## update

- v0.2.2 修改了OnConnection的回调函数, 若回调函数返回error, 则 conn 直接关闭, 该功能可用于鉴定权限. 优化消息解析代码. 初步开始设计分布式结构.
- v0.2.1 添加webds node list/stop 指令
- v0.2.0 基于topic订阅机制初步重构完 server， go.client，command tool
- v0.1.0 old version just a websocket server

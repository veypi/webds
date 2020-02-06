# webds
Distributed System with websocket

referring to github.com/kataras/iris

## 设计

- 整体结构

![](doc/webds.png)

Server层为核心服务层，通过选举产生一个中心节点
SubServer层实例不可被选举为中心节点，但可以跨网段部署，其余与server节点等效
client层节点可以连接任意server节点

- 通信方式

        websocket

- 通信协议

        未定 主要以订阅topic形式设计

- 命令行工具设计

        参照ros命令行设计：形如
            - webds topic list/pub/echo

## TODO

- 分布式，选举产生中心通信节点

- 连接权限校验, 话题发布校验


## update

- 0.1.0 old version just a websocket server

# dubbo-java-polaris

[![codecov](https://codecov.io/gh/polarismesh/dubbo-java-polaris/branch/dubbo-2.7.x/graph/badge.svg?token=I9fctxnRWi)](https://app.codecov.io/gh/polarismesh/dubbo-java-polaris/tree/dubbo-2.7.x)
[![Testing](https://github.com/polarismesh/dubbo-java-polaris/actions/workflows/testing.yml/badge.svg?branch=dubbo-2.7.x)](https://github.com/polarismesh/dubbo-java-polaris/actions/workflows/testing.yml)

## 介绍

dubbo-java-polaris 是 [Apache Dubbo](https://github.com/apache/dubbo) 框架的扩展，便于使用 Dubbo 框架开发的应用接入并使用北极星的各部分功能。

当前支持版本到 3.2.x、2.7.x。

> **接入文档**：[接入指南.md](接入指南.md)（面向本仓库 Dubbo 2.7.x / `2.1.2.0-2.7.23`）

## 插件功能说明

### 服务注册发现

实现 Dubbo 服务往北极星上进行注册，以及服务调用时从北极星拉取服务实例的功能。相关插件：

- Apache Dubbo：`dubbo-registry-polaris`

### 动态路由

对接北极星路由规则（含就近路由），在调用前过滤目标实例。相关插件：

- Apache Dubbo：`dubbo-router-polaris`

### 负载均衡

提供加权随机、加权轮询、一致性哈希、最短响应、最少连接等策略。相关插件：

- Apache Dubbo：`dubbo-loadbalance-polaris`

### 配置中心

对接北极星配置中心，作为 Dubbo 动态配置源。相关插件：

- Apache Dubbo：`dubbo-configcenter-polaris`

### 限流

Provider 侧按北极星限流规则进行流量控制。相关插件：

- Apache Dubbo：`dubbo-ratelimit-polaris`（需单独引入，未包含在 `dubbo-polaris-all`）

### 熔断

Consumer 侧按北极星熔断规则进行故障隔离。相关插件：

- Apache Dubbo：`dubbo-circuitbreaker-polaris`（需单独引入，未包含在 `dubbo-polaris-all`）

### 元数据上报（目前仅 3.2.x 版本支持）

实验 Dubbo 服务的元数据（服务、接口信息）上报到北极星，以及服务消费方拉取服务提供方的元数据信息。相关插件：

- Apache Dubbo：`dubbo-metadatareport-polaris`

## 快速开始

```xml
<dependency>
    <groupId>com.tencent.polaris</groupId>
    <artifactId>dubbo-polaris-all</artifactId>
    <version>2.1.2.0-2.7.23</version>
</dependency>
```

```properties
# Provider / Consumer 公共
dubbo.registry.address=polaris://127.0.0.1:8091
dubbo.config-center.address=polaris://127.0.0.1:8093

# Consumer 建议开启调用结果上报
dubbo.consumer.filter=polaris_report
dubbo.consumer.loadbalance=polaris_weighted_random
```

完整步骤、参数说明与示例运行方式见 [接入指南](接入指南.md)。本地可参考 `dubbo-examples/dubbo-quickstart-example`。

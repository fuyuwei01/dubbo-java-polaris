# Dubbo 3 Quick Provider 多服务压测

用于给 `dubbo-discovery-example/dubbo-quick-provider`（Dubbo 3.2.x）批量生成 Dubbo 接口和实现，并在注册时给每个服务注入可调大小的实例 metadata（Polaris URL 参数）。默认走现有 `@EnableDubbo` 扫描，不必改 `Main.java`。

2.7.x 对应分支：`feat/quickstart-metadata-press`（目标模块是 `dubbo-quickstart-provider`）。

## 生成 / 扩缩容服务数量

在仓库根目录：

```bash
./dubbo-examples/scripts/gen-press-services.sh gen 50   # 生成正好 50 个服务（多删少补）
./dubbo-examples/scripts/gen-press-services.sh count    # 当前已生成数量
./dubbo-examples/scripts/gen-press-services.sh gen 10   # 缩到 10 个
./dubbo-examples/scripts/gen-press-services.sh clean    # 删除 PressService* 源码，保留 PressMetadataPadding
./dubbo-examples/scripts/gen-press-services.sh build    # 编译 api + provider
```

生成结果：

| 产物 | 路径 |
|---|---|
| 接口 | `dubbo-api-example/.../api/press/PressServiceNNNN.java` |
| 实现 | `dubbo-quick-provider/.../provider/press/PressServiceNNNNImpl.java` |

实现类只有 `@DubboService(parameters = {"press.meta.index", "N"})`。padding 不写进源码，由运行时注入。

`clean` 不会删除 `PressMetadataPadding.java`。

## 调整每个服务的 metadata 大小

`PressMetadataInjector` 在 export 前调用 `PressMetadataPadding.parameters()`，把 padding 拆成若干 URL 参数（每段不超过 `CHUNK_BYTES`，默认 48000）写进 `ServiceConfig`，随后 `PolarisRegistry` 会把这些参数注册为实例 metadata。

优先级：**JVM 参数 / 环境变量 > 类里的 `DEFAULT_BYTES`**。

1. 改默认值（需重新编译）：

```java
// dubbo-api-example/.../press/PressMetadataPadding.java
public static final int DEFAULT_BYTES = 512 * 1024 + 1024; // 超过 512KB
```

2. 启动时覆盖（不必改代码）：

```bash
export PRESS_METADATA_BYTES=525312
# 或
-Dpress.metadata.bytes=525312
```

3. 代码里直接指定：

```java
PressMetadataPadding.parameters(525312);
PressMetadataPadding.parameters(); // 走 resolveBytes()
```

`PRESS_METADATA_BYTES=0` 表示不注入 padding。

## 启动 provider

先改 `dubbo-quick-provider/src/main/resources/spring/dubbo-provider.properties` 里的北极星地址。

当前示例默认 `dubbo.application.register-mode=instance`（应用级注册）。若要每个接口在北极星上单独成一个服务，改成：

```properties
dubbo.application.register-mode=interface
# 或 all
```

然后：

```bash
./dubbo-examples/scripts/gen-press-services.sh gen 50
./dubbo-examples/scripts/gen-press-services.sh build
```

在 IDE 中运行 `com.tencent.polaris.dubbo.discovery.example.provider.Main`。需要覆盖 padding 时，加上 VM options `-Dpress.metadata.bytes=525312`，或启动前 `export PRESS_METADATA_BYTES=525312`。

原有 `GreetingService` / `EchoService` 仍会一起暴露；只有 `...api.press.PressService*` 会注入 padding。

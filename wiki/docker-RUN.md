# 使用 Docker Compose 部署

[文档目录](README.zh_CN.md) · [English](docker-RUN.en.md)

使用部署版本自带的 [Compose 配置](../docker-compose.yaml)。默认支付服务内置 ChainScanner。

## 启动网关

在仓库根目录设置 Compose 环境变量（例如写入项目根目录的 `.env`）。入口脚本每次启动都会读取公开收银台地址。

```dotenv
APP_URI=https://pay.example.com
EPUSDT_PORT=8000
```

```sh
docker compose up -d epusdt
docker compose logs --tail=100 epusdt
curl -fsS http://127.0.0.1:8000/payments/gmpay/v1/config
```

将 HTTPS 反向代理指向映射端口。在后台添加收款地址后，检查响应中的 `data.supported_assets`。

## 镜像与标签

默认 Compose 配置使用 Docker Hub 的 `gmwallet/epusdt`。同一 Dockerfile 构建的镜像也发布到 GitHub Container Registry（GHCR）：`ghcr.io/gmwalletapp/epusdt`。两处均提供 `linux/amd64` 和 `linux/arm64`。

GHCR 标签如下。Docker Hub 的版本标签带 `v` 前缀（如 `v1.2.3`），GHCR 不带。

| 标签 | 内容 |
| --- | --- |
| `latest` | 最新正式版本 |
| `1.2.3`、`1.2` | 指定正式版本；`1.2` 随该系列的补丁版本更新 |
| `1.2.3-rc1` | 候选版本，不更新 `latest` |
| `edge` | `master` 最新提交，未经发布，不用于生产 |
| `sha-<提交号前 7 位>` | `master` 上的某次提交 |

```sh
docker pull ghcr.io/gmwalletapp/epusdt:latest
```

改用 GHCR 时，将 Compose 文件中两处 `image:` 改为 GHCR 地址并写明所需标签，然后拉取镜像再启动：

```sh
docker compose pull epusdt
docker compose up -d epusdt
```

Fork 仓库的工作流发布到 `ghcr.io/<owner>/<repo>`（全小写）。新包默认私有；拉取时报 `denied` 或 `unauthorized`，请仓库维护者按[发布说明](../RELEASING.zh_CN.md#容器镜像)将包设为公开。

## 持久化与升级

Compose 将宿主机 `./runtime/epusdt` 挂载到容器的 `/app/runtime`。其中包含应用的 `.env`、业务库、运行库和内置扫描器数据库。项目根目录的 Compose 环境文件与此处的应用配置文件分别维护。

从 v2.0.0 升级时，先停止旧服务，备份配置和数据库，**在重建旧容器之前**将数据迁移到新挂载目录。保留原备份，并核对配置中的数据库文件名。[升级说明](https://github.com/GMWalletApp/epusdt/releases/tag/v2.1.0)

## 使用独立扫描器

设置 `CHAINSCANNER_URL=http://chainscanner:9081`，配置不同的消费者令牌和管理员令牌。升级已有独立部署时，应保留原扫描器数据卷（`./runtime/chainscanner`）、连接地址和凭据。

```sh
docker compose --profile scanner up -d
```

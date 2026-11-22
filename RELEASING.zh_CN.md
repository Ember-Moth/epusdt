# 发布发行版

[文档目录](wiki/README.zh_CN.md) · [English](RELEASING.md)

推送以 `v` 开头的标签会触发 GitHub Actions，通过 GoReleaser 发布二进制包。[发布工作流](.github/workflows/release.yml) · [打包配置](src/.goreleaser.yaml)

## 发布内容

Linux、macOS、Windows 均构建 amd64 和 arm64 两种架构。压缩包包含 `epusdt`、`chainscanner` 和 `.env.example`。Windows 使用 ZIP，其他平台使用 tar.gz。发行页同时提供 `SHA256SUMS` 与 GitHub 构建来源证明。

## 容器镜像

同一 `v*` 标签还会构建 `linux/amd64` 与 `linux/arm64` 镜像：

- [`docker-alpine`](.github/workflows/docker-alpine.yml) 推送到 Docker Hub 的 `gmwallet/epusdt`，需要 `DOCKERHUB_USERNAME` 和 `DOCKERHUB_TOKEN` 两个密钥。
- [`ghcr-image`](.github/workflows/ghcr.yml) 使用 `GITHUB_TOKEN` 推送到 `ghcr.io/<owner>/<repo>`（全小写）。正式标签生成 `X.Y.Z`、`X.Y` 和 `latest`，候选版本只生成 `X.Y.Z-rc1`。每次推送到 `master` 还会发布 `edge` 和 `sha-<短提交号>`；拉取请求只构建、不推送。

设置了仓库变量 `API_RATE_URL` 时，两个工作流都会将其传入构建。

### 将 GHCR 包设为公开

GHCR 在首次推送时创建的包默认私有。`ghcr-image` 首次成功运行后操作一次：

1. 从仓库侧栏的 Packages 或所有者主页的 Packages 标签页打开该包，进入 **Package settings**。
2. 在 **Danger Zone** 中将可见性改为 **Public**。
3. 确认包页面显示本仓库。镜像的 `org.opencontainers.image.source` 标签通常会自动关联；未关联时，在包页面点击 **Connect repository**。

组织仓库还需在 **Organization settings → Packages → Package creation** 中允许创建公开包。

## 验证并打标签

1. 确认待发布提交已进入默认分支。在 `src` 目录运行快照构建，检查 `src/dist/` 中的产物。

   ```sh
   cd src
   go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean
   ```

2. 在该提交上创建附注标签并推送。以下版本号仅为示例。

   ```sh
   git tag -a v1.0.0 -m "release v1.0.0"
   git push origin v1.0.0
   ```

3. 等待 `release` 工作流完成，从发行页下载校验文件和对应平台的压缩包。验证构建来源及下载包的 SHA-256。

   ```sh
   gh attestation verify SHA256SUMS -R GMWalletApp/epusdt
   ```

## 候选版本

`v1.2.3-rc1` 一类标签将标为预发布。正式标签（如 `v1.2.3`）的更新日志与上一个稳定版本比较。

# 奔奔王国批量兑换

一个用于管理《奔奔王国》账号、批量兑换礼包码，并自动抓取新兑换码后触发兑换的 Web 工具。

## 技术栈

- Node.js 22+
- TypeScript
- Express
- sqlite3

## 主要功能

- 批量新增账号，按行导入并自动去重
- 账号列表管理，支持搜索、删除、清空、拉黑状态
- 批量兑换，支持失败重试和进度实时推送
- 自动保存和刷新兑换站点 `REDEEM_TOKEN`
- 自动抓取兑换码并写入 `redeem_codes`
- 新兑换码自动给当前所有账号兑换
- 新增账号后自动兑换最新一个兑换码，批量新增时会按队列执行
- 支持管理员账号和临时账号两种登录角色
- 支持访问日志和 IP 黑名单

## 兑换码来源

项目支持两种兑换码来源，但同一进程只会启用其中一种。

### 默认模式：TapTap

不带额外参数启动时，默认抓取 TapTap《奔奔王国》官方公告：

```text
https://www.taptap.cn/app/759692/topic?type=official
```

服务端会读取 TapTap 官方帖子列表接口，解析帖子标题和正文中的兑换码。

### 微信模式：微信公众号

使用微信模式启动时，不会再启动 TapTap 抓取。

微信模式会登录微信公众平台后台，定时检查目标公众号文章列表。默认目标公众号 `fakeid`：

```text
MzE5MTIzOTAzNQ==
```

文章列表中已经包含文章详情链接，服务端会使用类似下面的微信移动端 User-Agent 请求详情页：

```text
https://mp.weixin.qq.com/s/xxxxxx
```

然后从文章正文中解析兑换码。

微信模式会优先复用上一次登录保存的 `token/cookie/userAgent`。启动时会先校验 `config.json` 中的微信登录态；校验通过就直接启动微信轮询，不再扫码。校验失败或没有历史登录态时，终端才会输出二维码；扫码并在手机上确认后，会把新的登录态保存到 `config.json`。如果终端二维码无法识别，也会保存本地二维码图片到 `data/wechat-login-qrcode.jpg`。

## 抓取规则

- 服务启动后会立即抓取一次。
- 服务运行期间每 1 分钟抓取一次。
- 抓到的兑换码会写入 `redeem_codes` 表。
- 同一个兑换码按 `code` 去重。
- 重复抓到同一个兑换码时，只更新来源标题、正文、发布时间和最后发现时间。
- 只解析标题或正文中类似 `兑换码：xxx`、`礼包码：xxx`、`CDK：xxx` 的内容。
- TapTap 兑换码发布时间来自帖子发布时间。
- 微信兑换码发布时间来自微信公众号文章发布时间。
- 微信模式会把文章记录写入 `wechat_articles` 表。
- 微信登录态失效时，会停止微信抓取并输出日志，但不会影响 Web 服务继续运行。

## 自动兑换规则

- 只有新入库的兑换码会触发自动兑换。
- 只自动兑换最近 24 小时发布的兑换码。
- 超过 24 小时的历史兑换码只入库，不自动兑换。
- 如果一次抓到多个新兑换码，会按队列顺序逐个兑换，不会并发执行。
- 自动兑换期间会暂停当前启用的兑换码来源轮询，兑换结束后恢复。
- 自动兑换前会检查 `REDEEM_TOKEN`；如果为空或需要刷新，会自动从兑换站点抓取并保存。
- 每个兑换码会在 `redeem_code_redemptions` 表记录兑换状态，避免重复触发。
- 自动兑换会对当前所有未删除、未拉黑账号执行。
- 自动兑换结束后，如果存在失败账号，会只针对失败账号额外重试 1 次，避免死循环。
- 自动兑换结果会记录总数、成功数、已领取数、失败数和错误信息。

## 启动方式

安装依赖：

```bash
npm install
```

开发模式：

```bash
npm run dev
```

生产模式：

```bash
npm run build
npm run start
```

默认地址：

```text
http://localhost:3458
```

## 微信模式启动

生产模式推荐使用：

```bash
npm run build
npm run start:wechat
```

开发模式可以使用：

```bash
npm run dev -- --wechat
```

说明：

- `npm run start:wechat` 等价于 `node dist/server.js --wechat`。
- 微信模式启动后会先校验 `config.json` 中已有的公众号后台登录态。
- 已有登录态有效时会跳过扫码，直接启动微信文章轮询。
- 没有历史登录态或登录态失效时，才会进入扫码登录流程。
- 扫码成功后会保存新的 `token/cookie/userAgent`，供下次启动校验复用。
- 如果扫码失败、超时或登录态不可用，Web 服务仍会启动，只是不启用微信轮询。
- 如果运行中微信登录态过期，微信轮询会停止，Web 服务和手动兑换功能不受影响。

## 环境变量

复制 `.env.example` 到 `.env` 后配置：

- `PORT`：服务端口，默认 `3458`
- `REDEEM_TOKEN`：兑换站点签名 TOKEN，可以为空，自动兑换前会尝试自动获取
- `ADMIN_USERNAME`：管理员登录账号
- `ADMIN_PASSWORD`：管理员登录密码
- `TEMP_USERNAME`：临时用户登录账号
- `TEMP_PASSWORD`：临时用户登录密码
- `SESSION_SECRET`：登录会话签名密钥
- `WECHAT_MP_FAKEID`：微信模式目标公众号 fakeid，默认 `MzE5MTIzOTAzNQ==`

## 常见日志说明

```text
auto redeem skipped for old code xxx, publishedAt=...
```

表示该兑换码已经入库，但发布时间超过 24 小时，所以不会自动兑换。

```text
WeChat login failed, web server will continue without WeChat polling
```

表示微信扫码登录失败或超时，服务会继续启动，但不会执行微信公众号抓取。

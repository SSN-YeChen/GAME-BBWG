# bb-web

仿造 `E:\B2` 功能的网页版实现，技术栈：

- Node.js 22+
- TypeScript
- sqlite3
- Express

## 功能

- 批量新增账号（按行导入，自动去重）
- 账号列表（搜索、单删、全删）
- 批量兑换（保存 TOKEN、执行兑换、重试失败账号、强制全部设为已兑换）
- 兑换进度实时推送（SSE）
- 打开站点需要账号密码登录，账号密码由后端 `.env` 校验
- 支持管理员和临时用户两种角色，临时用户仅可新增账号和查看数据

## 启动

```bash
npm install
npm run dev
```

默认地址：`http://localhost:3458`

## 环境变量

复制 `.env.example` 到 `.env` 后可选配置：

- `PORT`：服务端口，默认 `3458`
- `REDEEM_TOKEN`：兑换签名 TOKEN（也可在页面内保存）
- `ADMIN_USERNAME`：登录账号
- `ADMIN_PASSWORD`：登录密码
- `TEMP_USERNAME`：临时账号
- `TEMP_PASSWORD`：临时密码
- `SESSION_SECRET`：登录会话签名密钥

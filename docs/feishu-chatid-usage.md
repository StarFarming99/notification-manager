# 飞书 ChatID 配置使用指南

本文档介绍如何使用飞书应用通过 `chatid` 推送消息到指定的群聊。

## 前置准备

### 1. 创建飞书应用并获取凭证

1. 登录 [飞书开放平台](https://open.feishu.cn/)
2. 创建企业自建应用
3. 获取应用的 `AppID` 和 `AppSecret`
4. 为应用配置以下权限：
   - `读取和发送单聊、群聊消息`
   - `批量发送消息给多个用户`
   - `批量发送消息给一个或多个部门`

### 2. 获取群聊 ChatID

获取群聊 ChatID 的方法：

1. **通过 API 获取**：
   - 调用 [获取群信息](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/chat/get) API
   - 或使用飞书开发者工具获取

2. **从群聊链接获取**：
   - 群聊链接格式：`https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}`
   - ChatID 通常是 `oc` 开头的字符串，例如：`oc_a0553eda9014c201e6969b478895c230`

## 配置步骤

### 步骤 1: 创建 Secret 存储凭证

创建一个 Secret 来存储飞书应用的 `AppID` 和 `AppSecret`：

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feishu-config-secret
  namespace: kubesphere-monitoring-system
type: Opaque
stringData:
  appid: "cli_xxxxxxxxxxxxx"      # 替换为你的 AppID
  appsecret: "xxxxxxxxxxxxxxxxxx" # 替换为你的 AppSecret
```

应用 Secret：

```bash
kubectl apply -f feishu-config-secret.yaml
```

### 步骤 2: 创建 Config 资源

创建飞书 Config 资源，用于配置应用凭证：

```yaml
apiVersion: notification.kubesphere.io/v2beta2
kind: Config
metadata:
  name: feishu-config
  labels:
    type: default
    app: notification-manager
spec:
  feishu:
    appID:
      valueFrom:
        secretKeyRef:
          key: appid
          name: feishu-config-secret
          namespace: kubesphere-monitoring-system
    appSecret:
      valueFrom:
        secretKeyRef:
          key: appsecret
          name: feishu-config-secret
          namespace: kubesphere-monitoring-system
```

应用 Config：

```bash
kubectl apply -f feishu-config.yaml
```

### 步骤 3: 创建 Receiver 资源

创建 Receiver 资源，配置使用 `chatids` 发送消息：

#### 示例 1: 使用单个 ChatID

```yaml
apiVersion: notification.kubesphere.io/v2beta2
kind: Receiver
metadata:
  name: feishu-chat-receiver
  labels:
    type: global
    app: notification-manager
spec:
  feishu:
    enabled: true
    # 配置 ChatIDs，支持多个群聊
    chatids:
      - oc_a0553eda9014c201e6969b478895c230
    # 选择 Config，通过 label selector 匹配
    feishuConfigSelector:
      matchLabels:
        type: default
    # 消息模板类型：text, post 或 interactive
    tmplType: text
    # 可选：指定模板名称
    template: nm.default.text
    # 可选：自定义模板内容
    tmplText:
      name: notification-manager-template
      namespace: kubesphere-monitoring-system
```

#### 示例 2: 使用多个 ChatID

```yaml
apiVersion: notification.kubesphere.io/v2beta2
kind: Receiver
metadata:
  name: feishu-multi-chat-receiver
  labels:
    type: global
    app: notification-manager
spec:
  feishu:
    enabled: true
    # 配置多个 ChatIDs，消息会发送到所有指定的群聊
    chatids:
      - oc_a0553eda9014c201e6969b478895c230
      - oc_b0553eda9014c201e6969b478895c231
      - oc_c0553eda9014c201e6969b478895c232
    feishuConfigSelector:
      matchLabels:
        type: default
    tmplType: interactive  # 使用交互式卡片
    template: nm.default.interactive
```

#### 示例 3: 组合使用多种发送方式

```yaml
apiVersion: notification.kubesphere.io/v2beta2
kind: Receiver
metadata:
  name: feishu-combined-receiver
  labels:
    type: global
    app: notification-manager
spec:
  feishu:
    enabled: true
    # 可以同时配置多种发送方式
    chatids:
      - oc_a0553eda9014c201e6969b478895c230
    user:
      - user1
      - user2
    department:
      - dev
    chatbot:
      webhook:
        valueFrom:
          secretKeyRef:
            key: webhook
            name: feishu-webhook-secret
            namespace: kubesphere-monitoring-system
    feishuConfigSelector:
      matchLabels:
        type: default
    tmplType: text
```

应用 Receiver：

```bash
kubectl apply -f feishu-receiver.yaml
```

## 消息类型说明

支持三种消息类型：

### 1. Text（文本消息）

```yaml
tmplType: text
```

纯文本消息，适合简单的告警通知。

### 2. Post（富文本消息）

```yaml
tmplType: post
```

富文本消息，支持更丰富的格式展示。

### 3. Interactive（交互式卡片）

```yaml
tmplType: interactive
```

交互式卡片消息，支持按钮、交互等高级功能，默认类型。

## 验证配置

### 1. 检查 Config 状态

```bash
kubectl get config feishu-config -n kubesphere-monitoring-system
```

### 2. 检查 Receiver 状态

```bash
kubectl get receiver feishu-chat-receiver -n kubesphere-monitoring-system
kubectl describe receiver feishu-chat-receiver -n kubesphere-monitoring-system
```

### 3. 查看日志

查看 notification-manager 的日志，确认消息发送状态：

```bash
kubectl logs -n kubesphere-monitoring-system -l app=notification-manager --tail=100 | grep FeishuNotifier
```

## 注意事项

1. **权限要求**：
   - 确保飞书应用具有发送消息到群聊的权限
   - 应用需要被添加到目标群聊中

2. **ChatID 格式**：
   - ChatID 通常是 `oc_` 开头的字符串
   - 确保 ChatID 正确，否则消息发送会失败

3. **消息发送限制**：
   - 飞书 API 有频率限制（5次/秒）
   - 系统会自动重试超过限制的请求

4. **Config 选择**：
   - Receiver 通过 `feishuConfigSelector` 选择 Config
   - 确保 Config 的 labels 与 selector 匹配

5. **异步发送**：
   - 消息发送是异步的，可能会有延迟
   - 多个 ChatID 的消息会并发发送

## 故障排查

### 问题 1: 消息发送失败

**检查项**：
- 验证 AppID 和 AppSecret 是否正确
- 确认应用权限是否配置正确
- 检查 ChatID 是否正确
- 查看 notification-manager 日志中的错误信息

### 问题 2: Config 未找到

**检查项**：
- 确认 Config 资源已创建
- 检查 `feishuConfigSelector` 的 labels 是否匹配
- 确认 Config 和 Receiver 在同一 namespace 或使用正确的 namespace 引用

### 问题 3: 权限错误

**检查项**：
- 确认应用已添加到目标群聊
- 验证应用权限是否包含"发送消息到群聊"
- 检查 IP 白名单设置（如已配置）

## 完整示例

以下是一个完整的配置示例，包含所有必要的资源：

```yaml
# 1. Secret
apiVersion: v1
kind: Secret
metadata:
  name: feishu-config-secret
  namespace: kubesphere-monitoring-system
type: Opaque
stringData:
  appid: "cli_xxxxxxxxxxxxx"
  appsecret: "xxxxxxxxxxxxxxxxxx"
---
# 2. Config
apiVersion: notification.kubesphere.io/v2beta2
kind: Config
metadata:
  name: feishu-config
  namespace: kubesphere-monitoring-system
  labels:
    type: default
    app: notification-manager
spec:
  feishu:
    appID:
      valueFrom:
        secretKeyRef:
          key: appid
          name: feishu-config-secret
          namespace: kubesphere-monitoring-system
    appSecret:
      valueFrom:
        secretKeyRef:
          key: appsecret
          name: feishu-config-secret
          namespace: kubesphere-monitoring-system
---
# 3. Receiver
apiVersion: notification.kubesphere.io/v2beta2
kind: Receiver
metadata:
  name: feishu-chat-receiver
  namespace: kubesphere-monitoring-system
  labels:
    type: global
    app: notification-manager
spec:
  feishu:
    enabled: true
    chatids:
      - oc_a0553eda9014c201e6969b478895c230
    feishuConfigSelector:
      matchLabels:
        type: default
    tmplType: interactive
```

应用完整配置：

```bash
kubectl apply -f feishu-complete-config.yaml
```

## 参考链接

- [飞书开放平台文档](https://open.feishu.cn/document/)
- [发送消息到群聊 API](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create)
- [获取群信息 API](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/chat/get)



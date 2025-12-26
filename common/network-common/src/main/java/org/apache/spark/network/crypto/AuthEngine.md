# AuthEngine 类分析文档

## 类的概述和定义

`AuthEngine` 是 Spark 网络加密模块中的核心认证引擎类，实现了 `Closeable` 接口。该类负责实现基于 X25519 Diffie-Hellman 密钥交换的前向安全认证协议。

**核心功能定位**：
- 提供基于 X25519 椭圆曲线密码学的密钥交换机制
- 使用预共享密钥派生 AES-GCM 加密密钥
- 支持客户端挑战生成和服务器响应验证
- 管理会话密码的创建和生命周期

**类定义**：
```java
class AuthEngine implements Closeable
```

**包级可见性**：该类为包级可见，主要供 `AuthClientBootstrap` 和 `AuthServerBootstrap` 内部使用。

## 构造函数参数说明

### 构造函数签名
```java
AuthEngine(String appId, String preSharedSecret, TransportConf conf)
```

### 参数详细说明

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `appId` | `String` | 应用标识符，用于区分不同应用的认证会话 |
| `preSharedSecret` | `String` | 预共享密钥，作为密钥派生的基础材料 |
| `conf` | `TransportConf` | 传输配置对象，包含加密算法和协议版本配置 |

**参数验证**：构造函数使用 `Preconditions.checkNotNull()` 确保 `appId` 和 `preSharedSecret` 不为空。

## 核心属性分析

### 静态常量

| 常量名 | 类型 | 值 | 说明 |
|--------|------|----|------|
| `DERIVED_KEY_INFO` | `byte[]` | `"derivedKey"` | HKDF 密钥派生信息字段 |
| `INPUT_IV_INFO` | `byte[]` | `"inputIv"` | 输入IV的HKDF信息字段 |
| `OUTPUT_IV_INFO` | `byte[]` | `"outputIv"` | 输出IV的HKDF信息字段 |
| `MAC_ALGORITHM` | `String` | `"HMACSHA256"` | HMAC算法名称 |
| `LEGACY_CIPHER_ALGORITHM` | `String` | `"AES/CTR/NoPadding"` | 传统加密算法 |
| `CIPHER_ALGORITHM` | `String` | `"AES/GCM/NoPadding"` | 现代加密算法 |
| `AES_GCM_KEY_SIZE_BYTES` | `int` | `16` | AES-GCM密钥大小（128位） |
| `EMPTY_TRANSCRIPT` | `byte[]` | 空数组 | 空协议转录 |
| `UNSAFE_SKIP_HKDF_VERSION` | `int` | `1` | 跳过最终HKDF的协议版本 |

### 实例属性

| 属性名 | 类型 | 访问修饰符 | 说明 |
|--------|------|------------|------|
| `appId` | `String` | `private final` | 应用标识符 |
| `preSharedSecret` | `byte[]` | `private final` | 预共享密钥字节数组 |
| `conf` | `TransportConf` | `private final` | 传输配置 |
| `cryptoConf` | `Properties` | `private final` | 加密配置属性 |
| `unsafeSkipFinalHkdf` | `boolean` | `private final` | 是否跳过最终HKDF（向后兼容） |
| `clientPrivateKey` | `byte[]` | `private` | 客户端私钥 |
| `sessionCipher` | `TransportCipher` | `private` | 会话密码实例 |

## 主要方法分类和说明

### 1. 核心认证流程方法

#### `challenge()`
**功能**：生成客户端挑战消息

**执行流程**：
1. 生成客户端X25519私钥：`X25519.generatePrivateKey()`
2. 计算对应的公钥：`X25519.publicFromPrivate()`
3. 加密公钥：调用 `encryptEphemeralPublicKey()`
4. 返回包含加密公钥的 `AuthMessage`

**技术特点**：
- 使用临时密钥对确保前向安全性
- 支持空协议转录的初始挑战

#### `response(AuthMessage encryptedClientPublicKey)`
**功能**：处理客户端挑战并生成服务器响应

**执行流程**：
1. 验证应用ID匹配
2. 解密客户端公钥：`decryptEphemeralPublicKey()`
3. 生成服务器临时密钥对
4. 加密服务器公钥并返回
5. 计算共享密钥并生成会话密码

**安全机制**：
- 应用ID验证防止重放攻击
- 双向密钥交换确保相互认证

#### `deriveSessionCipher(AuthMessage encryptedClientPublicKey, AuthMessage encryptedServerPublicKey)`
**功能**：客户端验证服务器响应并派生会话密码

**执行流程**：
1. 验证双方应用ID一致性
2. 解密服务器公钥
3. 使用客户端私钥和服务器公钥计算共享密钥
4. 生成最终的会话密码

### 2. 加密解密核心方法

#### `encryptEphemeralPublicKey(byte[] ephemeralX25519PublicKey, byte[] transcript)`
**功能**：使用派生密钥加密X25519公钥

**加密流程**：
1. 生成随机盐值：`Random.randBytes()`
2. 构建认证数据（AAD）：应用ID + 盐值 + 协议转录
3. HKDF派生加密密钥：`Hkdf.computeHkdf()`
4. AES-GCM加密公钥：`AesGcmJce.encrypt()`

**安全特性**：
- 盐值随机化确保每次加密不同
- AAD机制提供关联数据认证

#### `decryptEphemeralPublicKey(AuthMessage encryptedPublicKey, byte[] transcript)`
**功能**：解密加密的X25519公钥

**解密流程**：
1. 验证应用ID匹配
2. 重构AAD数据
3. HKDF派生解密密钥
4. AES-GCM解密获取原始公钥

**错误处理**：AAD不匹配或数据篡改会抛出 `GeneralSecurityException`

### 3. 密钥派生和密码生成

#### `generateTransportCipher(byte[] sharedSecret, boolean isClient, byte[] transcript)`
**功能**：根据共享密钥生成传输密码

**派生流程**：
1. 派生会话密钥：HKDF(共享密钥, 转录, DERIVED_KEY_INFO)
2. 派生客户端IV：HKDF(共享密钥, 转录, INPUT_IV_INFO)
3. 派生服务器IV：HKDF(共享密钥, 转录, OUTPUT_IV_INFO)
4. 根据配置选择加密算法实现

**算法支持**：
- `CtrTransportCipher`：AES/CTR模式（传统支持）
- `GcmTransportCipher`：AES/GCM模式（现代标准）

### 4. 辅助方法

#### `getTranscript(AuthMessage... encryptedPublicKeys)`
**功能**：构建协议转录数据

**实现方式**：将所有加密公钥序列化到ByteBuf中，确保协议消息的顺序完整性。

#### `sessionCipher()`
**功能**：获取已生成的会话密码

**状态检查**：使用 `Preconditions.checkState()` 确保密码已初始化。

## 设计特点总结

### 1. 前向安全设计
- **临时密钥**：每次会话使用新的X25519密钥对
- **密钥分离**：认证密钥和会话密钥完全分离
- **协议版本**：支持不同安全级别的协议版本

### 2. 加密算法灵活性
- **多算法支持**：同时支持AES/GCM和AES/CTR
- **配置驱动**：通过 `TransportConf` 动态选择算法
- **向后兼容**：支持传统加密模式

### 3. 协议转录机制
- **消息完整性**：转录包含所有协议消息的序列化
- **密钥绑定**：派生密钥与具体协议流程绑定
- **防重放**：确保协议消息的顺序和完整性

### 4. 安全最佳实践
- **盐值随机化**：每次加密使用不同的随机盐
- **AAD认证**：关联数据认证防止数据篡改
- **HKDF标准化**：使用标准密钥派生函数

## 配置参数说明

### 关键配置参数

| 配置项 | 说明 | 默认值 | 影响范围 |
|--------|------|--------|----------|
| `spark.network.crypto.cipher.transformation` | 加密算法变换 | AES/GCM/NoPadding | 决定使用GCM还是CTR模式 |
| `spark.network.auth.engine.version` | 认证引擎版本 | 2 | 控制是否跳过最终HKDF |
| `spark.network.crypto.config.*` | 加密配置属性 | 空 | 提供算法特定参数 |

### 协议版本兼容性
- **版本1**：跳过最终HKDF派生（不安全，向后兼容）
- **版本2+**：执行完整的HKDF密钥派生（安全标准）

## 密码学技术详解

### X25519密钥交换
- **椭圆曲线**：Curve25519，提供128位安全强度
- **临时密钥**：每次会话生成新的密钥对
- **共享秘密**：`X25519.computeSharedSecret()` 计算共享密钥

### HKDF密钥派生
- **标准化**：RFC 5869标准密钥派生函数
- **多用途**：派生会话密钥、IV值等
- **信息分离**：使用不同的info字段区分派生用途

### AES-GCM加密
- **认证加密**：同时提供机密性和完整性
- **关联数据**：AAD机制保护协议上下文
- **随机IV**：每次加密使用不同的初始化向量

## 使用场景和最佳实践

### 适用场景
1. **安全通信**：需要前向安全性的网络通信
2. **双向认证**：客户端和服务器相互认证的场景
3. **密钥协商**：动态协商会话密钥的加密通道

### 安全建议
1. **协议版本**：生产环境使用版本2或更高
2. **密钥管理**：确保预共享密钥的安全存储
3. **算法选择**：优先使用AES-GCM模式
4. **随机数质量**：使用安全的随机数生成器

### 性能考虑
1. **密钥生成**：X25519密钥生成相对高效
2. **加密开销**：AES-GCM提供良好的性能安全平衡
3. **内存使用**：合理控制协议转录的大小
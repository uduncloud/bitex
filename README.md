# bitex
交易所系统

## 技术说明
    本系统使用JAVA进行开发，基于spring-cloud进行系统搭建，使用maven进行构建工程。

## 环境准备
1. JDK1.8
2. mysql数据库,用于存储业务信息
3. redis数据库,用于处理用户登录会话信息
4. mongodb数据库,用于存储币币K线数据
5. 安装kafka
6. 优盾钱包接入(https://www.uduncloud.com)

## 额外功能准备
1. OSS配置
2. 短信服务配置
3. 邮件配置
4. 极验配置
5. 谷歌验证配置

## 打包环境
打包命令,生成可执行jar包
```
mvn clean install
```
## 工程包说明
1. core: 工程核心依赖包
2. cloud: 微服务注册中心包
3. admin: 管理后台服务
4. exchange-core: 币币交易核心依赖包
5. exchange：币币交易业务包
6. exchange-api: 币币交易API服务包
7. market: 行情服务包
8. ucenter-api: 用户中心包
9. wallet-bipay: 钱包管理服务包,接入优盾钱包

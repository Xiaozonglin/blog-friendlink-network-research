# 数据库配置示例
# 复制此文件为 config.py 并填入你的数据库信息

DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',  # 修改为你的MySQL用户名
    'password': 'your_password',  # 修改为你的MySQL密码
    'charset': 'utf8mb4'
}

# 腾讯云配置示例（用于AI判断博客网站）
# 获取SecretId和SecretKey: https://console.cloud.tencent.com/cam/capi
TENCENT_CONFIG = {
    'secret_id': 'your_secret_id',  # 修改为你的腾讯云SecretId
    'secret_key': 'your_secret_key',  # 修改为你的腾讯云SecretKey
    'region': 'ap-beijing',  # 区域，可选: ap-beijing, ap-shanghai, ap-guangzhou等
    'model': 'hunyuan-a13b'  # 模型名称，可选: hunyuan-a13b, hunyuan-turbo等
}


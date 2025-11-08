# 博客网站爬虫

从多个数据源爬取博客网站信息并存储到MySQL数据库。

## 功能特性

- 从 `zhblogs.net` API 爬取博客数据
- 从 `alexsci.com` 爬取博客数据
- 从 `bf.zzxworld.com` API 爬取博客数据
- 从 `foreverblog.cn` (十年之约) 爬取博客数据
- 自动创建数据库和表
- 数据去重处理
- URL规范化
- 支持配置文件和环境变量
- 从外部网站识别博客网站（使用AI判断）
- 自动将识别出的博客添加到sites表并创建友情链接关系

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置数据库

### 方法1: 使用配置文件（推荐）

1. 复制配置文件示例：
```bash
cp config.example.py config.py
```

2. 编辑 `config.py`，填入你的数据库信息和腾讯云配置：
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',  # 你的MySQL用户名
    'password': 'your_password',  # 你的MySQL密码
    'charset': 'utf8mb4'
}

# 腾讯云配置（用于AI判断博客网站，可选）
TENCENT_CONFIG = {
    'secret_id': 'your_secret_id',  # 你的腾讯云SecretId
    'secret_key': 'your_secret_key',  # 你的腾讯云SecretKey
    'region': 'ap-beijing',  # 区域
    'model': 'hunyuan-a13b'  # 模型名称
}
```

**获取腾讯云密钥**：
1. 登录 [腾讯云控制台](https://console.cloud.tencent.com/)
2. 进入 [API密钥管理](https://console.cloud.tencent.com/cam/capi)
3. 创建密钥，获取 SecretId 和 SecretKey
4. 开通 [腾讯混元大模型服务](https://console.cloud.tencent.com/hunyuan)

### 方法2: 使用环境变量

```bash
# Windows PowerShell
$env:DB_USER="root"
$env:DB_PASSWORD="your_password"

# Linux/Mac
export DB_USER="root"
export DB_PASSWORD="your_password"
```

## 使用方法

### 1. 爬取博客网站列表

1. 确保MySQL服务正在运行

2. 运行爬虫脚本：
```bash
python crawl_blogs.py
```

3. 程序会自动：
   - 创建数据库 `blog_link`（如果不存在）
   - 创建表 `sites`（如果不存在）
   - 从多个数据源爬取博客信息
   - 去重并保存到数据库

### 2. 爬取友链关系

1. 运行友链爬虫脚本：
```bash
python crawl_friend_links.py
```

2. 程序会自动：
   - 从 `sites` 表中读取博客
   - 爬取每个博客的主页和友情链接页面
   - 提取友链关系并保存到 `friend_links` 表
   - 将外部链接保存到 `external_sites` 表

### 3. 识别外部网站中的博客（使用AI）

1. 确保已配置腾讯云密钥（见配置部分）

2. 运行外部网站识别脚本：
```bash
python crawl_external_sites.py
```

3. 程序会自动：
   - 从 `external_sites` 表获取未处理的外链
   - 爬取每个外链的首页和关键信息
   - 使用腾讯混元AI判断是否为博客网站
   - 如果是博客，添加到 `sites` 表
   - 创建友情链接关系并保存到 `friend_links` 表
   - 标记 `external_sites` 表中的记录为已处理

**注意**：
- 该脚本可以与 `crawl_friend_links.py` 同时运行，使用数据库锁避免冲突
- 建议并发线程数设置为1-5，避免API限流
- 程序会自动处理并发冲突和重复数据

### 4. 深度筛查友链数量为0的博客（使用Selenium）

1. 确保已安装Selenium和ChromeDriver：
```bash
pip install selenium
# 下载ChromeDriver: https://chromedriver.chromium.org/
```

2. 运行深度筛查脚本：
```bash
python rescan_zero_friend_links.py
```

3. 程序会自动：
   - 查找友链数量为0的博客站点
   - 使用Selenium模拟浏览器访问（处理JavaScript渲染）
   - 重新扫描首页和友情链接页面
   - 提取并保存新发现的友链关系

**注意**：
- 需要使用Chrome浏览器和ChromeDriver
- 可以处理客户端渲染的网站（React、Vue等）
- 处理速度较慢，建议批量处理

### 5. 人工随机抽检博客网站

1. 运行人工抽检脚本：
```bash
python manual_review_blogs.py
```

2. 程序会：
   - 随机选择未审核的博客网站
   - 显示网站URL和已抽检数量
   - 可选在浏览器中打开链接
   - 人工输入判断结果：
     - 输入 `0`: 是博客，标记为已审核，继续下一个
     - 输入 `1`: 不是博客，删除该网站及其相关的友链、外链数据
     - 输入 `q`: 退出程序

**注意**：
- 删除操作需要输入 `yes` 确认
- 删除站点会自动删除相关的友链和外链数据（外键约束）
- 可以随时退出，已审核的记录会保存

### 6. 图分析：最短路径分布与柱状图

使用脚本 `analyze_shortest_paths.py` 对博客友链构成的有向图计算“从一个博客到另一个博客”的最短路径长度分布，并输出柱状图（含不可达对数）。

支持三种数据来源：

1. 数据库 (db)
2. CSV 文件 (csv)
3. 随机示例图 (sample) —— 便于演示或无数据时调试

#### 依赖安装（若尚未安装新增库）
requirements.txt 已添加 `networkx`, `matplotlib`, `pandas`，确保重新安装：
```powershell
pip install -r requirements.txt
```

#### 运行方式

1) 使用数据库（默认数据库名 `blog_link`，可通过 `--database` 指定；需确保已有 `sites` 与 `friend_links` 数据）
```powershell
python analyze_shortest_paths.py --source db --config config.py --database blog_link --out shortest_path_histogram.png --sample 500
```
说明：
- `--sample 500` 表示只抽取 500 个源节点来加速（可去掉以全量计算，节点多时较慢）。
- 若 `config.py` 的 `DB_CONFIG` 中包含 `database` 字段，可省略 `--database`。

2) 使用 CSV：
```powershell
python analyze_shortest_paths.py --source csv --csv-file links.csv --out shortest_path_histogram.png --sample 300
```
CSV 列命名支持（按优先级匹配）：
- `from,to`
- `from_url,to_url`
- 其它：自动取前两列

3) 使用随机示例图（快速演示）：
```powershell
python analyze_shortest_paths.py --source sample --n 400 --p 0.015 --seed 42 --out shortest_path_histogram.png
```
参数解释：
- `--n` 节点数
- `--p` 随机边生成概率（Erdős–Rényi G(n,p) 有向图）
- `--seed` 固定随机种子确保可复现

#### 输出说明
- 终端打印：每个最短路径长度对应的有序节点对数量，以及不可达对（unreachable）数量与比例。
- 图片：`shortest_path_histogram.png`（可通过 `--out` 自定义文件名）。
- X 轴：有向最短路径长度（整数）；若存在不可达对，增加 `unreachable` 一栏。
- Y 轴：有序节点对数量（源 ≠ 目标）。

#### 性能/加速建议
- 图很大时使用 `--sample N` 抽样源节点（复杂度近似 O(N * (E/N + log N)) 对每个源进行单源最短路径）。
- 可多次运行不同 `--seed` 验证稳定性。
- 若仅需强连通主成分，可先筛选：未来可扩展 `--largest-scc`（当前未实现，可自行对 networkx 结果再处理）。

#### 典型解读
- 最短路径集中在 2~4：说明网络具有“小世界”特征。
- 大量 `unreachable`：说明有很多弱连通分量或边方向强约束。
- 长尾（长度很大）极少：通常是少量“链式”传播路径，可能提示稀疏桥接节点。

#### 导出 CSV（可选）
如果你想从数据库导出一份最小 CSV 手动分析：
```sql
SELECT s1.url AS from_url, s2.url AS to_url
INTO OUTFILE '/tmp/friend_links.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM friend_links f
JOIN sites s1 ON f.from_site_id = s1.id
JOIN sites s2 ON f.to_site_id = s2.id;
```
Windows 下可改为使用客户端工具导出，或在 Python 脚本中用 `pandas` 写出。

#### 常见问题 (FAQ)
- 报错 `No database selected`：在 `config.py` 的 `DB_CONFIG` 中加入 `'database': 'blog_link'` 或使用 `--database blog_link`。
- 报错 `1049 Unknown database`：尚未初始化，先运行 `crawl_blogs.py` 创建数据库与表。
- 报错 `1146 Table doesn't exist`：尚未爬取友链，运行 `crawl_friend_links.py`。
- 柱状图只有距离=1：可能数据尚少或只包含少量直接边，没有更长路径形成。

#### 后续可扩展（尚未内置）
- 计算强连通分量大小分布
- 输出平均距离、直径（对最大强连通子图）
- 生成累计分布 (CDF) 图
- 发现“桥接”节点（Betweenness 或 PageRank）

如需这些功能，可提出需求进一步完善。

## 数据库结构

- **数据库名**: `blog_link`
- **表名**: `sites`
  - `id`: INT (主键，自增)
  - `name`: VARCHAR(255) (博客名称)
  - `url`: VARCHAR(500) (博客首页地址，唯一)
  - `reviewed`: TINYINT(1) (是否已审核: 0-未审核, 1-已审核)
- **表名**: `friend_links`
  - `id`: INT (主键，自增)
  - `from_site_id`: INT (来源博客ID)
  - `to_site_id`: INT (目标博客ID)
  - `link_type`: VARCHAR(20) (链接类型: homepage/friend_page)
  - `page_url`: VARCHAR(500) (发现链接的页面URL)
  - `created_at`: DATETIME (创建时间)
- **表名**: `external_sites`
  - `id`: INT (主键，自增)
  - `url`: VARCHAR(500) (外部网站URL)
  - `domain`: VARCHAR(255) (域名，唯一)
  - `discovered_from_site_id`: INT (从哪个博客发现的)
  - `discovered_from_page`: VARCHAR(500) (从哪个页面发现的)
  - `link_type`: VARCHAR(20) (链接类型: homepage/friend_page)
  - `processed`: TINYINT (处理状态: 0-未处理, 1-已处理, 2-处理失败, 3-处理中)
  - `created_at`: DATETIME (创建时间)

## 数据源

1. **zhblogs.net**: https://www.zhblogs.net/api/blog/list?page=1&pageSize=7000
   - 返回JSON格式数据
   - 包含博客名称和URL

2. **alexsci.com**: https://alexsci.com/rss-blogroll-network/discover/
   - HTML页面
   - 从表格中提取博客信息

3. **bf.zzxworld.com**: https://bf.zzxworld.com/api/sites
   - API端点，返回JSON格式数据
   - 包含博客标题和URL

4. **foreverblog.cn (十年之约)**: https://www.foreverblog.cn/blogs.html
   - HTML页面
   - 从成员列表页面提取博客信息

5. **GitHub chinese-independent-blogs**: https://github.com/timqian/chinese-independent-blogs
   - CSV文件格式
   - 从GitHub仓库的 `blogs-original.csv` 文件获取博客信息
   - 包含博客名称和URL

## 注意事项

- 确保MySQL服务已启动
- 确保有网络连接
- 首次运行可能需要较长时间（取决于数据量）
- 程序会自动处理重复数据（基于URL）

## 故障排除

### 数据库连接失败
- 检查MySQL服务是否运行
- 检查用户名和密码是否正确
- 检查端口是否正确（默认3306）

### 爬取失败
- 检查网络连接
- 检查目标网站是否可访问
- 查看错误信息了解详细原因

## 许可证

MIT License


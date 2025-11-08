#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
博客友链爬虫脚本
从sites表中读取博客，爬取每个博客的主页和友情链接页面，提取外链关系
"""

import requests
import pymysql
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs
import re
import os
import urllib3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 尝试从配置文件导入数据库配置
try:
    from config import DB_CONFIG as CONFIG_DB_CONFIG
    DB_CONFIG = CONFIG_DB_CONFIG.copy()
except ImportError:
    # 如果没有配置文件，使用环境变量或默认值
    DB_CONFIG = {}

# 如果配置文件没有提供完整配置，使用环境变量或默认值补充
DB_CONFIG.setdefault('host', os.getenv('DB_HOST', 'localhost'))
DB_CONFIG.setdefault('port', int(os.getenv('DB_PORT', 3306)))
DB_CONFIG.setdefault('user', os.getenv('DB_USER', 'root'))
DB_CONFIG.setdefault('password', os.getenv('DB_PASSWORD', 'root'))
DB_CONFIG.setdefault('charset', 'utf8mb4')

DB_NAME = 'blog_link'
SITES_TABLE = 'sites'
FRIEND_LINKS_TABLE = 'friend_links'
EXTERNAL_SITES_TABLE = 'external_sites'

# 常见的友情链接页面URI
FRIEND_LINK_URIS = ['friend', 'friend.html', 'friends', 'friends.html', 'link', 'link.html', 'links', 'links.html',
    'friendship', 'friendship.html', '友情链接', 'about/friends', 'page/friends', 'page/friends.html', 'friendlink',
    'friend-link',
]

# 请求配置
REQUEST_TIMEOUT = 15
REQUEST_DELAY = 1  # 请求间隔（秒）
MAX_WORKERS = 10  # 最大并发线程数
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# 线程锁（用于保护打印输出和统计信息）
print_lock = threading.Lock()
stats_lock = threading.Lock()
url_map_lock = threading.Lock()  # URL映射锁，用于动态更新

def init_database():
    """初始化数据库和表"""
    try:
        connection = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        
        with connection.cursor() as cursor:
            # 使用数据库
            cursor.execute(f"USE {DB_NAME}")
            
            # 创建友链关系表
            # 注意：使用前缀索引避免键长度超过1000字节限制
            # utf8mb4字符集下，每个字符最多4字节
            # INT字段占4字节，VARCHAR(100)前缀索引占400字节
            # 唯一索引: from_site_id(4) + to_site_id(4) + page_url(100*4) = 408字节 < 1000字节
            create_friend_links_table = f"""
            CREATE TABLE IF NOT EXISTS {FRIEND_LINKS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                from_site_id INT NOT NULL,
                to_site_id INT NOT NULL,
                link_type VARCHAR(20) NOT NULL COMMENT 'homepage: 主页, friend_page: 友链页面',
                page_url VARCHAR(500) NOT NULL COMMENT '发现链接的页面URL',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_from_site (from_site_id),
                INDEX idx_to_site (to_site_id),
                INDEX idx_link_type (link_type),
                UNIQUE KEY unique_friend_link (from_site_id, to_site_id, page_url(100)),
                FOREIGN KEY (from_site_id) REFERENCES {SITES_TABLE}(id) ON DELETE CASCADE,
                FOREIGN KEY (to_site_id) REFERENCES {SITES_TABLE}(id) ON DELETE CASCADE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """
            cursor.execute(create_friend_links_table)
            
            # 检查并修复友链关系表的索引（如果表已存在但索引不正确）
            try:
                cursor.execute(f"SHOW INDEX FROM {FRIEND_LINKS_TABLE} WHERE Key_name = 'unique_friend_link'")
                existing_index = cursor.fetchone()
                if existing_index:
                    # 检查索引定义，如果前缀长度不对，删除并重新创建
                    cursor.execute(f"SHOW CREATE TABLE {FRIEND_LINKS_TABLE}")
                    create_table_sql = cursor.fetchone()['Create Table']
                    if 'page_url(100)' not in create_table_sql and 'page_url(191)' in create_table_sql:
                        print(f"  检测到友链关系表索引需要修复...")
                        cursor.execute(f"ALTER TABLE {FRIEND_LINKS_TABLE} DROP INDEX unique_friend_link")
                        cursor.execute(f"ALTER TABLE {FRIEND_LINKS_TABLE} ADD UNIQUE KEY unique_friend_link (from_site_id, to_site_id, page_url(100))")
                        print(f"  ✓ 已修复友链关系表索引")
            except Exception as e:
                # 如果表不存在或没有索引，忽略错误
                pass
            
            # 创建外部网站表
            # 唯一索引改为按域名（domain字段），避免同一个域名的外部网站重复存储
            # domain字段使用VARCHAR(255)，唯一索引使用前缀索引domain(100): 100*4 = 400字节 < 1000字节
            # 先检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{EXTERNAL_SITES_TABLE}'")
            table_exists = cursor.fetchone() is not None
            
            if table_exists:
                # 表已存在，检查并修复表结构
                print(f"  检测到外部网站表已存在，检查表结构...")
                try:
                    # 检查是否有domain字段
                    cursor.execute(f"SHOW COLUMNS FROM {EXTERNAL_SITES_TABLE} LIKE 'domain'")
                    has_domain = cursor.fetchone() is not None
                    
                    if not has_domain:
                        # 添加domain字段
                        print(f"  添加domain字段...")
                        cursor.execute(f"ALTER TABLE {EXTERNAL_SITES_TABLE} ADD COLUMN domain VARCHAR(255) NOT NULL DEFAULT '' AFTER url")
                        # 更新已有数据的domain字段
                        print(f"  更新已有数据的domain字段...")
                        cursor.execute(f"""
                            UPDATE {EXTERNAL_SITES_TABLE} 
                            SET domain = LOWER(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '://', -1), '/', 1), 'www.', ''))
                            WHERE domain = '' OR domain IS NULL
                        """)
                        print(f"  ✓ 已添加domain字段并更新数据")
                    
                    # 检查并删除旧的唯一索引
                    cursor.execute(f"SHOW INDEX FROM {EXTERNAL_SITES_TABLE} WHERE Key_name = 'unique_external_site'")
                    if cursor.fetchone():
                        cursor.execute(f"ALTER TABLE {EXTERNAL_SITES_TABLE} DROP INDEX unique_external_site")
                        print(f"  已删除旧的复合唯一索引")
                    
                    cursor.execute(f"SHOW INDEX FROM {EXTERNAL_SITES_TABLE} WHERE Key_name = 'unique_url'")
                    if cursor.fetchone():
                        cursor.execute(f"ALTER TABLE {EXTERNAL_SITES_TABLE} DROP INDEX unique_url")
                        print(f"  已删除旧的url唯一索引")
                    
                    # 清理重复数据（按域名）
                    print(f"  清理重复的外部网站记录（按域名）...")
                    try:
                        # 查找重复的域名，保留ID最小的记录
                        cursor.execute(f"""
                            DELETE t1 FROM {EXTERNAL_SITES_TABLE} t1
                            INNER JOIN {EXTERNAL_SITES_TABLE} t2 
                            WHERE t1.id > t2.id 
                            AND t1.domain = t2.domain
                            AND t1.domain != ''
                        """)
                        deleted_count = cursor.rowcount
                        if deleted_count > 0:
                            print(f"  已删除 {deleted_count} 条重复记录（按域名）")
                        else:
                            print(f"  未发现重复记录")
                    except Exception as e:
                        print(f"  警告: 清理重复数据时出错: {e}")
                    
                    # 检查是否已有domain的唯一索引
                    cursor.execute(f"SHOW INDEX FROM {EXTERNAL_SITES_TABLE} WHERE Key_name = 'unique_domain'")
                    domain_unique_index = cursor.fetchone()
                    if not domain_unique_index:
                        # 创建新的唯一索引（按域名）
                        cursor.execute(f"ALTER TABLE {EXTERNAL_SITES_TABLE} ADD UNIQUE KEY unique_domain (domain(100))")
                        print(f"  ✓ 已创建domain唯一索引")
                    else:
                        print(f"  ✓ domain唯一索引已存在")
                except Exception as e:
                    # 如果索引创建失败，可能是表结构问题
                    if '1071' in str(e) or 'key was too long' in str(e).lower():
                        print(f"  索引修复失败，尝试重新创建表...")
                        print(f"  警告: 这将删除现有的外部网站数据！")
                        # 这里不自动删除表，而是提示用户手动处理
                        raise Exception(f"外部网站表的索引长度问题无法自动修复。请手动执行:\n"
                                      f"  DROP TABLE IF EXISTS {EXTERNAL_SITES_TABLE};\n"
                                      f"然后重新运行此脚本。")
                    else:
                        # 其他错误，可能是索引已存在或其他问题，继续
                        pass
            
            # 如果表不存在，创建新表
            if not table_exists:
                create_external_sites_table = f"""
                CREATE TABLE {EXTERNAL_SITES_TABLE} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    url VARCHAR(500) NOT NULL,
                    domain VARCHAR(255) NOT NULL COMMENT '域名（去除www前缀）',
                    discovered_from_site_id INT NOT NULL COMMENT '从哪个博客发现的',
                    discovered_from_page VARCHAR(500) NOT NULL COMMENT '从哪个页面发现的',
                    link_type VARCHAR(20) NOT NULL COMMENT 'homepage: 主页, friend_page: 友链页面',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_url (url(100)),
                    INDEX idx_domain (domain(100)),
                    INDEX idx_from_site (discovered_from_site_id),
                    INDEX idx_link_type (link_type),
                    UNIQUE KEY unique_domain (domain(100)),
                    FOREIGN KEY (discovered_from_site_id) REFERENCES {SITES_TABLE}(id) ON DELETE CASCADE
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
                cursor.execute(create_external_sites_table)
                print(f"  ✓ 已创建外部网站表")
            
        connection.commit()
        print(f"✓ 数据库表初始化成功")
        connection.close()
        return True
    except Exception as e:
        print(f"✗ 数据库初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def normalize_url(url):
    """规范化URL"""
    if not url:
        return None
    
    # 去除前后空格
    url = url.strip()
    
    # 跳过非HTTP(S)链接
    if url.startswith(('mailto:', 'tel:', 'javascript:', '#')):
        return None
    
    # 如果没有协议，返回None（需要绝对URL）
    if not url.startswith(('http://', 'https://')):
        return None
    
    # 移除末尾的斜杠（保留协议后的）
    url = url.rstrip('/')
    if url.endswith('://'):
        return None
    
    # 移除fragment（#后面的部分）
    parsed = urlparse(url)
    url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ''))
    
    return url

def get_base_url(url):
    """获取URL的基础URL（协议+域名）"""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except:
        return None

def extract_domain(url):
    """提取URL的域名（去除www前缀）"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # 去除www前缀
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

def is_same_domain(url1, url2):
    """判断两个URL是否属于同一个域名"""
    try:
        domain1 = urlparse(url1).netloc.lower()
        domain2 = urlparse(url2).netloc.lower()
        # 去除www前缀
        domain1 = domain1.replace('www.', '')
        domain2 = domain2.replace('www.', '')
        return domain1 == domain2
    except:
        return False

def extract_redirect_url_from_html(html, base_url):
    """从HTML中提取跳转URL（处理安全跳转页面、外链转内链等）"""
    if not html:
        return None
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 方法1: 查找meta refresh标签
        meta_refresh = soup.find('meta', attrs={'http-equiv': re.compile('refresh', re.I)})
        if meta_refresh:
            content = meta_refresh.get('content', '')
            # 格式: "0;url=http://example.com" 或 "5; URL=http://example.com"
            match = re.search(r'url\s*=\s*([^\s;]+)', content, re.I)
            if match:
                redirect_url = match.group(1).strip('\'"')
                if redirect_url:
                    return urljoin(base_url, redirect_url)
        
        # 方法2: 查找JavaScript跳转
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string or ''
            # 匹配常见的跳转模式
            patterns = [
                r'window\.location\s*=\s*["\']([^"\']+)["\']',
                r'window\.location\.href\s*=\s*["\']([^"\']+)["\']',
                r'location\.href\s*=\s*["\']([^"\']+)["\']',
                r'location\.replace\s*\(\s*["\']([^"\']+)["\']',
                r'window\.open\s*\(\s*["\']([^"\']+)["\']',
                r'url\s*=\s*["\']([^"\']+)["\']',  # 通用URL变量
            ]
            for pattern in patterns:
                matches = re.findall(pattern, script_text, re.I)
                for match in matches:
                    if match and match.startswith(('http://', 'https://')):
                        return match
        
        # 方法3: 查找iframe中的src（某些跳转页面使用iframe）
        iframe = soup.find('iframe', src=True)
        if iframe:
            iframe_src = iframe.get('src', '')
            if iframe_src.startswith(('http://', 'https://')):
                return urljoin(base_url, iframe_src)
        
        # 方法4: 查找data-url或data-href属性（某些网站使用）
        for tag in soup.find_all(attrs={'data-url': True}):
            data_url = tag.get('data-url', '')
            if data_url.startswith(('http://', 'https://')):
                return urljoin(base_url, data_url)
        
        for tag in soup.find_all(attrs={'data-href': True}):
            data_href = tag.get('data-href', '')
            if data_href.startswith(('http://', 'https://')):
                return urljoin(base_url, data_href)
        
        # 方法5: 查找包含"跳转"、"redirect"等关键词的链接
        redirect_keywords = ['跳转', 'redirect', 'go', 'visit', '访问', '继续']
        for tag in soup.find_all('a', href=True):
            text = tag.get_text(strip=True).lower()
            href = tag.get('href', '')
            if any(keyword in text for keyword in redirect_keywords):
                if href.startswith(('http://', 'https://')):
                    return urljoin(base_url, href)
        
    except Exception as e:
        pass
    
    return None

def fetch_page(url, max_retries=3, follow_redirects=True):
    """获取网页内容，处理安全跳转页面和外链转内链"""
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, 
                headers=REQUEST_HEADERS, 
                timeout=REQUEST_TIMEOUT,
                verify=False,
                allow_redirects=follow_redirects
            )
            
            if response.status_code == 200:
                # 检查内容类型
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type:
                    html = response.text
                    final_url = response.url
                    
                    # 检查是否是安全跳转页面或外链转内链页面
                    # 如果页面内容很短且包含跳转逻辑，尝试提取真实URL
                    if len(html) < 5000:  # 短页面可能是跳转页面
                        redirect_url = extract_redirect_url_from_html(html, final_url)
                        if redirect_url and redirect_url != final_url:
                            # 递归获取真实页面
                            if attempt < max_retries - 1:
                                return fetch_page(redirect_url, max_retries - attempt - 1, follow_redirects=True)
                    
                    return html, final_url
                else:
                    return None, None
            elif response.status_code in [301, 302, 303, 307, 308]:
                # 处理HTTP重定向
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    redirect_url = urljoin(url, redirect_url)
                    if attempt < max_retries - 1:
                        return fetch_page(redirect_url, max_retries - attempt - 1, follow_redirects=True)
            return None, None
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None, None
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None, None
        except Exception as e:
            return None, None
    
    return None, None

def extract_links(html, base_url):
    """从HTML中提取外部链接，处理外链转内链的情况"""
    if not html:
        return set()
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        base_domain = urlparse(base_url).netloc.lower().replace('www.', '')
        
        # 查找所有链接
        for tag in soup.find_all('a', href=True):
            href = tag.get('href', '').strip()
            if not href:
                continue
            
            # 规范化URL
            # 如果是相对路径，转换为绝对路径
            if not href.startswith(('http://', 'https://')):
                href = urljoin(base_url, href)
            
            normalized = normalize_url(href)
            if not normalized:
                continue
            
            # 检查是否是外链转内链的情况（链接指向同一域名但可能是跳转页面）
            link_domain = urlparse(normalized).netloc.lower().replace('www.', '')
            
            # 如果链接指向同一域名，检查是否是跳转链接
            if link_domain == base_domain:
                # 检查链接是否包含跳转参数（常见的外链转内链模式）
                parsed = urlparse(normalized)
                query_params = parsed.query.lower()
                # 常见的跳转参数
                redirect_params = ['url', 'target', 'link', 'redirect', 'goto', 'jump', 'to', 'href']
                if any(param in query_params for param in redirect_params):
                    # 尝试从查询参数中提取真实URL
                    params = parse_qs(parsed.query)
                    for param in redirect_params:
                        if param in params:
                            real_url = params[param][0]
                            if real_url.startswith(('http://', 'https://')):
                                normalized = normalize_url(real_url)
                                if normalized:
                                    link_domain = urlparse(normalized).netloc.lower().replace('www.', '')
                                    # 如果提取出的真实URL是外部链接，使用它
                                    if link_domain != base_domain:
                                        links.add(normalized)
                                    continue
                # 如果是同一域名的普通链接，跳过
                continue
            
            # 跳过常见的不相关链接
            skip_domains = [
                'github.com', 'twitter.com', 'facebook.com', 'linkedin.com',
                'weibo.com', 'zhihu.com', 'douban.com', 'bilibili.com',
                'youtube.com', 'instagram.com', 'pinterest.com',
                'mailto:', 'tel:', 'javascript:', '#'
            ]
            
            if any(skip in normalized.lower() for skip in skip_domains):
                continue
            
            links.add(normalized)
        
        return links
    except Exception as e:
        print(f"    提取链接失败: {e}")
        return set()

def find_friend_link_page_urls(homepage_html, homepage_url):
    """从首页HTML中查找友情链接页面的URL"""
    friend_page_urls = []
    base_url = get_base_url(homepage_url)
    
    # 方法1: 从首页HTML中查找友链链接（如果提供了HTML）
    if homepage_html:
        try:
            soup = BeautifulSoup(homepage_html, 'html.parser')
            
            # 查找包含"友链"、"友情链接"等关键词的链接
            keywords = ['友链', '友情链接', 'friends', 'friend', 'link', 'links', 'blogroll', '友情', '链接']
            for tag in soup.find_all('a', href=True):
                text = tag.get_text(strip=True).lower()
                href = tag.get('href', '').strip()
                
                # 检查链接文本或href中是否包含关键词
                if any(keyword in text for keyword in keywords) or \
                   any(keyword in href.lower() for keyword in ['friend', 'link', '友链']):
                    # 转换为绝对URL
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(homepage_url, href)
                    
                    normalized = normalize_url(href)
                    if normalized and is_same_domain(normalized, homepage_url):
                        friend_page_urls.append(normalized)
        except Exception as e:
            print(f"    解析首页HTML失败: {e}")
    
    # 方法2: 添加预存的URI
    if base_url:
        for uri in FRIEND_LINK_URIS:
            if uri.startswith('/'):
                test_url = urljoin(base_url, uri)
            else:
                test_url = urljoin(base_url, '/' + uri)
            if test_url not in friend_page_urls:
                friend_page_urls.append(test_url)
    
    # 去重并返回
    return list(set(friend_page_urls))

def get_all_sites():
    """从数据库获取所有博客站点"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, name, url FROM {SITES_TABLE} ORDER BY id")
            sites = cursor.fetchall()
        
        connection.close()
        return sites
    except Exception as e:
        print(f"✗ 获取站点列表失败: {e}")
        return []

def build_site_url_map(sites):
    """构建站点URL到ID的映射字典，支持多种URL格式匹配"""
    url_map = {}
    base_url_map = {}  # 基础URL（去除www和路径）到站点ID的映射
    
    for site in sites:
        site_id = site['id']
        site_url = site['url']
        
        # 精确URL映射
        url_map[site_url] = site_id
        
        # 基础URL映射（用于模糊匹配）
        try:
            parsed = urlparse(site_url)
            # 去除www前缀
            netloc = parsed.netloc.lower().replace('www.', '')
            base_url = f"{parsed.scheme}://{netloc}"
            
            # 如果同一个基础URL有多个站点，使用第一个
            if base_url not in base_url_map:
                base_url_map[base_url] = site_id
        except:
            pass
    
    return url_map, base_url_map

def get_site_by_url(url, url_map, base_url_map):
    """根据URL查找站点ID（使用预构建的映射字典）"""
    if not url:
        return None
    
    # 尝试精确匹配
    if url in url_map:
        return url_map[url]
    
    # 尝试基础URL匹配（去除www和路径）
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower().replace('www.', '')
        base_url = f"{parsed.scheme}://{netloc}"
        
        if base_url in base_url_map:
            return base_url_map[base_url]
    except:
        pass
    
    return None

def batch_save_friend_links(friend_links_data):
    """批量保存友链关系到数据库"""
    if not friend_links_data:
        return 0
    
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        saved_count = 0
        with connection.cursor() as cursor:
            insert_query = f"""
            INSERT INTO {FRIEND_LINKS_TABLE} (from_site_id, to_site_id, link_type, page_url)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE id = id
            """
            # 批量插入
            try:
                cursor.executemany(insert_query, friend_links_data)
                saved_count = cursor.rowcount
            except Exception as e:
                # 如果批量插入失败，尝试逐个插入
                if 'Duplicate' not in str(e) and '1062' not in str(e):
                    for data in friend_links_data:
                        try:
                            cursor.execute(insert_query, data)
                            saved_count += 1
                        except:
                            pass
        
        connection.commit()
        connection.close()
        return saved_count
    except Exception as e:
        return 0

def batch_save_external_sites(external_sites_data, has_domain=True):
    """批量保存外部网站到数据库（按域名去重）"""
    if not external_sites_data:
        return 0
    
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        saved_count = 0
        with connection.cursor() as cursor:
            if has_domain:
                insert_query = f"""
                INSERT INTO {EXTERNAL_SITES_TABLE} (url, domain, discovered_from_site_id, discovered_from_page, link_type)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id = id
                """
            else:
                insert_query = f"""
                INSERT INTO {EXTERNAL_SITES_TABLE} (url, discovered_from_site_id, discovered_from_page, link_type)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id = id
                """
            
            # 批量插入
            try:
                cursor.executemany(insert_query, external_sites_data)
                saved_count = cursor.rowcount
            except Exception as e:
                # 如果批量插入失败，尝试逐个插入
                if 'Duplicate' not in str(e) and '1062' not in str(e):
                    for data in external_sites_data:
                        try:
                            cursor.execute(insert_query, data)
                            saved_count += 1
                        except:
                            pass
        
        connection.commit()
        connection.close()
        return saved_count
    except Exception as e:
        return 0

def check_table_has_domain():
    """检查external_sites表是否有domain字段"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {EXTERNAL_SITES_TABLE} LIKE 'domain'")
            has_domain = cursor.fetchone() is not None
        connection.close()
        return has_domain
    except:
        return False

def safe_print(*args, **kwargs):
    """线程安全的打印函数"""
    with print_lock:
        print(*args, **kwargs)

def update_site_url_map(url_map, base_url_map, url_map_lock):
    """动态更新站点URL映射（线程安全）"""
    try:
        # 获取最新的站点列表
        all_sites = get_all_sites()
        if not all_sites:
            return url_map, base_url_map
        
        # 构建新的URL映射
        new_url_map, new_base_url_map = build_site_url_map(all_sites)
        
        # 线程安全地更新映射
        with url_map_lock:
            # 合并新的映射到现有映射（保留现有数据，添加新数据）
            url_map.update(new_url_map)
            base_url_map.update(new_base_url_map)
        
        return url_map, base_url_map
    except Exception as e:
        safe_print(f"  警告: 更新站点URL映射失败: {e}")
        return url_map, base_url_map

def get_url_map_snapshot(url_map, base_url_map, url_map_lock):
    """获取URL映射的快照（线程安全）"""
    with url_map_lock:
        # 返回映射的副本，避免在多线程环境下出现问题
        return url_map.copy(), base_url_map.copy()

def crawl_site_links(site, url_map_ref, base_url_map_ref, url_map_lock, has_domain_field=True):
    """爬取单个站点的链接（线程安全版本，返回数据而不是直接保存）
    
    参数:
        url_map_ref: URL映射字典的引用（会被动态更新）
        base_url_map_ref: 基础URL映射字典的引用（会被动态更新）
        url_map_lock: URL映射的线程锁
    """
    site_id = site['id']
    site_name = site['name']
    site_url = site['url']
    
    try:
        friend_links_data = []  # 收集友链数据
        external_sites_data = []  # 收集外部网站数据
        
        # 获取URL映射的快照（线程安全）
        url_map, base_url_map = get_url_map_snapshot(url_map_ref, base_url_map_ref, url_map_lock)
        
        # 1. 爬取主页
        homepage_html, final_url = fetch_page(site_url)
        if homepage_html:
            homepage_links = extract_links(homepage_html, final_url or site_url)
            
            # 处理主页链接
            for link_url in homepage_links:
                to_site_id = get_site_by_url(link_url, url_map, base_url_map)
                
                # 如果没找到，获取最新的URL映射快照（可能新添加了站点）
                if not to_site_id:
                    url_map, base_url_map = get_url_map_snapshot(url_map_ref, base_url_map_ref, url_map_lock)
                    to_site_id = get_site_by_url(link_url, url_map, base_url_map)
                
                if to_site_id:
                    friend_links_data.append((site_id, to_site_id, 'homepage', final_url or site_url))
                else:
                    domain = extract_domain(link_url)
                    if domain:
                        if has_domain_field:
                            external_sites_data.append((link_url, domain, site_id, final_url or site_url, 'homepage'))
                        else:
                            external_sites_data.append((link_url, site_id, final_url or site_url, 'homepage'))
        
        # 2. 查找并爬取友情链接页面
        friend_page_urls = find_friend_link_page_urls(homepage_html, final_url or site_url)
        
        if friend_page_urls:
            # 优先尝试从首页找到的链接，然后尝试预存的URI
            # 最多尝试5个页面
            friend_page_found = False
            for friend_page_url in friend_page_urls[:5]:
                time.sleep(REQUEST_DELAY)
                
                friend_page_html, final_friend_url = fetch_page(friend_page_url)
                if friend_page_html:
                    friend_page_links = extract_links(friend_page_html, final_friend_url or friend_page_url)
                    
                    # 处理友链页面链接
                    for link_url in friend_page_links:
                        to_site_id = get_site_by_url(link_url, url_map, base_url_map)
                        
                        # 如果没找到，获取最新的URL映射快照（可能新添加了站点）
                        if not to_site_id:
                            url_map, base_url_map = get_url_map_snapshot(url_map_ref, base_url_map_ref, url_map_lock)
                            to_site_id = get_site_by_url(link_url, url_map, base_url_map)
                        
                        if to_site_id:
                            friend_links_data.append((site_id, to_site_id, 'friend_page', final_friend_url or friend_page_url))
                        else:
                            domain = extract_domain(link_url)
                            if domain:
                                if has_domain_field:
                                    external_sites_data.append((link_url, domain, site_id, final_friend_url or friend_page_url, 'friend_page'))
                                else:
                                    external_sites_data.append((link_url, site_id, final_friend_url or friend_page_url, 'friend_page'))
                    
                    friend_page_found = True
                    # 如果找到的链接数量较多，认为这是有效的友链页面，可以停止尝试
                    if len(friend_page_links) > 3:
                        break
        
        # 批量保存数据
        friend_links_count = batch_save_friend_links(friend_links_data)
        external_sites_count = batch_save_external_sites(external_sites_data, has_domain_field)
        
        return friend_links_count, external_sites_count, None
    except Exception as e:
        import traceback
        with print_lock:
            traceback.print_exc()
        return 0, 0, str(e)

def main():
    """主函数"""
    print("=" * 60)
    print("博客友链爬虫程序")
    print("=" * 60)
    
    # 初始化数据库
    if not init_database():
        print("\n数据库初始化失败！")
        return
    
    # 获取所有站点
    print("\n从数据库获取站点列表...")
    sites = get_all_sites()
    if not sites:
        print("没有找到任何站点！")
        return
    
    print(f"找到 {len(sites)} 个站点")
    
    # 询问是否从上次中断的地方继续
    print("\n选项:")
    print("1. 从头开始爬取所有站点")
    print("2. 从指定ID开始爬取")
    print("3. 只爬取未处理的站点（推荐）")
    
    try:
        choice = input("请选择 (1/2/3，默认3): ").strip() or "3"
    except:
        choice = "3"
    
    start_id = 1
    if choice == "2":
        try:
            start_id = int(input("请输入起始站点ID: ").strip())
        except:
            start_id = 1
    
    # 获取已处理的站点ID（如果选择选项3）
    processed_site_ids = set()
    if choice == "3":
        try:
            connection = pymysql.connect(
                **DB_CONFIG,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor
            )
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT from_site_id FROM {FRIEND_LINKS_TABLE}")
                processed_site_ids = {row['from_site_id'] for row in cursor.fetchall()}
                cursor.execute(f"SELECT DISTINCT discovered_from_site_id FROM {EXTERNAL_SITES_TABLE}")
                processed_site_ids.update({row['discovered_from_site_id'] for row in cursor.fetchall()})
            connection.close()
            print(f"已处理的站点数: {len(processed_site_ids)}")
        except:
            pass
    
    # 筛选要处理的站点
    sites_to_process = []
    for site in sites:
        if choice == "2" and site['id'] < start_id:
            continue
        if choice == "3" and site['id'] in processed_site_ids:
            continue
        sites_to_process.append(site)
    
    print(f"\n将处理 {len(sites_to_process)} 个站点")
    
    # 询问线程数
    print("\n线程配置:")
    try:
        max_workers_input = input(f"请输入并发线程数 (默认{MAX_WORKERS}，建议3-10): ").strip()
        max_workers = int(max_workers_input) if max_workers_input else MAX_WORKERS
        if max_workers < 1:
            max_workers = 1
        if max_workers > 20:
            print("警告: 线程数过多可能导致性能下降或被服务器封禁，已限制为20")
            max_workers = 20
    except:
        max_workers = MAX_WORKERS
    
    print(f"使用 {max_workers} 个并发线程")
    
    # 构建站点URL映射（用于快速查找）
    print("\n构建站点URL映射...")
    url_map, base_url_map = build_site_url_map(sites)
    print(f"已构建 {len(url_map)} 个精确URL映射，{len(base_url_map)} 个基础URL映射")
    
    # 检查表是否有domain字段（只检查一次）
    has_domain_field = check_table_has_domain()
    
    # 线程安全的统计信息
    class Stats:
        def __init__(self):
            self.friend_links = 0
            self.external_sites = 0
            self.success = 0
            self.fail = 0
            self.processed = 0
            self.lock = threading.Lock()
        
        def update(self, friend_links, external_sites, error=None):
            with self.lock:
                self.processed += 1
                if error:
                    self.fail += 1
                else:
                    self.success += 1
                    self.friend_links += friend_links
                    self.external_sites += external_sites
        
        def get_stats(self):
            with self.lock:
                return {
                    'processed': self.processed,
                    'success': self.success,
                    'fail': self.fail,
                    'friend_links': self.friend_links,
                    'external_sites': self.external_sites
                }
    
    stats = Stats()
    start_time = time.time()
    executor = None
    
    # 使用线程池并发爬取
    print(f"\n开始爬取...")
    print("=" * 60)
    print("提示: 站点列表会动态更新，新添加的博客站点会被自动识别为友链")
    
    # 创建进度条（在底部显示）
    pbar = tqdm(total=len(sites_to_process), desc="爬取进度", unit="站点", 
                position=0, leave=True, ncols=100, 
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
    
    # URL映射更新计数器（每处理N个站点后更新一次）
    url_map_update_interval = 10  # 每处理10个站点后更新一次URL映射
    processed_count_since_update = 0
    
    try:
        executor = ThreadPoolExecutor(max_workers=max_workers)
        # 提交所有任务
        future_to_site = {
            executor.submit(crawl_site_links, site, url_map, base_url_map, url_map_lock, has_domain_field): site 
            for site in sites_to_process
        }
        
        # 处理完成的任务
        try:
            for future in as_completed(future_to_site):
                site = future_to_site[future]
                try:
                    friend_links, external_sites, error = future.result()
                    stats.update(friend_links, external_sites, error)
                    
                    processed_count_since_update += 1
                    
                    # 定期更新URL映射（包含新添加的站点）
                    if processed_count_since_update >= url_map_update_interval:
                        safe_print(f"\n  更新站点URL映射（包含新添加的博客站点）...")
                        url_map, base_url_map = update_site_url_map(url_map, base_url_map, url_map_lock)
                        with url_map_lock:
                            map_size = len(url_map)
                            base_map_size = len(base_url_map)
                        safe_print(f"  当前站点URL映射: {map_size} 个精确URL，{base_map_size} 个基础URL")
                        processed_count_since_update = 0
                    
                    # 更新进度条
                    current_stats = stats.get_stats()
                    pbar.set_postfix({
                        '成功': current_stats['success'],
                        '失败': current_stats['fail'],
                        '友链': current_stats['friend_links'],
                        '外站': current_stats['external_sites']
                    })
                    pbar.update(1)
                        
                except Exception as e:
                    stats.update(0, 0, str(e))
                    pbar.update(1)
        except KeyboardInterrupt:
            pbar.close()
            safe_print("\n\n用户中断，正在等待当前任务完成...")
            # 取消未完成的任务
            for future in future_to_site:
                future.cancel()
            # 等待已完成的任务
            executor.shutdown(wait=True)
            safe_print("已停止")
            return
        finally:
            pbar.close()
            executor.shutdown(wait=True)
                
    except Exception as e:
        pbar.close()
        safe_print(f"线程池执行异常: {e}")
        if executor:
            executor.shutdown(wait=False)
    
    # 统计信息
    final_stats = stats.get_stats()
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("爬取完成！")
    print("=" * 60)
    print(f"处理站点数: {final_stats['processed']}/{len(sites_to_process)}")
    print(f"成功: {final_stats['success']}")
    print(f"失败: {final_stats['fail']}")
    print(f"累计发现友链关系: {final_stats['friend_links']} 个")
    print(f"累计发现外部网站: {final_stats['external_sites']} 个")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    if final_stats['processed'] > 0:
        print(f"平均速度: {final_stats['processed']/elapsed_time:.2f} 站点/秒")
    print("=" * 60)

if __name__ == '__main__':
    main()


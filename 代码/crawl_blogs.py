#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
博客网站爬虫脚本
从两个API爬取博客网站信息并存储到MySQL数据库
"""

import requests
import pymysql
from bs4 import BeautifulSoup
import json
import time
from urllib.parse import urlparse
import re
import os
import urllib3
import csv
import io

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
TABLE_NAME = 'sites'

def init_database():
    """初始化数据库和表"""
    try:
        # 检查数据库配置
        if not DB_CONFIG.get('password') and not os.getenv('DB_PASSWORD'):
            print("警告: 未设置数据库密码，如果MySQL需要密码，请设置环境变量 DB_PASSWORD 或创建 config.py 文件")
        
        # 连接MySQL服务器（不指定数据库）
        connection = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        
        with connection.cursor() as cursor:
            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {DB_NAME}")
            
            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
            table_exists = cursor.fetchone() is not None
            
            if table_exists:
                # 如果表已存在，尝试删除有问题的索引并重新创建
                try:
                    # 尝试删除旧的唯一索引
                    cursor.execute(f"SHOW INDEX FROM {TABLE_NAME} WHERE Key_name = 'unique_url'")
                    if cursor.fetchone():
                        cursor.execute(f"ALTER TABLE {TABLE_NAME} DROP INDEX unique_url")
                        print(f"  删除旧的唯一索引...")
                except:
                    pass
                
                # 检查并修改url字段长度
                try:
                    cursor.execute(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN url VARCHAR(500) NOT NULL")
                except:
                    pass
                
                # 重新创建前缀索引
                try:
                    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD UNIQUE KEY unique_url (url(191))")
                    print(f"  已更新表结构...")
                except Exception as e:
                    print(f"  警告: 更新索引时出现错误: {e}")
            else:
                # 创建新表
                # 使用前缀索引避免键长度超过限制（utf8mb4下，191字符*4字节=764字节<1000字节）
                create_table_query = f"""
                CREATE TABLE {TABLE_NAME} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    url VARCHAR(500) NOT NULL,
                    UNIQUE KEY unique_url (url(191))
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
                cursor.execute(create_table_query)
            
        connection.commit()
        print(f"✓ 数据库 {DB_NAME} 和表 {TABLE_NAME} 初始化成功")
        connection.close()
        return True
    except Exception as e:
        print(f"✗ 数据库初始化失败: {e}")
        return False

def normalize_url(url):
    """规范化URL"""
    if not url:
        return None
    
    # 去除前后空格
    url = url.strip()
    
    # 如果没有协议，添加https://
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # 移除末尾的斜杠（保留协议后的）
    url = url.rstrip('/')
    if url.endswith('://'):
        url = url[:-3]
    
    return url

def crawl_zhblogs():
    """从zhblogs.net API爬取博客数据"""
    blogs = []
    url = "https://www.zhblogs.net/api/blog/list?page=1&pageSize=7000"
    
    try:
        print(f"正在爬取: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # API返回格式: {"code": 200, "data": {"data": [...], "total": ...}}
        if 'data' in data and isinstance(data['data'], dict):
            items = data['data'].get('data', [])
        elif 'data' in data and isinstance(data['data'], list):
            items = data['data']
        elif isinstance(data, list):
            items = data
        else:
            items = []
        
        for item in items:
            name = item.get('name') or item.get('title') or item.get('blogName')
            url_value = item.get('url') or item.get('link') or item.get('homepage')
            
            if name and url_value:
                url_normalized = normalize_url(url_value)
                if url_normalized:
                    blogs.append({
                        'name': name.strip(),
                        'url': url_normalized
                    })
        
        print(f"✓ 从zhblogs.net获取到 {len(blogs)} 个博客")
        return blogs
        
    except Exception as e:
        print(f"✗ 爬取zhblogs.net失败: {e}")
        import traceback
        traceback.print_exc()
        return []

def crawl_alexsci():
    """从alexsci.com爬取博客数据"""
    blogs = []
    base_url = "https://alexsci.com/rss-blogroll-network/discover/"
    
    # 首先尝试查找API端点
    api_urls = [
        "https://alexsci.com/rss-blogroll-network/api/feeds",
        "https://alexsci.com/api/feeds",
        "https://alexsci.com/rss-blogroll-network/feeds.json",
    ]
    
    for api_url in api_urls:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            response = requests.get(api_url, headers=headers, timeout=10)
            if response.status_code == 200:
                try:
                    data = response.json()
                    # 处理JSON格式的API响应
                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict) and 'data' in data:
                        items = data['data'] if isinstance(data['data'], list) else []
                    elif isinstance(data, dict) and 'feeds' in data:
                        items = data['feeds'] if isinstance(data['feeds'], list) else []
                    else:
                        items = []
                    
                    for item in items:
                        name = item.get('name') or item.get('title') or item.get('blogName')
                        url_value = item.get('url') or item.get('link') or item.get('homepage') or item.get('feed')
                        
                        if name and url_value:
                            url_normalized = normalize_url(url_value)
                            if url_normalized and 'alexsci.com' not in url_normalized:
                                blogs.append({
                                    'name': name.strip(),
                                    'url': url_normalized
                                })
                    
                    if blogs:
                        print(f"✓ 从alexsci.com API获取到 {len(blogs)} 个博客")
                        return blogs
                except json.JSONDecodeError:
                    continue
        except:
            continue
    
    # 如果API不可用，解析HTML页面
    try:
        print(f"正在爬取: {base_url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 解析HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 方法1: 查找表格中的数据
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 1:
                    # 第一列是博客名称，可能包含链接
                    name_cell = cols[0]
                    
                    # 查找链接
                    link_tag = name_cell.find('a', href=True)
                    if link_tag:
                        name = link_tag.get_text(strip=True)
                        url_value = link_tag.get('href', '').strip()
                        
                        # 如果URL是相对路径，转换为绝对路径
                        if url_value and not url_value.startswith(('http://', 'https://')):
                            if url_value.startswith('/'):
                                url_value = 'https://alexsci.com' + url_value
                            else:
                                url_value = 'https://alexsci.com/' + url_value
                    else:
                        # 如果没有链接，尝试获取文本作为名称
                        name = name_cell.get_text(strip=True)
                        url_value = None
                    
                    # 如果名称存在但URL不存在，检查其他列
                    if name and not url_value and len(cols) > 1:
                        for col in cols[1:]:
                            link = col.find('a', href=True)
                            if link:
                                url_value = link.get('href', '').strip()
                                break
                            # 检查列文本是否为URL
                            text = col.get_text(strip=True)
                            if text and text.startswith(('http://', 'https://')):
                                url_value = text
                                break
                    
                    if name and url_value:
                        url_normalized = normalize_url(url_value)
                        if url_normalized and 'alexsci.com' not in url_normalized:
                            blogs.append({
                                'name': name,
                                'url': url_normalized
                            })
        
        # 方法2: 如果表格解析失败，查找页面上的所有外部链接
        if not blogs:
            print("  表格解析未找到数据，尝试查找页面链接...")
            # 查找主要内容区域的链接
            main_content = soup.find('main') or soup.find('div', class_='content') or soup.find('body')
            if main_content:
                links = main_content.find_all('a', href=True)
            else:
                links = soup.find_all('a', href=True)
            
            seen_urls = set()
            for link in links:
                href = link.get('href', '').strip()
                text = link.get_text(strip=True)
                
                # 跳过空链接和站内链接
                if not href or 'alexsci.com' in href:
                    continue
                
                # 处理相对路径
                if not href.startswith(('http://', 'https://')):
                    continue
                
                url_normalized = normalize_url(href)
                if url_normalized and url_normalized not in seen_urls:
                    # 使用链接文本作为名称，如果没有文本则使用域名
                    if not text or len(text.strip()) < 2:
                        try:
                            parsed = urlparse(url_normalized)
                            text = parsed.netloc.replace('www.', '')
                        except:
                            text = url_normalized
                    
                    # 过滤掉明显不是博客的链接（如社交媒体、邮箱等）
                    if any(skip in url_normalized.lower() for skip in ['mailto:', 'twitter.com', 'facebook.com', 'github.com', 'linkedin.com']):
                        continue
                    
                    blogs.append({
                        'name': text.strip(),
                        'url': url_normalized
                    })
                    seen_urls.add(url_normalized)
        
        print(f"✓ 从alexsci.com获取到 {len(blogs)} 个博客")
        return blogs
        
    except Exception as e:
        print(f"✗ 爬取alexsci.com失败: {e}")
        import traceback
        traceback.print_exc()
        return []

def crawl_bf_zzxworld():
    """从bf.zzxworld.com API爬取博客数据（支持分页）"""
    blogs = []
    base_url = "https://bf.zzxworld.com/api/sites"
    page = 1
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        while True:
            if page == 1:
                url = base_url
            else:
                url = f"{base_url}/{page}"
            
            print(f"正在爬取: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # API返回格式: {"sites": [...], "has_more_page": true/false}
            if 'sites' in data and isinstance(data['sites'], list):
                items = data['sites']
            elif isinstance(data, list):
                items = data
            else:
                items = []
            
            for item in items:
                name = item.get('title') or item.get('name') or item.get('blogName')
                url_value = item.get('url') or item.get('link') or item.get('homepage')
                
                if name and url_value:
                    url_normalized = normalize_url(url_value)
                    if url_normalized:
                        blogs.append({
                            'name': name.strip(),
                            'url': url_normalized
                        })
            
            # 检查是否还有更多页面
            has_more = data.get('has_more_page', False)
            if not has_more:
                break
            
            page += 1
            time.sleep(0.5)  # 避免请求过快
        
        print(f"✓ 从bf.zzxworld.com获取到 {len(blogs)} 个博客（共 {page} 页）")
        return blogs
        
    except Exception as e:
        print(f"✗ 爬取bf.zzxworld.com失败: {e}")
        if blogs:
            print(f"  已获取 {len(blogs)} 个博客")
        import traceback
        traceback.print_exc()
        return blogs

def crawl_foreverblog():
    """从foreverblog.cn爬取博客数据"""
    blogs = []
    base_url = "https://www.foreverblog.cn/"
    member_url = "https://www.foreverblog.cn/blogs.html"
    
    # 首先尝试查找API端点
    api_urls = [
        "https://www.foreverblog.cn/api/members",
        "https://www.foreverblog.cn/api/blog/list",
        "https://www.foreverblog.cn/members.json",
        "https://www.foreverblog.cn/api/blogs",
    ]
    
    for api_url in api_urls:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            # 禁用SSL验证
            response = requests.get(api_url, headers=headers, timeout=10, verify=False)
            if response.status_code == 200:
                try:
                    data = response.json()
                    # 处理JSON格式的API响应
                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict) and 'data' in data:
                        items = data['data'] if isinstance(data['data'], list) else []
                    elif isinstance(data, dict) and 'members' in data:
                        items = data['members'] if isinstance(data['members'], list) else []
                    elif isinstance(data, dict) and 'blogs' in data:
                        items = data['blogs'] if isinstance(data['blogs'], list) else []
                    else:
                        items = []
                    
                    for item in items:
                        name = item.get('name') or item.get('title') or item.get('blogName')
                        url_value = item.get('url') or item.get('link') or item.get('homepage') or item.get('blog_url')
                        
                        if name and url_value:
                            url_normalized = normalize_url(url_value)
                            if url_normalized and 'foreverblog.cn' not in url_normalized:
                                blogs.append({
                                    'name': name.strip(),
                                    'url': url_normalized
                                })
                    
                    if blogs:
                        print(f"✓ 从foreverblog.cn API获取到 {len(blogs)} 个博客")
                        return blogs
                except json.JSONDecodeError:
                    continue
        except:
            continue
    
    # 如果API不可用，解析HTML页面
    try:
        print(f"正在爬取: {member_url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        # 禁用SSL验证以避免证书问题
        response = requests.get(member_url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        
        # 解析HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 方法1: 查找表格中的数据
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 1:
                    # 查找包含博客链接的列
                    for col in cols:
                        link_tag = col.find('a', href=True)
                        if link_tag:
                            href = link_tag.get('href', '').strip()
                            text = link_tag.get_text(strip=True)
                            
                            # 过滤外部博客链接
                            if href and href.startswith(('http://', 'https://')):
                                if 'foreverblog.cn' not in href:
                                    # 排除非博客链接
                                    skip_domains = ['qm.qq.com', 'github.com', 'beian.miit.gov.cn', 'jetli.com.cn']
                                    if not any(domain in href for domain in skip_domains):
                                        url_normalized = normalize_url(href)
                                        if url_normalized:
                                            name = text.strip() if text.strip() else url_normalized
                                            blogs.append({
                                                'name': name,
                                                'url': url_normalized
                                            })
        
        # 方法2: 查找所有外部链接
        if not blogs:
            print("  表格解析未找到数据，尝试查找页面链接...")
            # 查找主要内容区域的链接
            main_content = soup.find('main') or soup.find('div', class_='content') or soup.find('div', id='content') or soup.find('body')
            if main_content:
                links = main_content.find_all('a', href=True)
            else:
                links = soup.find_all('a', href=True)
            
            seen_urls = set()
            skip_domains = ['foreverblog.cn', 'qm.qq.com', 'github.com', 'beian.miit.gov.cn', 'jetli.com.cn', 'qq.com']
            
            for link in links:
                href = link.get('href', '').strip()
                text = link.get_text(strip=True)
                
                # 跳过空链接和站内链接
                if not href or not href.startswith(('http://', 'https://')):
                    continue
                
                # 排除不需要的域名
                if any(domain in href for domain in skip_domains):
                    continue
                
                url_normalized = normalize_url(href)
                if url_normalized and url_normalized not in seen_urls:
                    # 使用链接文本作为名称，如果没有文本则使用域名
                    if not text or len(text.strip()) < 2:
                        try:
                            parsed = urlparse(url_normalized)
                            text = parsed.netloc.replace('www.', '')
                        except:
                            text = url_normalized
                    
                    blogs.append({
                        'name': text.strip(),
                        'url': url_normalized
                    })
                    seen_urls.add(url_normalized)
        
        print(f"✓ 从foreverblog.cn获取到 {len(blogs)} 个博客")
        return blogs
        
    except Exception as e:
        print(f"✗ 爬取foreverblog.cn失败: {e}")
        import traceback
        traceback.print_exc()
        return []

def crawl_github_chinese_blogs():
    """从GitHub仓库 timqian/chinese-independent-blogs 爬取博客数据"""
    blogs = []
    csv_url = "https://raw.githubusercontent.com/timqian/chinese-independent-blogs/master/blogs-original.csv"
    
    try:
        print(f"正在爬取: {csv_url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(csv_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 解析CSV内容
        csv_content = response.text
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        for row in csv_reader:
            # CSV列名: 'Introduction', ' Address', ' RSS feed', ' tags'
            # 注意有些列名前有空格，需要处理
            name = row.get('Introduction') or row.get('name') or row.get('title') or row.get('博客名称') or row.get('名称')
            url_value = row.get(' Address') or row.get('Address') or row.get('url') or row.get('link') or row.get('博客链接') or row.get('链接') or row.get('homepage')
            
            # 如果没有找到标准列名，尝试获取第一列作为名称，第二列作为URL
            if not name and not url_value:
                values = list(row.values())
                if len(values) >= 2:
                    name = values[0] if values[0] else None
                    url_value = values[1] if values[1] else None
            
            if name and url_value:
                # 清理数据
                name = name.strip().strip('"').strip("'").strip()
                url_value = url_value.strip().strip('"').strip("'").strip()
                
                # 跳过空值或无效值
                if not name or not url_value or name.lower() in ['none', 'null', ''] or url_value.lower() in ['none', 'null', '']:
                    continue
                
                # 跳过标题行（如果存在）
                if name.lower() in ['introduction', 'name', '博客名称', '名称']:
                    continue
                
                # 规范化URL
                url_normalized = normalize_url(url_value)
                if url_normalized:
                    blogs.append({
                        'name': name,
                        'url': url_normalized
                    })
        
        print(f"✓ 从GitHub chinese-independent-blogs获取到 {len(blogs)} 个博客")
        return blogs
        
    except Exception as e:
        print(f"✗ 爬取GitHub chinese-independent-blogs失败: {e}")
        import traceback
        traceback.print_exc()
        return []

def save_to_database(blogs):
    """将博客数据保存到数据库"""
    if not blogs:
        print("没有数据需要保存")
        return
    
    try:
        # 连接到数据库
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        inserted_count = 0
        skipped_count = 0
        
        with connection.cursor() as cursor:
            for blog in blogs:
                try:
                    # 使用INSERT IGNORE或ON DUPLICATE KEY UPDATE来避免重复
                    insert_query = f"""
                    INSERT INTO {TABLE_NAME} (name, url) 
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE name = VALUES(name)
                    """
                    cursor.execute(insert_query, (blog['name'], blog['url']))
                    # 检查受影响的行数：1表示插入新记录，2表示更新了已有记录
                    if cursor.rowcount == 1:
                        inserted_count += 1
                    elif cursor.rowcount == 2:
                        skipped_count += 1
                    else:
                        inserted_count += 1
                except Exception as e:
                    print(f"  插入记录失败: {blog.get('name', 'Unknown')} - {e}")
                    skipped_count += 1
                    continue
        
        connection.commit()
        print(f"✓ 成功插入 {inserted_count} 条记录，跳过 {skipped_count} 条重复记录")
        connection.close()
        
    except Exception as e:
        print(f"✗ 保存到数据库失败: {e}")
        import traceback
        traceback.print_exc()

def drop_table_if_exists():
    """删除表（用于修复表结构问题）"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        connection.commit()
        connection.close()
        print(f"✓ 已删除表 {TABLE_NAME}")
        return True
    except Exception as e:
        print(f"✗ 删除表失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 50)
    print("博客网站爬虫程序")
    print("=" * 50)
    
    # 初始化数据库
    if not init_database():
        print("\n数据库初始化失败！")
        print("如果是因为表结构问题，可以尝试手动删除表后重新运行。")
        print(f"执行以下SQL: DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME};")
        return
    
    # 爬取数据
    all_blogs = []
    
    # 从zhblogs.net爬取（已爬取，跳过）
    # zhblogs_data = crawl_zhblogs()
    # all_blogs.extend(zhblogs_data)
    # time.sleep(1)
    
    # 从alexsci.com爬取（已爬取，跳过）
    # alexsci_data = crawl_alexsci()
    # all_blogs.extend(alexsci_data)
    # time.sleep(1)
    
    # 从bf.zzxworld.com爬取（已爬取，跳过）
    # bf_data = crawl_bf_zzxworld()
    # all_blogs.extend(bf_data)
    # time.sleep(1)
    
    # 从foreverblog.cn爬取（已爬取，跳过）
    # foreverblog_data = crawl_foreverblog()
    # all_blogs.extend(foreverblog_data)
    # time.sleep(1)
    
    # 从GitHub chinese-independent-blogs爬取
    github_data = crawl_github_chinese_blogs()
    all_blogs.extend(github_data)
    
    # 去重（基于URL）
    seen_urls = set()
    unique_blogs = []
    for blog in all_blogs:
        if blog['url'] not in seen_urls:
            seen_urls.add(blog['url'])
            unique_blogs.append(blog)
    
    print(f"\n总共获取 {len(all_blogs)} 个博客，去重后 {len(unique_blogs)} 个")
    
    # 保存到数据库
    if unique_blogs:
        save_to_database(unique_blogs)
    else:
        print("没有有效数据需要保存")
    
    print("\n程序执行完成！")

if __name__ == '__main__':
    main()


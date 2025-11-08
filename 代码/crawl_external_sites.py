#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
外部网站博客识别脚本
从external_sites表获取外链，爬取首页和关键信息，使用AI判断是否为博客网站
如果是博客网站，添加到sites表，并将原本的外链改成友情链接存到friend_links表
"""

import requests
import pymysql
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urljoin
import re
import os
import urllib3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
import json

# 尝试导入腾讯云SDK
try:
    from tencentcloud.common import credential
    from tencentcloud.common.profile.client_profile import ClientProfile
    from tencentcloud.common.profile.http_profile import HttpProfile
    from tencentcloud.hunyuan.v20230901 import hunyuan_client, models
    TENCENT_SDK_AVAILABLE = True
except ImportError:
    TENCENT_SDK_AVAILABLE = False
    print("警告: 未安装腾讯云SDK，请运行: pip install tencentcloud-sdk-python")

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 尝试从配置文件导入数据库配置和腾讯云配置
try:
    from config import DB_CONFIG as CONFIG_DB_CONFIG
    DB_CONFIG = CONFIG_DB_CONFIG.copy()
except ImportError:
    DB_CONFIG = {}

# 尝试从配置文件导入腾讯云配置
try:
    from config import TENCENT_CONFIG
except ImportError:
    TENCENT_CONFIG = {}

# 数据库配置
DB_CONFIG.setdefault('host', os.getenv('DB_HOST', 'localhost'))
DB_CONFIG.setdefault('port', int(os.getenv('DB_PORT', 3306)))
DB_CONFIG.setdefault('user', os.getenv('DB_USER', 'root'))
DB_CONFIG.setdefault('password', os.getenv('DB_PASSWORD', 'root'))
DB_CONFIG.setdefault('charset', 'utf8mb4')

# 腾讯云配置
TENCENT_CONFIG.setdefault('secret_id', os.getenv('TENCENT_SECRET_ID', ''))
TENCENT_CONFIG.setdefault('secret_key', os.getenv('TENCENT_SECRET_KEY', ''))
TENCENT_CONFIG.setdefault('region', os.getenv('TENCENT_REGION', 'ap-beijing'))
TENCENT_CONFIG.setdefault('model', os.getenv('TENCENT_MODEL', 'hunyuan-a13b'))

DB_NAME = 'blog_link'
SITES_TABLE = 'sites'
EXTERNAL_SITES_TABLE = 'external_sites'
FRIEND_LINKS_TABLE = 'friend_links'

# 请求配置
REQUEST_TIMEOUT = 15
REQUEST_DELAY = 1  # 请求间隔（秒）
MAX_WORKERS = 5  # 最大并发线程数（AI API有并发限制）
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# 线程锁
print_lock = threading.Lock()
db_lock = threading.Lock()  # 数据库操作锁，避免并发冲突

def safe_print(*args, **kwargs):
    """线程安全的打印函数"""
    with print_lock:
        print(*args, **kwargs)

def init_database():
    """初始化数据库，添加processed字段到external_sites表"""
    try:
        connection = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        
        with connection.cursor() as cursor:
            cursor.execute(f"USE {DB_NAME}")
            
            # 检查external_sites表是否有processed字段
            cursor.execute(f"SHOW COLUMNS FROM {EXTERNAL_SITES_TABLE} LIKE 'processed'")
            has_processed = cursor.fetchone() is not None
            
            if not has_processed:
                # 添加processed字段，标记是否已处理
                # 0-未处理, 1-已处理（是博客）, 2-处理失败（不是博客或其他错误）, 3-处理中（临时状态）
                cursor.execute(f"""
                    ALTER TABLE {EXTERNAL_SITES_TABLE} 
                    ADD COLUMN processed TINYINT DEFAULT 0 COMMENT '处理状态: 0-未处理, 1-已处理, 2-处理失败, 3-处理中' AFTER created_at,
                    ADD INDEX idx_processed (processed)
                """)
                print("  ✓ 已添加processed字段到external_sites表")
            else:
                # 如果字段已存在，检查是否有长时间处于处理中状态的记录（超过1小时）
                # 将这些记录重置为未处理状态（防止程序异常退出导致记录一直处于处理中状态）
                cursor.execute(f"""
                    UPDATE {EXTERNAL_SITES_TABLE} 
                    SET processed = 0 
                    WHERE processed = 3 
                    AND created_at < DATE_SUB(NOW(), INTERVAL 1 HOUR)
                """)
                reset_count = cursor.rowcount
                if reset_count > 0:
                    print(f"  ✓ 已重置 {reset_count} 条长时间处于处理中状态的记录")
            
        connection.commit()
        connection.close()
        return True
    except Exception as e:
        print(f"✗ 数据库初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def fetch_page(url, max_retries=3):
    """获取网页内容"""
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, 
                headers=REQUEST_HEADERS, 
                timeout=REQUEST_TIMEOUT,
                verify=False,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type:
                    html = response.text
                    final_url = response.url
                    return html, final_url
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

def extract_key_info(html, url):
    """从HTML中提取关键信息（全面提取以避免AI误判）"""
    if not html:
        return {}
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        info = {
            'title': '',
            'description': '',
            'keywords': '',
            'content_preview': '',
            'navigation_links': [],
            'article_links': [],
            'rss_feeds': [],
            'categories': [],
            'tags': [],
            'post_count': 0,
            'has_blog_indicators': False,
            'page_structure': '',
            'main_content': ''
        }
        
        # 提取标题
        title_tag = soup.find('title')
        if title_tag:
            info['title'] = title_tag.get_text(strip=True)
        
        # 提取meta描述
        meta_desc = soup.find('meta', attrs={'name': re.compile('description', re.I)})
        if meta_desc:
            info['description'] = meta_desc.get('content', '').strip()
        
        # 提取关键词
        meta_keywords = soup.find('meta', attrs={'name': re.compile('keywords', re.I)})
        if meta_keywords:
            info['keywords'] = meta_keywords.get('content', '').strip()
        
        # 提取RSS/Feed链接
        # 方法1: 从link标签提取（type包含rss/atom/feed/xml）
        for link in soup.find_all('link', {'type': re.compile('rss|atom|feed|xml', re.I)}):
            href = link.get('href')
            if href:
                rss_url = urljoin(url, href)
                if rss_url not in info['rss_feeds']:
                    info['rss_feeds'].append(rss_url)
        
        # 方法2: 从rel=alternate的link标签提取
        for link in soup.find_all('link', {'rel': re.compile('alternate', re.I)}):
            link_type = link.get('type', '').lower()
            if 'rss' in link_type or 'atom' in link_type or 'xml' in link_type:
                href = link.get('href')
                if href:
                    rss_url = urljoin(url, href)
                    if rss_url not in info['rss_feeds']:
                        info['rss_feeds'].append(rss_url)
        
        # 方法3: 从a标签提取RSS链接（链接文本或href包含rss/feed/atom等关键词）
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            text = a_tag.get_text(strip=True).lower()
            # 检查href或链接文本是否包含RSS相关关键词
            if (re.search(r'\.(rss|xml|atom|feed)|/feed|/rss', href, re.I) or 
                any(keyword in text for keyword in ['rss', 'feed', 'atom', '订阅', 'subscribe'])):
                rss_url = urljoin(url, href)
                if rss_url not in info['rss_feeds']:
                    info['rss_feeds'].append(rss_url)
        
        # 查找导航链接（通常包含博客相关的关键词）
        nav_keywords = ['博客', 'blog', '文章', 'post', 'archive', '归档', '分类', 'category', 
                       '标签', 'tag', '标签云', '标签页', '目录', 'categories', 'tags',
                       '关于', 'about', '友链', 'friend', '链接', 'link', '留言', 'comment',
                       '评论', '留言板', 'guestbook']
        nav_links = []
        for nav in soup.find_all(['nav', 'ul', 'div'], class_=re.compile('nav|menu|header', re.I)):
            for link in nav.find_all('a', href=True):
                link_text = link.get_text(strip=True).lower()
                href = link.get('href', '')
                if any(keyword in link_text for keyword in nav_keywords):
                    nav_links.append(f"{link_text} -> {href}")
        info['navigation_links'] = nav_links[:10]  # 限制数量
        
        # 查找文章/帖子链接
        article_links = []
        seen_links = set()  # 用于去重
        
        # 方法1: 从article标签中提取链接
        for article in soup.find_all('article')[:20]:
            for link in article.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if href and text and len(text) > 5 and href not in seen_links:  # 过滤太短的文本
                    seen_links.add(href)
                    article_links.append(f"{text[:50]} -> {href}")
        
        # 方法2: 从包含post/article/entry/blog类的div中提取链接
        for div in soup.find_all('div', class_=re.compile('post|article|entry|blog|content', re.I))[:30]:
            for link in div.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                # 过滤掉太短的链接文本和常见的非文章链接
                if (href and text and len(text) > 5 and href not in seen_links and
                    not any(skip in text.lower() for skip in ['首页', 'home', '关于', 'about', '联系', 'contact', '登录', 'login', '注册', 'register'])):
                    seen_links.add(href)
                    article_links.append(f"{text[:50]} -> {href}")
        
        # 方法3: 从URL路径包含post/article/blog/entry/p的链接中提取
        for link in soup.find_all('a', href=re.compile(r'/(post|article|blog|entry|p|archives|archive)/', re.I))[:30]:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if href and text and len(text) > 3 and href not in seen_links:
                seen_links.add(href)
                article_links.append(f"{text[:50]} -> {href}")
        
        # 方法4: 从class包含post/article/entry的a标签中提取
        for link in soup.find_all('a', class_=re.compile('post|article|entry|title', re.I))[:20]:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if href and text and len(text) > 5 and href not in seen_links:
                seen_links.add(href)
                article_links.append(f"{text[:50]} -> {href}")
        
        # 方法5: 从h1-h6标题标签中查找链接（标题通常指向文章）
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            link = heading.find('a', href=True)
            if link:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if href and text and len(text) > 5 and href not in seen_links:
                    seen_links.add(href)
                    article_links.append(f"{text[:50]} -> {href}")
        
        info['article_links'] = article_links[:15]  # 限制数量
        info['post_count'] = len(info['article_links'])
        
        # 查找分类和标签
        category_patterns = [
            soup.find_all('a', href=re.compile(r'/category|/cat|/categories', re.I)),
            soup.find_all('a', class_=re.compile('category|cat', re.I)),
            soup.find_all('div', class_=re.compile('category|categories', re.I)),
        ]
        categories = []
        for pattern_result in category_patterns:
            for item in pattern_result:
                text = item.get_text(strip=True)
                if text and text not in categories:
                    categories.append(text)
        info['categories'] = categories[:10]
        
        tag_patterns = [
            soup.find_all('a', href=re.compile(r'/tag|/tags', re.I)),
            soup.find_all('a', class_=re.compile('tag', re.I)),
            soup.find_all('div', class_=re.compile('tag|tags', re.I)),
        ]
        tags = []
        for pattern_result in tag_patterns:
            for item in pattern_result:
                text = item.get_text(strip=True)
                if text and text not in tags:
                    tags.append(text)
        info['tags'] = tags[:20]
        
        # 提取主要内容预览
        # 移除script和style标签
        for script in soup(['script', 'style', 'noscript']):
            script.decompose()
        
        # 获取body内容
        body = soup.find('body')
        if body:
            text = body.get_text(separator=' ', strip=True)
            info['content_preview'] = text[:800] if len(text) > 800 else text
            info['main_content'] = text[:1500] if len(text) > 1500 else text
        
        # 分析页面结构
        structure_info = []
        if soup.find('article'):
            structure_info.append("包含<article>标签")
        if soup.find('main'):
            structure_info.append("包含<main>标签")
        if soup.find_all('article'):
            structure_info.append(f"包含{len(soup.find_all('article'))}个文章元素")
        if soup.find_all('time', datetime=True):
            structure_info.append(f"包含{len(soup.find_all('time', datetime=True))}个时间戳")
        if soup.find_all('div', class_=re.compile('post|article|entry', re.I)):
            structure_info.append("包含文章相关的div元素")
        info['page_structure'] = ', '.join(structure_info) if structure_info else "未检测到明显的文章结构"
        
        # 检查博客特征关键词（更全面的关键词列表）
        blog_keywords = [
            '博客', 'blog', '文章', 'post', 'archive', '归档', 'archives',
            '分类', 'category', 'categories', '标签', 'tag', 'tags', '标签云',
            '评论', 'comment', 'comments', '留言', '留言板',
            'rss', 'feed', 'atom', '订阅', 'subscribe', 'feeds',
            '文章列表', 'post list', '最新文章', 'recent posts',
            '发布时间', 'publish', 'author', '作者', '发表日期',
            '上一篇', '下一篇', 'previous', 'next', 'related posts'
        ]
        page_text = (info['title'] + ' ' + info['description'] + ' ' + info['content_preview']).lower()
        info['has_blog_indicators'] = any(keyword in page_text for keyword in blog_keywords)
        
        return info
    except Exception as e:
        return {}

def judge_blog_by_features(key_info):
    """基于特征判断是否为博客网站（不使用AI）"""
    score = 0  # 博客特征得分
    max_score = 0  # 最大可能得分
    
    # 1. 文章链接数量（权重：30%）
    post_count = key_info.get('post_count', 0)
    max_score += 30
    if post_count >= 5:
        score += 30  # 5个以上文章链接，强烈暗示是博客
    elif post_count >= 3:
        score += 20  # 3-4个文章链接
    elif post_count >= 1:
        score += 10  # 1-2个文章链接
    
    # 2. RSS/Feed链接（权重：20%）
    rss_feeds = key_info.get('rss_feeds', [])
    max_score += 20
    if len(rss_feeds) > 0:
        score += 20  # 有RSS/Feed链接，强烈暗示是博客
    
    # 3. 分类和标签（权重：20%）
    categories = key_info.get('categories', [])
    tags = key_info.get('tags', [])
    max_score += 20
    if len(categories) > 0 or len(tags) > 0:
        score += 20  # 有分类或标签，强烈暗示是博客
    
    # 4. 页面结构特征（权重：15%）
    page_structure = key_info.get('page_structure', '')
    max_score += 15
    if 'article' in page_structure.lower() or '文章' in page_structure:
        score += 15  # 包含article标签或文章结构
    elif '时间戳' in page_structure or 'time' in page_structure.lower():
        score += 10  # 有时间戳
    
    # 5. 导航链接中的博客关键词（权重：10%）
    navigation_links = key_info.get('navigation_links', [])
    max_score += 10
    nav_text = ' '.join(navigation_links).lower()
    blog_nav_keywords = ['blog', '博客', '文章', 'post', 'archive', '归档', '分类', 'category', '标签', 'tag']
    if any(keyword in nav_text for keyword in blog_nav_keywords):
        score += 10
    
    # 6. 内容中的博客关键词（权重：5%）
    has_blog_indicators = key_info.get('has_blog_indicators', False)
    max_score += 5
    if has_blog_indicators:
        score += 5
    
    # 计算得分比例
    if max_score == 0:
        return False
    
    score_ratio = score / max_score
    
    # 判断阈值：得分比例 >= 0.4 认为是博客
    # 这意味着至少需要满足以下条件之一：
    # - 有3个以上文章链接
    # - 有RSS/Feed链接
    # - 有分类或标签
    # - 或者多个弱特征组合
    is_blog = score_ratio >= 0.4
    
    return is_blog

def extract_site_name(html, url):
    """从HTML中提取网站名称"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 方法1: 从title标签提取
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # 移除常见的后缀
            title = re.sub(r'\s*[-|]\s*(博客|Blog|首页|Home).*$', '', title, flags=re.I)
            if title:
                return title[:255]  # 限制长度
        
        # 方法2: 从域名提取
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.split('.')[0].capitalize()
    except:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.split('.')[0].capitalize()

def process_external_site(external_site):
    """处理单个外部网站"""
    external_id = external_site['id']
    url = external_site['url']
    domain = external_site.get('domain', '')
    discovered_from_site_id = external_site['discovered_from_site_id']
    discovered_from_page = external_site['discovered_from_page']
    link_type = external_site['link_type']
    
    try:
        # 1. 爬取首页
        safe_print(f"  处理: {url}")
        html, final_url = fetch_page(url)
        if not html:
            safe_print(f"    ✗ 无法访问: {url}")
            mark_external_site_processed(external_id, status=2)  # 标记为处理失败
            return False, False, False
        
        # 2. 提取关键信息
        key_info = extract_key_info(html, final_url or url)
        
        # 3. 使用基础特征判断是否为博客（不使用AI）
        is_blog = judge_blog_by_features(key_info)
        
        # 显示判断详情
        post_count = key_info.get('post_count', 0)
        rss_count = len(key_info.get('rss_feeds', []))
        categories_count = len(key_info.get('categories', []))
        tags_count = len(key_info.get('tags', []))
        safe_print(f"    特征判断: {'是博客' if is_blog else '不是博客'} "
                  f"(文章:{post_count}, RSS:{rss_count}, 分类:{categories_count}, 标签:{tags_count})")
        
        if not is_blog:
            # 不是博客，标记为处理失败（保留在external_sites表中）
            mark_external_site_processed(external_id, status=2)
            return False, False, False
        
        # 4. 提取网站名称
        site_name = extract_site_name(html, final_url or url)
        
        # 5. 添加到sites表并创建友情链接关系
        # 使用数据库锁和事务保证一致性
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            # 开始事务
            connection.begin()
            
            with connection.cursor() as cursor:
                # 检查sites表中是否已存在该URL
                cursor.execute(f"SELECT id FROM {SITES_TABLE} WHERE url = %s FOR UPDATE", (final_url or url,))
                existing_site = cursor.fetchone()
                
                if existing_site:
                    # 如果已存在，使用现有的site_id
                    site_id = existing_site['id']
                    safe_print(f"    网站已存在于sites表，ID: {site_id}")
                    added_to_sites = False
                else:
                    # 插入新站点
                    try:
                        cursor.execute(
                            f"INSERT INTO {SITES_TABLE} (name, url) VALUES (%s, %s)",
                            (site_name, final_url or url)
                        )
                        site_id = cursor.lastrowid
                        safe_print(f"    ✓ 已添加到sites表，ID: {site_id}")
                        added_to_sites = True
                    except pymysql.err.IntegrityError:
                        # 如果插入时发生唯一约束冲突，可能是并发插入，重新查询
                        cursor.execute(f"SELECT id FROM {SITES_TABLE} WHERE url = %s", (final_url or url,))
                        existing_site = cursor.fetchone()
                        if existing_site:
                            site_id = existing_site['id']
                            safe_print(f"    网站已存在于sites表（并发插入），ID: {site_id}")
                            added_to_sites = False
                        else:
                            raise
                
                # 检查friend_links表中是否已存在该友情链接
                cursor.execute(
                    f"""SELECT id FROM {FRIEND_LINKS_TABLE} 
                        WHERE from_site_id = %s AND to_site_id = %s AND page_url = %s""",
                    (discovered_from_site_id, site_id, discovered_from_page)
                )
                existing_link = cursor.fetchone()
                
                if not existing_link:
                    # 插入友情链接关系
                    try:
                        cursor.execute(
                            f"""INSERT INTO {FRIEND_LINKS_TABLE} 
                                (from_site_id, to_site_id, link_type, page_url) 
                                VALUES (%s, %s, %s, %s)""",
                            (discovered_from_site_id, site_id, link_type, discovered_from_page)
                        )
                        safe_print(f"    ✓ 已创建友情链接关系")
                        created_link = True
                    except pymysql.err.IntegrityError:
                        # 如果插入时发生唯一约束冲突，说明已存在
                        safe_print(f"    友情链接关系已存在（并发插入）")
                        created_link = False
                else:
                    safe_print(f"    友情链接关系已存在")
                    created_link = False
                
                # 如果是博客，从external_sites表删除记录
                cursor.execute(
                    f"DELETE FROM {EXTERNAL_SITES_TABLE} WHERE id = %s",
                    (external_id,)
                )
                safe_print(f"    ✓ 已从external_sites表删除记录")
                
                # 提交事务
                connection.commit()
                return True, added_to_sites, created_link
                
        except pymysql.err.IntegrityError as e:
            # 处理唯一约束冲突（可能是并发插入）
            connection.rollback()
            safe_print(f"    警告: 数据冲突，可能已存在: {e}")
            # 即使有冲突，也从external_sites表删除（因为已经添加到sites表）
            try:
                conn = pymysql.connect(
                    **DB_CONFIG,
                    database=DB_NAME,
                    cursorclass=pymysql.cursors.DictCursor
                )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM {EXTERNAL_SITES_TABLE} WHERE id = %s",
                        (external_id,)
                    )
                conn.commit()
                conn.close()
            except:
                pass
            return False, False, False
        except Exception as e:
            connection.rollback()
            safe_print(f"    ✗ 数据库操作失败: {e}")
            # 标记为处理失败（保留在external_sites表中）
            try:
                conn = pymysql.connect(
                    **DB_CONFIG,
                    database=DB_NAME,
                    cursorclass=pymysql.cursors.DictCursor
                )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE {EXTERNAL_SITES_TABLE} SET processed = %s WHERE id = %s",
                        (2, external_id)
                    )
                conn.commit()
                conn.close()
            except:
                pass
            return False, False, False
        finally:
            connection.close()
        
    except Exception as e:
        safe_print(f"    ✗ 处理失败: {e}")
        mark_external_site_processed(external_id, status=2)
        return False, False, False

def mark_external_site_processed(external_id, status, connection=None):
    """标记external_site为已处理"""
    try:
        if connection:
            # 使用现有连接
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {EXTERNAL_SITES_TABLE} SET processed = %s WHERE id = %s",
                    (status, external_id)
                )
        else:
            # 创建新连接
            conn = pymysql.connect(
                **DB_CONFIG,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE {EXTERNAL_SITES_TABLE} SET processed = %s WHERE id = %s",
                        (status, external_id)
                    )
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        safe_print(f"    警告: 标记处理状态失败: {e}")

def get_unprocessed_external_sites(limit=100):
    """获取未处理的外部网站（使用原子更新避免并发冲突）"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            # 开始事务
            connection.begin()
            
            with connection.cursor() as cursor:
                # 使用原子更新方式获取未处理的记录
                # 先选择ID列表，然后原子更新为临时状态（3表示处理中）
                # 这样可以避免多个线程处理同一条记录
                
                # 获取未处理的记录ID（排除处理中的记录）
                cursor.execute(f"""
                    SELECT id 
                    FROM {EXTERNAL_SITES_TABLE} 
                    WHERE (processed = 0 OR processed IS NULL)
                    ORDER BY id
                    LIMIT %s
                    FOR UPDATE
                """, (limit,))
                ids = [row['id'] for row in cursor.fetchall()]
                
                if not ids:
                    connection.commit()
                    return []
                
                # 原子更新这些记录为处理中状态（使用临时值3）
                # 注意：这里使用临时值，处理完成后会更新为1或2
                placeholders = ','.join(['%s'] * len(ids))
                cursor.execute(f"""
                    UPDATE {EXTERNAL_SITES_TABLE} 
                    SET processed = 3 
                    WHERE id IN ({placeholders}) AND (processed = 0 OR processed IS NULL)
                """, ids)
                
                # 获取实际更新的记录（只返回成功更新的记录）
                cursor.execute(f"""
                    SELECT id, url, domain, discovered_from_site_id, discovered_from_page, link_type 
                    FROM {EXTERNAL_SITES_TABLE} 
                    WHERE id IN ({placeholders}) AND processed = 3
                """, ids)
                sites = cursor.fetchall()
            
            # 提交事务
            connection.commit()
            return sites
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            connection.close()
    except Exception as e:
        safe_print(f"✗ 获取未处理外部网站失败: {e}")
        return []

def main():
    """主函数"""
    print("=" * 60)
    print("外部网站博客识别程序")
    print("=" * 60)
    
    # 提示：当前使用基础特征判断，不使用AI
    print("\n提示: 当前使用基础特征判断模式（不使用AI）")
    print("判断标准：")
    print("  - 文章链接数量 >= 3")
    print("  - 有RSS/Feed链接")
    print("  - 有分类或标签功能")
    print("  - 页面包含article标签等博客结构特征")
    print("  - 满足以上多个条件时判断为博客")
    
    # 初始化数据库
    if not init_database():
        print("\n数据库初始化失败！")
        return
    
    # 询问每次处理的批量大小
    print("\n批量处理配置:")
    try:
        batch_size = int(input("请输入每次处理的批量大小 (默认100): ").strip() or "100")
        if batch_size < 1:
            batch_size = 100
    except:
        batch_size = 100
    
    # 询问线程数
    print("\n线程配置:")
    try:
        max_workers_input = input(f"请输入并发线程数 (默认{MAX_WORKERS}，建议1-5): ").strip()
        max_workers = int(max_workers_input) if max_workers_input else MAX_WORKERS
        if max_workers < 1:
            max_workers = 1
        if max_workers > 10:
            print("警告: 线程数过多可能导致API限流，已限制为10")
            max_workers = 10
    except:
        max_workers = MAX_WORKERS
    
    print(f"使用 {max_workers} 个并发线程")
    
    # 统计信息
    class Stats:
        def __init__(self):
            self.processed = 0
            self.success = 0
            self.fail = 0
            self.added_to_sites = 0
            self.created_links = 0
            self.lock = threading.Lock()
        
        def update(self, success, added_to_sites=False, created_link=False):
            with self.lock:
                self.processed += 1
                if success:
                    self.success += 1
                    if added_to_sites:
                        self.added_to_sites += 1
                    if created_link:
                        self.created_links += 1
                else:
                    self.fail += 1
        
        def get_stats(self):
            with self.lock:
                return {
                    'processed': self.processed,
                    'success': self.success,
                    'fail': self.fail,
                    'added_to_sites': self.added_to_sites,
                    'created_links': self.created_links
                }
    
    stats = Stats()
    start_time = time.time()
    
    # 主循环：持续处理未处理的外部网站
    print(f"\n开始处理外部网站...")
    print("=" * 60)
    
    pbar = tqdm(desc="处理进度", unit="网站", position=0, leave=True, ncols=100)
    
    try:
        while True:
            # 获取未处理的外部网站
            external_sites = get_unprocessed_external_sites(limit=batch_size)
            
            if not external_sites:
                print("\n所有外部网站已处理完成！")
                break
            
            print(f"\n获取到 {len(external_sites)} 个未处理的外部网站")
            
            # 使用线程池并发处理
            executor = ThreadPoolExecutor(max_workers=max_workers)
            future_to_site = {
                executor.submit(process_external_site, site): site 
                for site in external_sites
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_site):
                site = future_to_site[future]
                try:
                    success, added_to_sites, created_link = future.result()
                    stats.update(success, added_to_sites, created_link)
                    
                    # 更新进度条
                    current_stats = stats.get_stats()
                    pbar.set_postfix({
                        '成功': current_stats['success'],
                        '失败': current_stats['fail'],
                        '新增站点': current_stats['added_to_sites']
                    })
                    pbar.update(1)
                    
                    # 请求间隔
                    time.sleep(REQUEST_DELAY)
                except Exception as e:
                    stats.update(False)
                    pbar.update(1)
            
            executor.shutdown(wait=True)
            
            # 如果获取的数量少于批量大小，说明已经处理完了
            if len(external_sites) < batch_size:
                break
            
            # 批次间隔
            time.sleep(2)
    
    except KeyboardInterrupt:
        pbar.close()
        safe_print("\n\n用户中断，正在停止...")
    finally:
        pbar.close()
    
    # 统计信息
    final_stats = stats.get_stats()
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("处理完成！")
    print("=" * 60)
    print(f"处理网站数: {final_stats['processed']}")
    print(f"成功: {final_stats['success']}")
    print(f"失败: {final_stats['fail']}")
    print(f"新增到sites表: {final_stats['added_to_sites']}")
    print(f"创建友情链接: {final_stats['created_links']}")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    if final_stats['processed'] > 0:
        print(f"平均速度: {final_stats['processed']/elapsed_time:.2f} 网站/秒")
    print("=" * 60)

if __name__ == '__main__':
    main()


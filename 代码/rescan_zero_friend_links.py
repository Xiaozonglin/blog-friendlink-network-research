#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
深度筛查友链数量为0的博客网站
使用Selenium处理JavaScript渲染，适配客户端渲染的网站
"""

import pymysql
import time
import os
import urllib3
from urllib.parse import urlparse, urljoin
import re
from bs4 import BeautifulSoup

# 尝试导入Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("警告: 未安装Selenium，请运行: pip install selenium")

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 尝试从配置文件导入数据库配置
try:
    from config import DB_CONFIG as CONFIG_DB_CONFIG
    DB_CONFIG = CONFIG_DB_CONFIG.copy()
except ImportError:
    DB_CONFIG = {}

# 数据库配置
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
FRIEND_LINK_URIS = [
    'friend', 'friend.html', 'friends', 'friends.html',
    'link', 'link.html', 'links', 'links.html',
    'friendship', 'friendship.html', '友情链接',
    'about/friends', 'page/friends', 'page/friends.html',
    'friendlink', 'friend-link',
]

# Selenium配置
SELENIUM_TIMEOUT = 30  # 页面加载超时时间（秒）
PAGE_LOAD_DELAY = 2  # 页面加载后的等待时间（秒）
SCROLL_PAUSE_TIME = 1  # 滚动间隔时间

def init_selenium_driver(headless=True):
    """初始化Selenium WebDriver"""
    if not SELENIUM_AVAILABLE:
        return None
    
    try:
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')  # 无头模式
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # 设置User-Agent，模拟真实浏览器
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 禁用图片加载以提高速度（可选）
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # chrome_options.add_experimental_option("prefs", prefs)
        
        # 初始化WebDriver
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(SELENIUM_TIMEOUT)
        driver.implicitly_wait(5)
        
        return driver
    except Exception as e:
        print(f"初始化Selenium失败: {e}")
        return None

def scroll_page(driver, max_scrolls=3):
    """滚动页面以触发懒加载内容"""
    try:
        # 获取页面初始高度
        last_height = driver.execute_script("return document.body.scrollHeight")
        scrolls = 0
        
        while scrolls < max_scrolls:
            # 滚动到页面底部
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            
            # 计算新的页面高度
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            # 如果页面高度没有变化，停止滚动
            if new_height == last_height:
                break
            
            last_height = new_height
            scrolls += 1
        
        # 滚动回顶部
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
    except Exception as e:
        print(f"  滚动页面失败: {e}")

def fetch_page_with_selenium(url, driver):
    """使用Selenium获取页面内容（处理JavaScript渲染）"""
    try:
        driver.get(url)
        
        # 等待页面加载
        time.sleep(PAGE_LOAD_DELAY)
        
        # 滚动页面以触发懒加载
        scroll_page(driver)
        
        # 获取页面HTML
        html = driver.page_source
        final_url = driver.current_url
        
        return html, final_url
    except TimeoutException:
        print(f"    页面加载超时: {url}")
        return None, None
    except WebDriverException as e:
        print(f"    Selenium错误: {e}")
        return None, None
    except Exception as e:
        print(f"    获取页面失败: {e}")
        return None, None

def normalize_url(url):
    """规范化URL"""
    if not url:
        return None
    
    url = url.strip()
    
    # 跳过非HTTP(S)链接
    if url.startswith(('mailto:', 'tel:', 'javascript:', '#')):
        return None
    
    # 如果没有协议，返回None
    if not url.startswith(('http://', 'https://')):
        return None
    
    # 移除末尾的斜杠
    url = url.rstrip('/')
    if url.endswith('://'):
        return None
    
    # 移除fragment
    parsed = urlparse(url)
    url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}{('?' + parsed.query) if parsed.query else ''}"
    
    return url

def extract_domain(url):
    """提取URL的域名（去除www前缀）"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

def is_same_domain(url1, url2):
    """判断两个URL是否属于同一个域名"""
    try:
        domain1 = extract_domain(url1)
        domain2 = extract_domain(url2)
        return domain1 == domain2
    except:
        return False

def extract_links_from_html(html, base_url):
    """从HTML中提取链接"""
    if not html:
        return set()
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        base_domain = extract_domain(base_url)
        
        for tag in soup.find_all('a', href=True):
            href = tag.get('href', '').strip()
            if not href:
                continue
            
            # 转换为绝对URL
            if not href.startswith(('http://', 'https://')):
                href = urljoin(base_url, href)
            
            normalized = normalize_url(href)
            if not normalized:
                continue
            
            # 跳过同域名的链接
            link_domain = extract_domain(normalized)
            if link_domain == base_domain:
                continue
            
            # 跳过常见的不相关链接
            skip_domains = [
                'github.com', 'twitter.com', 'facebook.com', 'linkedin.com',
                'weibo.com', 'zhihu.com', 'douban.com', 'bilibili.com',
                'youtube.com', 'instagram.com', 'pinterest.com',
            ]
            
            if any(skip in normalized.lower() for skip in skip_domains):
                continue
            
            links.add(normalized)
        
        return links
    except Exception as e:
        print(f"    提取链接失败: {e}")
        return set()

def find_friend_link_pages_selenium(driver, base_url):
    """使用Selenium查找友情链接页面"""
    friend_page_urls = []
    
    try:
        # 获取首页HTML
        html, final_url = fetch_page_with_selenium(base_url, driver)
        if not html:
            return friend_page_urls
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # 查找包含"友链"、"友情链接"等关键词的链接
        keywords = ['友链', '友情链接', 'friends', 'friend', 'link', 'links', 'blogroll', '友情', '链接']
        for tag in soup.find_all('a', href=True):
            text = tag.get_text(strip=True).lower()
            href = tag.get('href', '').strip()
            
            if any(keyword in text for keyword in keywords) or \
               any(keyword in href.lower() for keyword in ['friend', 'link', '友链']):
                if not href.startswith(('http://', 'https://')):
                    href = urljoin(base_url, href)
                
                normalized = normalize_url(href)
                if normalized and is_same_domain(normalized, base_url):
                    friend_page_urls.append(normalized)
        
        # 添加预存的URI
        for uri in FRIEND_LINK_URIS:
            if uri.startswith('/'):
                test_url = urljoin(base_url, uri)
            else:
                test_url = urljoin(base_url, '/' + uri)
            if test_url not in friend_page_urls:
                friend_page_urls.append(test_url)
        
        return list(set(friend_page_urls))
    except Exception as e:
        print(f"    查找友链页面失败: {e}")
        return friend_page_urls

def get_sites_with_zero_friend_links():
    """获取友链数量为0的博客站点"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # 查询友链数量为0的站点
            cursor.execute(f"""
                SELECT s.id, s.name, s.url
                FROM {SITES_TABLE} s
                LEFT JOIN (
                    SELECT from_site_id, COUNT(*) as link_count
                    FROM {FRIEND_LINKS_TABLE}
                    GROUP BY from_site_id
                ) fl ON s.id = fl.from_site_id
                WHERE fl.link_count IS NULL OR fl.link_count = 0
                ORDER BY s.id
            """)
            sites = cursor.fetchall()
        
        connection.close()
        return sites
    except Exception as e:
        print(f"✗ 获取站点列表失败: {e}")
        return []

def get_all_sites():
    """获取所有站点（用于URL映射）"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, url FROM {SITES_TABLE} ORDER BY id")
            sites = cursor.fetchall()
        
        connection.close()
        return sites
    except Exception as e:
        return []

def build_site_url_map(sites):
    """构建站点URL到ID的映射"""
    url_map = {}
    base_url_map = {}
    
    for site in sites:
        site_id = site['id']
        site_url = site['url']
        
        url_map[site_url] = site_id
        
        try:
            parsed = urlparse(site_url)
            netloc = parsed.netloc.lower().replace('www.', '')
            base_url = f"{parsed.scheme}://{netloc}"
            if base_url not in base_url_map:
                base_url_map[base_url] = site_id
        except:
            pass
    
    return url_map, base_url_map

def get_site_by_url(url, url_map, base_url_map):
    """根据URL查找站点ID"""
    if not url:
        return None
    
    if url in url_map:
        return url_map[url]
    
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower().replace('www.', '')
        base_url = f"{parsed.scheme}://{netloc}"
        if base_url in base_url_map:
            return base_url_map[base_url]
    except:
        pass
    
    return None

def save_friend_links(friend_links_data):
    """保存友链关系到数据库"""
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
        print(f"    保存友链失败: {e}")
        return 0

def rescan_site(site, driver, url_map, base_url_map):
    """重新扫描单个站点"""
    site_id = site['id']
    site_name = site['name']
    site_url = site['url']
    
    print(f"\n处理: {site_name} ({site_url})")
    
    try:
        friend_links_data = []
        
        # 1. 使用Selenium获取首页
        print(f"  获取首页...")
        homepage_html, final_url = fetch_page_with_selenium(site_url, driver)
        if not homepage_html:
            print(f"    ✗ 无法访问首页")
            return 0
        
        # 2. 从首页提取链接
        homepage_links = extract_links_from_html(homepage_html, final_url or site_url)
        print(f"    从首页提取到 {len(homepage_links)} 个外部链接")
        
        # 3. 处理首页链接
        print(f"  检查首页链接...")
        for link_url in homepage_links:
            to_site_id = get_site_by_url(link_url, url_map, base_url_map)
            if to_site_id:
                friend_links_data.append((site_id, to_site_id, 'homepage', final_url or site_url))
        
        # 4. 查找并访问友情链接页面
        print(f"  查找友情链接页面...")
        friend_page_urls = find_friend_link_pages_selenium(driver, final_url or site_url)
        print(f"    找到 {len(friend_page_urls)} 个可能的友链页面")
        
        processed_friend_page_links = set()  # 记录已处理的链接，避免重复
        for friend_page_url in friend_page_urls[:5]:  # 最多尝试5个页面
            print(f"    访问: {friend_page_url}")
            time.sleep(1)  # 避免请求过快
            
            friend_page_html, final_friend_url = fetch_page_with_selenium(friend_page_url, driver)
            if friend_page_html:
                links = extract_links_from_html(friend_page_html, final_friend_url or friend_page_url)
                print(f"      提取到 {len(links)} 个链接")
                
                # 处理友链页面中的链接
                for link_url in links:
                    # 跳过已经在首页处理过的链接
                    if link_url in homepage_links:
                        continue
                    
                    # 检查是否是博客站点
                    to_site_id = get_site_by_url(link_url, url_map, base_url_map)
                    if to_site_id:
                        # 避免重复添加相同的友链关系
                        link_key = (site_id, to_site_id, final_friend_url or friend_page_url)
                        if link_key not in processed_friend_page_links:
                            friend_links_data.append((site_id, to_site_id, 'friend_page', final_friend_url or friend_page_url))
                            processed_friend_page_links.add(link_key)
                
                # 如果找到的链接数量较多，认为这是有效的友链页面
                if len(links) > 3:
                    break
        
        # 6. 保存友链关系
        if friend_links_data:
            saved_count = save_friend_links(friend_links_data)
            print(f"  ✓ 保存了 {saved_count} 个友链关系")
            return saved_count
        else:
            print(f"  - 未找到友链")
            return 0
        
    except Exception as e:
        print(f"    ✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    """主函数"""
    print("=" * 60)
    print("深度筛查友链数量为0的博客网站")
    print("=" * 60)
    
    # 检查Selenium
    if not SELENIUM_AVAILABLE:
        print("\n错误: 未安装Selenium")
        print("请运行: pip install selenium")
        print("并确保已安装Chrome浏览器和ChromeDriver")
        return
    
    # 初始化Selenium WebDriver
    print("\n初始化Selenium WebDriver...")
    driver = init_selenium_driver(headless=True)
    if not driver:
        print("✗ 无法初始化Selenium WebDriver")
        return
    
    print("✓ Selenium WebDriver初始化成功")
    
    try:
        # 获取友链数量为0的站点
        print("\n获取友链数量为0的站点...")
        sites = get_sites_with_zero_friend_links()
        if not sites:
            print("没有找到友链数量为0的站点")
            return
        
        print(f"找到 {len(sites)} 个友链数量为0的站点")
        
        # 获取所有站点用于URL映射
        print("\n构建站点URL映射...")
        all_sites = get_all_sites()
        url_map, base_url_map = build_site_url_map(all_sites)
        print(f"已构建 {len(url_map)} 个精确URL映射，{len(base_url_map)} 个基础URL映射")
        
        # 询问处理数量
        print(f"\n选项:")
        print(f"1. 处理所有 {len(sites)} 个站点")
        print(f"2. 处理前N个站点")
        
        try:
            choice = input("请选择 (1/2，默认1): ").strip() or "1"
        except:
            choice = "1"
        
        if choice == "2":
            try:
                limit = int(input(f"请输入要处理的站点数量 (1-{len(sites)}): ").strip())
                sites = sites[:limit]
            except:
                pass
        
        print(f"\n将处理 {len(sites)} 个站点")
        
        # 统计信息
        total_found = 0
        processed = 0
        
        # 处理每个站点
        for i, site in enumerate(sites, 1):
            print(f"\n[{i}/{len(sites)}] ", end="")
            found = rescan_site(site, driver, url_map, base_url_map)
            total_found += found
            processed += 1
            
            # 每处理10个站点，更新一次URL映射
            if i % 10 == 0:
                print(f"\n更新站点URL映射...")
                all_sites = get_all_sites()
                url_map, base_url_map = build_site_url_map(all_sites)
                print(f"当前站点URL映射: {len(url_map)} 个精确URL，{len(base_url_map)} 个基础URL")
            
            # 请求间隔
            time.sleep(2)
        
        # 统计信息
        print("\n" + "=" * 60)
        print("处理完成！")
        print("=" * 60)
        print(f"处理站点数: {processed}")
        print(f"新发现的友链关系: {total_found}")
        print("=" * 60)
    
    finally:
        # 关闭WebDriver
        print("\n关闭Selenium WebDriver...")
        try:
            driver.quit()
        except:
            pass
        print("✓ 完成")

if __name__ == '__main__':
    main()


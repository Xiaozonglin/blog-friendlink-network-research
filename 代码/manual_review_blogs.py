#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
人工随机抽检博客网站
随机选择博客网站进行人工审核，可以标记为博客或非博客
"""

import pymysql
import random
import os
import time
from urllib.parse import urlparse

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

def get_all_sites():
    """获取所有博客站点"""
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

def get_reviewed_sites():
    """获取已审核的站点ID集合"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # 检查是否有reviewed字段
            cursor.execute(f"SHOW COLUMNS FROM {SITES_TABLE} LIKE 'reviewed'")
            has_reviewed = cursor.fetchone() is not None
            
            if has_reviewed:
                cursor.execute(f"SELECT id FROM {SITES_TABLE} WHERE reviewed = 1")
                reviewed_ids = {row['id'] for row in cursor.fetchall()}
            else:
                reviewed_ids = set()
        
        connection.close()
        return reviewed_ids, has_reviewed
    except Exception as e:
        return set(), False

def init_reviewed_field():
    """初始化reviewed字段"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {SITES_TABLE} LIKE 'reviewed'")
            has_reviewed = cursor.fetchone() is not None
            
            if not has_reviewed:
                cursor.execute(f"""
                    ALTER TABLE {SITES_TABLE} 
                    ADD COLUMN reviewed TINYINT(1) DEFAULT 0 COMMENT '是否已审核: 0-未审核, 1-已审核' AFTER url,
                    ADD INDEX idx_reviewed (reviewed)
                """)
                print("  ✓ 已添加reviewed字段到sites表")
            
        connection.commit()
        connection.close()
        return True
    except Exception as e:
        print(f"✗ 初始化reviewed字段失败: {e}")
        return False

def mark_site_reviewed(site_id, is_blog):
    """标记站点为已审核"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                UPDATE {SITES_TABLE} 
                SET reviewed = 1 
                WHERE id = %s
            """, (site_id,))
        
        connection.commit()
        connection.close()
        return True
    except Exception as e:
        print(f"  ✗ 标记审核状态失败: {e}")
        return False

def delete_site_and_related_data(site_id):
    """删除站点及其相关的友链、外链数据"""
    try:
        connection = pymysql.connect(
            **DB_CONFIG,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # 由于有外键约束，删除sites表中的记录会自动删除相关的友链和外链数据
            # 但我们需要先统计一下
            
            # 统计将被删除的友链关系
            cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {FRIEND_LINKS_TABLE} 
                WHERE from_site_id = %s OR to_site_id = %s
            """, (site_id, site_id))
            friend_links_count = cursor.fetchone()['count']
            
            # 统计将被删除的外部网站记录
            cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {EXTERNAL_SITES_TABLE} 
                WHERE discovered_from_site_id = %s
            """, (site_id,))
            external_sites_count = cursor.fetchone()['count']
            
            # 获取站点信息
            cursor.execute(f"SELECT id, name, url FROM {SITES_TABLE} WHERE id = %s", (site_id,))
            site = cursor.fetchone()
            
            if site:
                # 删除站点（外键约束会自动删除相关数据）
                cursor.execute(f"DELETE FROM {SITES_TABLE} WHERE id = %s", (site_id,))
                
                print(f"  ✓ 已删除站点: {site['name']} ({site['url']})")
                print(f"  ✓ 已删除 {friend_links_count} 个友链关系")
                print(f"  ✓ 已删除 {external_sites_count} 个外部网站记录")
            else:
                print(f"  ✗ 站点不存在")
                connection.close()
                return False
        
        connection.commit()
        connection.close()
        return True
    except Exception as e:
        print(f"  ✗ 删除站点失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_random_site(sites, reviewed_ids):
    """获取一个随机未审核的站点"""
    unreviewed_sites = [site for site in sites if site['id'] not in reviewed_ids]
    if not unreviewed_sites:
        return None
    return random.choice(unreviewed_sites)

def open_url_in_browser(url):
    """在浏览器中打开URL（可选功能）"""
    import webbrowser
    try:
        webbrowser.open(url)
        return True
    except:
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("人工随机抽检博客网站")
    print("=" * 60)
    
    # 初始化reviewed字段
    if not init_reviewed_field():
        print("\n数据库初始化失败！")
        return
    
    # 获取所有站点
    print("\n获取站点列表...")
    sites = get_all_sites()
    if not sites:
        print("没有找到任何站点！")
        return
    
    print(f"找到 {len(sites)} 个站点")
    
    # 获取已审核的站点
    reviewed_ids, has_reviewed = get_reviewed_sites()
    print(f"已审核的站点: {len(reviewed_ids)} 个")
    print(f"未审核的站点: {len(sites) - len(reviewed_ids)} 个")
    
    if len(reviewed_ids) >= len(sites):
        print("\n所有站点都已审核完成！")
        return
    
    # 统计信息
    reviewed_count = len(reviewed_ids)
    is_blog_count = 0
    not_blog_count = 0
    
    # 询问是否在浏览器中打开链接
    print("\n选项:")
    try:
        open_browser = input("是否在浏览器中打开链接？(y/n，默认n): ").strip().lower() == 'y'
    except:
        open_browser = False
    
    print("\n开始随机抽检...")
    print("=" * 60)
    print("操作说明:")
    print("  - 输入 0: 是博客，标记为已审核，继续下一个")
    print("  - 输入 1: 不是博客，删除该网站及其相关的友链、外链数据")
    print("  - 输入 q: 退出程序")
    print("=" * 60)
    
    try:
        while True:
            # 获取随机未审核的站点
            site = get_random_site(sites, reviewed_ids)
            if not site:
                print("\n所有站点都已审核完成！")
                break
            
            # 显示站点信息
            print(f"\n{'='*60}")
            print(f"已抽检数量: {reviewed_count}")
            print(f"是博客: {is_blog_count}")
            print(f"不是博客: {not_blog_count}")
            print(f"{'='*60}")
            print(f"站点ID: {site['id']}")
            print(f"站点名称: {site['name']}")
            print(f"站点URL: {site['url']}")
            print(f"{'='*60}")
            
            # 在浏览器中打开（如果用户选择）
            if open_browser:
                print(f"\n正在浏览器中打开: {site['url']}")
                open_url_in_browser(site['url'])
                time.sleep(1)  # 等待浏览器打开
            
            # 获取用户输入
            while True:
                try:
                    user_input = input("\n请输入判断结果 (0=是博客, 1=不是博客, q=退出): ").strip().lower()
                    
                    if user_input == 'q':
                        print("\n退出程序")
                        return
                    elif user_input == '0':
                        # 是博客，标记为已审核
                        if mark_site_reviewed(site['id'], True):
                            reviewed_ids.add(site['id'])
                            reviewed_count += 1
                            is_blog_count += 1
                            print(f"  ✓ 已标记为博客（已审核）")
                        break
                    elif user_input == '1':
                        # 不是博客，删除站点及相关数据
                        print(f"\n确认删除站点 '{site['name']}' 及其相关的所有数据？")
                        confirm = input("输入 'yes' 确认删除，其他键取消: ").strip().lower()
                        
                        if confirm == 'yes':
                            if delete_site_and_related_data(site['id']):
                                # 从列表中移除
                                sites = [s for s in sites if s['id'] != site['id']]
                                reviewed_ids.add(site['id'])  # 标记为已处理
                                reviewed_count += 1
                                not_blog_count += 1
                                print(f"  ✓ 已删除")
                        else:
                            print(f"  ✗ 已取消删除")
                        break
                    else:
                        print("  无效输入，请输入 0、1 或 q")
                except KeyboardInterrupt:
                    print("\n\n用户中断")
                    return
                except EOFError:
                    print("\n\n退出程序")
                    return
    
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
    
    # 最终统计
    print("\n" + "=" * 60)
    print("抽检完成！")
    print("=" * 60)
    print(f"本次抽检数量: {reviewed_count}")
    print(f"是博客: {is_blog_count}")
    print(f"不是博客: {not_blog_count}")
    print(f"剩余未审核: {len(sites) - len(reviewed_ids)}")
    print("=" * 60)

if __name__ == '__main__':
    main()


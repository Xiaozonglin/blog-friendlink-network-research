#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
博客网络最短距离分析及可视化
从数据库中读取博客友链关系，计算博客之间的最短距离，并生成柱状图
"""

import pymysql
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict, deque
import os
import seaborn as sns
from datetime import datetime

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

class BlogNetworkAnalyzer:
    def __init__(self):
        self.graph = defaultdict(list)
        self.site_id_to_url = {}
        self.site_url_to_id = {}
        self.site_id_to_name = {}
        
    def load_data_from_db(self):
        """从数据库加载博客和友链数据"""
        try:
            connection = pymysql.connect(
                **DB_CONFIG,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor
            )
            
            with connection.cursor() as cursor:
                # 加载所有博客站点
                cursor.execute(f"SELECT id, name, url FROM {SITES_TABLE}")
                sites = cursor.fetchall()
                
                for site in sites:
                    site_id = site['id']
                    self.site_id_to_url[site_id] = site['url']
                    self.site_url_to_id[site['url']] = site_id
                    self.site_id_to_name[site_id] = site['name']
                
                # 加载友链关系
                cursor.execute(f"""
                    SELECT DISTINCT from_site_id, to_site_id 
                    FROM {FRIEND_LINKS_TABLE}
                    WHERE from_site_id != to_site_id
                """)
                links = cursor.fetchall()
                
                for link in links:
                    from_id = link['from_site_id']
                    to_id = link['to_site_id']
                    self.graph[from_id].append(to_id)
                    # 对于无向图，也添加反向链接（可选，根据你的需求）
                    # self.graph[to_id].append(from_id)
            
            connection.close()
            print(f"✓ 加载了 {len(sites)} 个博客站点和 {len(links)} 条友链关系")
            return True
            
        except Exception as e:
            print(f"✗ 从数据库加载数据失败: {e}")
            return False
    
    def bfs_shortest_paths(self, start_id):
        """使用BFS计算从起始博客到所有其他博客的最短距离"""
        distances = {}
        visited = set()
        queue = deque()
        
        distances[start_id] = 0
        visited.add(start_id)
        queue.append(start_id)
        
        while queue:
            current_id = queue.popleft()
            current_distance = distances[current_id]
            
            for neighbor_id in self.graph[current_id]:
                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    distances[neighbor_id] = current_distance + 1
                    queue.append(neighbor_id)
        
        return distances
    
    def calculate_all_pairs_shortest_paths(self, sample_size=None):
        """计算所有博客对之间的最短路径（抽样或全量）"""
        all_site_ids = list(self.site_id_to_url.keys())
        
        if sample_size and sample_size < len(all_site_ids):
            # 随机抽样一部分博客进行计算
            np.random.seed(42)  # 固定随机种子以便结果可重现
            sampled_ids = np.random.choice(all_site_ids, size=sample_size, replace=False)
            print(f"使用抽样方法: 从 {len(all_site_ids)} 个博客中抽取 {sample_size} 个")
        else:
            sampled_ids = all_site_ids
            print(f"使用全量计算: {len(all_site_ids)} 个博客")
        
        all_distances = []
        reachable_pairs = 0
        total_pairs = 0
        
        print("正在计算最短路径...")
        for i, start_id in enumerate(sampled_ids):
            if (i + 1) % 100 == 0:
                print(f"  进度: {i+1}/{len(sampled_ids)}")
            
            distances = self.bfs_shortest_paths(start_id)
            
            for target_id, distance in distances.items():
                if start_id != target_id:  # 排除自己到自己的距离
                    all_distances.append(distance)
                    reachable_pairs += 1
            
            total_pairs += (len(sampled_ids) - 1)  # 减去自己
        
        # 计算连通性统计
        connectivity_ratio = reachable_pairs / total_pairs if total_pairs > 0 else 0
        
        print(f"连通性统计:")
        print(f"  - 总博客对数量: {total_pairs}")
        print(f"  - 可达博客对数量: {reachable_pairs}")
        print(f"  - 连通比例: {connectivity_ratio:.4f} ({connectivity_ratio*100:.2f}%)")
        
        return all_distances, connectivity_ratio
    
    def analyze_network_properties(self):
        """分析网络的基本属性"""
        print("\n正在分析网络属性...")
        
        # 计算度分布
        in_degrees = defaultdict(int)
        out_degrees = defaultdict(int)
        
        for from_id, to_ids in self.graph.items():
            out_degrees[from_id] = len(to_ids)
            for to_id in to_ids:
                in_degrees[to_id] += 1
        
        # 统计入度和出度
        in_degree_values = list(in_degrees.values())
        out_degree_values = list(out_degrees.values())
        
        print(f"网络属性:")
        print(f"  - 节点数量: {len(self.graph)}")
        print(f"  - 边数量: {sum(out_degree_values)}")
        print(f"  - 平均入度: {np.mean(in_degree_values):.2f}")
        print(f"  - 平均出度: {np.mean(out_degree_values):.2f}")
        print(f"  - 最大入度: {max(in_degree_values) if in_degree_values else 0}")
        print(f"  - 最大出度: {max(out_degree_values) if out_degree_values else 0}")
        
        # 计算连通分量
        components = self.find_connected_components()
        print(f"  - 连通分量数量: {len(components)}")
        print(f"  - 最大连通分量大小: {max(len(comp) for comp in components) if components else 0}")
        
        return {
            'node_count': len(self.graph),
            'edge_count': sum(out_degree_values),
            'avg_in_degree': np.mean(in_degree_values),
            'avg_out_degree': np.mean(out_degree_values),
            'max_in_degree': max(in_degree_values) if in_degree_values else 0,
            'max_out_degree': max(out_degree_values) if out_degree_values else 0,
            'component_count': len(components),
            'largest_component': max(len(comp) for comp in components) if components else 0
        }
    
    def find_connected_components(self):
        """找到网络中的所有连通分量（无向图）"""
        visited = set()
        components = []
        
        # 创建无向图
        undirected_graph = defaultdict(list)
        for from_id, to_ids in self.graph.items():
            for to_id in to_ids:
                undirected_graph[from_id].append(to_id)
                undirected_graph[to_id].append(from_id)
        
        def bfs_component(start_id):
            component = set()
            queue = deque([start_id])
            visited.add(start_id)
            
            while queue:
                current_id = queue.popleft()
                component.add(current_id)
                
                for neighbor_id in undirected_graph[current_id]:
                    if neighbor_id not in visited:
                        visited.add(neighbor_id)
                        queue.append(neighbor_id)
            
            return component
        
        for node_id in undirected_graph.keys():
            if node_id not in visited:
                component = bfs_component(node_id)
                components.append(component)
        
        return components
    
    def create_distance_histogram(self, distances, output_dir='output'):
        """创建最短距离分布的柱状图"""
        if not distances:
            print("没有距离数据可绘制")
            return
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 计算统计信息
        unique_distances, counts = np.unique(distances, return_counts=True)
        total_pairs = len(distances)
        
        print(f"\n距离分布统计:")
        for dist, count in zip(unique_distances, counts):
            percentage = (count / total_pairs) * 100
            print(f"  距离 {dist}: {count} 对 ({percentage:.2f}%)")
        
        print(f"  平均距离: {np.mean(distances):.2f}")
        print(f"  最大距离: {max(distances)}")
        
        # 创建图表
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 左侧：频数柱状图
        bars = ax1.bar(unique_distances, counts, color='skyblue', alpha=0.7, edgecolor='navy')
        ax1.set_xlabel('最短距离')
        ax1.set_ylabel('博客对数量')
        ax1.set_title('博客网络最短距离分布（频数）')
        ax1.grid(True, alpha=0.3)
        
        # 在柱子上添加数值标签
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + max(counts)*0.01,
                    f'{count}', ha='center', va='bottom', fontsize=9)
        
        # 右侧：百分比柱状图
        percentages = [(count / total_pairs) * 100 for count in counts]
        bars2 = ax2.bar(unique_distances, percentages, color='lightcoral', alpha=0.7, edgecolor='darkred')
        ax2.set_xlabel('最短距离')
        ax2.set_ylabel('百分比 (%)')
        ax2.set_title('博客网络最短距离分布（百分比）')
        ax2.grid(True, alpha=0.3)
        
        # 在柱子上添加百分比标签
        for bar, percentage in zip(bars2, percentages):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + max(percentages)*0.01,
                    f'{percentage:.1f}%', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        
        # 保存图片
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'blog_distance_histogram_{timestamp}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✓ 图表已保存: {output_path}")
        
        plt.show()
        
        return {
            'distances': unique_distances,
            'counts': counts,
            'percentages': percentages,
            'mean_distance': np.mean(distances),
            'max_distance': max(distances),
            'output_path': output_path
        }
    
    def create_advanced_visualization(self, distances, network_props, output_dir='output'):
        """创建更高级的可视化图表"""
        if not distances:
            return
        
        os.makedirs(output_dir, exist_ok=True)
        
        # 设置样式
        plt.style.use('seaborn-v0_8')
        plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 创建多子图
        fig = plt.figure(figsize=(16, 12))
        
        # 1. 距离分布（累积分布）
        ax1 = plt.subplot(2, 2, 1)
        unique_distances, counts = np.unique(distances, return_counts=True)
        cumulative = np.cumsum(counts) / np.sum(counts) * 100
        
        ax1.bar(unique_distances, cumulative, alpha=0.7, color='teal')
        ax1.set_xlabel('最短距离')
        ax1.set_ylabel('累积百分比 (%)')
        ax1.set_title('最短距离累积分布')
        ax1.grid(True, alpha=0.3)
        
        # 2. 距离分布箱线图
        ax2 = plt.subplot(2, 2, 2)
        ax2.boxplot(distances, vert=True, patch_artist=True,
                   boxprops=dict(facecolor='lightblue', color='navy'),
                   medianprops=dict(color='red'))
        ax2.set_ylabel('最短距离')
        ax2.set_title('最短距离分布箱线图')
        ax2.grid(True, alpha=0.3)
        
        # 3. 网络属性摘要
        ax3 = plt.subplot(2, 2, 3)
        ax3.axis('off')
        
        props_text = f"""
网络属性摘要:

基本统计:
• 节点数量: {network_props['node_count']}
• 边数量: {network_props['edge_count']}
• 平均入度: {network_props['avg_in_degree']:.2f}
• 平均出度: {network_props['avg_out_degree']:.2f}

连通性:
• 连通分量: {network_props['component_count']}
• 最大分量: {network_props['largest_component']} 个节点

距离统计:
• 平均最短距离: {np.mean(distances):.2f}
• 最大最短距离: {max(distances)}
• 距离标准差: {np.std(distances):.2f}
"""
        
        ax3.text(0.1, 0.9, props_text, transform=ax3.transAxes, fontsize=12,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # 4. 距离分布密度图
        ax4 = plt.subplot(2, 2, 4)
        sns.histplot(distances, kde=True, ax=ax4, color='purple', alpha=0.7)
        ax4.set_xlabel('最短距离')
        ax4.set_ylabel('密度')
        ax4.set_title('最短距离密度分布')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存图片
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'blog_network_analysis_{timestamp}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✓ 高级分析图表已保存: {output_path}")
        
        plt.show()

def main():
    """主函数"""
    print("=" * 60)
    print("博客网络最短距离分析工具")
    print("=" * 60)
    
    # 创建分析器实例
    analyzer = BlogNetworkAnalyzer()
    
    # 从数据库加载数据
    if not analyzer.load_data_from_db():
        return
    
    # 分析网络属性
    network_props = analyzer.analyze_network_properties()
    
    # 询问计算方式
    print("\n计算选项:")
    print("1. 全量计算（较慢但准确）")
    print("2. 抽样计算（较快，适合大网络）")
    
    try:
        choice = input("请选择 (1/2，默认2): ").strip() or "2"
    except:
        choice = "2"
    
    sample_size = None
    if choice == "2":
        try:
            sample_size = int(input("请输入抽样数量 (默认500): ").strip() or "500")
        except:
            sample_size = 500
        print(f"使用抽样计算，样本量: {sample_size}")
    else:
        print("使用全量计算")
    
    # 计算最短路径
    distances, connectivity_ratio = analyzer.calculate_all_pairs_shortest_paths(sample_size)
    
    if not distances:
        print("没有计算出有效的距离数据")
        return
    
    # 生成基础柱状图
    print("\n生成基础柱状图...")
    basic_results = analyzer.create_distance_histogram(distances)
    
    # 生成高级可视化
    print("\n生成高级分析图表...")
    analyzer.create_advanced_visualization(distances, network_props)
    
    # 输出总结报告
    print("\n" + "=" * 60)
    print("分析完成！")
    print("=" * 60)
    print(f"网络规模: {network_props['node_count']} 个博客，{network_props['edge_count']} 条友链")
    print(f"连通性: {connectivity_ratio*100:.2f}% 的博客对可以相互到达")
    print(f"平均最短距离: {basic_results['mean_distance']:.2f}")
    print(f"最大最短距离: {basic_results['max_distance']}")
    print(f"图表文件已保存到 output/ 目录")
    print("=" * 60)

if __name__ == '__main__':
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
博客网络聚类参数分析
计算并可视化网络的聚类系数、传递性等参数
"""

import pymysql
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict, deque
import os
import seaborn as sns
from datetime import datetime
import math

# 数据库配置（与之前相同）
try:
    from config import DB_CONFIG as CONFIG_DB_CONFIG
    DB_CONFIG = CONFIG_DB_CONFIG.copy()
except ImportError:
    DB_CONFIG = {}

DB_CONFIG.setdefault('host', os.getenv('DB_HOST', 'localhost'))
DB_CONFIG.setdefault('port', int(os.getenv('DB_PORT', 3306)))
DB_CONFIG.setdefault('user', os.getenv('DB_USER', 'root'))
DB_CONFIG.setdefault('password', os.getenv('DB_PASSWORD', 'root'))
DB_CONFIG.setdefault('charset', 'utf8mb4')

DB_NAME = 'blog_link'
SITES_TABLE = 'sites'
FRIEND_LINKS_TABLE = 'friend_links'

class BlogClusteringAnalyzer:
    def __init__(self):
        self.graph = defaultdict(list)
        self.undirected_graph = defaultdict(list)  # 无向图版本
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
                    # 构建无向图（用于聚类系数计算）
                    self.undirected_graph[from_id].append(to_id)
                    self.undirected_graph[to_id].append(from_id)
            
            connection.close()
            print(f"✓ 加载了 {len(sites)} 个博客站点和 {len(links)} 条友链关系")
            return True
            
        except Exception as e:
            print(f"✗ 从数据库加载数据失败: {e}")
            return False
    
    def calculate_local_clustering_coefficient(self, node):
        """计算单个节点的局部聚类系数"""
        neighbors = set(self.undirected_graph[node])
        k = len(neighbors)
        
        if k < 2:
            return 0.0  # 度小于2的节点聚类系数为0
        
        # 计算邻居之间的实际边数
        actual_edges = 0
        for i, neighbor1 in enumerate(neighbors):
            for neighbor2 in list(neighbors)[i+1:]:
                if neighbor2 in self.undirected_graph[neighbor1]:
                    actual_edges += 1
        
        # 可能的边数：C(k, 2) = k*(k-1)/2
        possible_edges = k * (k - 1) / 2
        
        return actual_edges / possible_edges if possible_edges > 0 else 0.0
    
    def calculate_global_clustering_coefficient(self):
        """计算全局聚类系数（平均局部聚类系数）"""
        clustering_coefficients = []
        degrees = []
        
        for node in self.undirected_graph:
            cc = self.calculate_local_clustering_coefficient(node)
            clustering_coefficients.append(cc)
            degrees.append(len(self.undirected_graph[node]))
        
        # 平均聚类系数
        avg_clustering = np.mean(clustering_coefficients)
        
        # 按度加权的聚类系数
        weighted_clustering = np.average(clustering_coefficients, weights=degrees)
        
        return {
            'average_clustering': avg_clustering,
            'weighted_clustering': weighted_clustering,
            'all_coefficients': clustering_coefficients,
            'degrees': degrees
        }
    
    def calculate_transitivity(self):
        """计算传递性（全局聚类系数的另一种定义）"""
        triangles = 0  # 三角形数量
        triplets = 0   # 连通三元组数量
        
        for node in self.undirected_graph:
            neighbors = list(self.undirected_graph[node])
            k = len(neighbors)
            
            if k < 2:
                continue
            
            # 计算以该节点为中心的三元组数量
            triplets += k * (k - 1) / 2
            
            # 计算以该节点为中心的三角形数量
            for i in range(k):
                for j in range(i + 1, k):
                    if neighbors[j] in self.undirected_graph[neighbors[i]]:
                        triangles += 1
        
        # 每个三角形被计算了3次（每个顶点一次）
        triangles = triangles / 3
        
        transitivity = (3 * triangles) / triplets if triplets > 0 else 0
        
        return {
            'transitivity': transitivity,
            'triangles': triangles,
            'triplets': triplets
        }
    
    def calculate_degree_assortativity(self):
        """计算度同配性系数"""
        degrees = []
        for node in self.undirected_graph:
            degrees.append(len(self.undirected_graph[node]))
        
        if not degrees:
            return 0
        
        # 计算边两端的度
        source_degrees = []
        target_degrees = []
        
        for node in self.undirected_graph:
            source_degree = len(self.undirected_graph[node])
            for neighbor in self.undirected_graph[node]:
                target_degree = len(self.undirected_graph[neighbor])
                source_degrees.append(source_degree)
                target_degrees.append(target_degree)
        
        if not source_degrees:
            return 0
        
        # 计算皮尔逊相关系数
        correlation = np.corrcoef(source_degrees, target_degrees)[0, 1]
        
        return correlation if not np.isnan(correlation) else 0
    
    def analyze_clustering_properties(self):
        """全面分析网络的聚类特性"""
        print("\n正在分析网络聚类特性...")
        
        # 1. 计算聚类系数
        clustering_results = self.calculate_global_clustering_coefficient()
        
        # 2. 计算传递性
        transitivity_results = self.calculate_transitivity()
        
        # 3. 计算度同配性
        assortativity = self.calculate_degree_assortativity()
        
        # 4. 分析聚类系数与度的关系
        cc_vs_degree = self.analyze_clustering_vs_degree(
            clustering_results['all_coefficients'], 
            clustering_results['degrees']
        )
        
        # 5. 计算模块性（使用简单的社区检测）
        modularity_result = self.calculate_modularity()
        
        # 汇总结果
        results = {
            'average_clustering': clustering_results['average_clustering'],
            'weighted_clustering': clustering_results['weighted_clustering'],
            'transitivity': transitivity_results['transitivity'],
            'assortativity': assortativity,
            'modularity': modularity_result['modularity'],
            'community_count': modularity_result['community_count'],
            'clustering_distribution': cc_vs_degree,
            'triangles_count': transitivity_results['triangles'],
            'triplets_count': transitivity_results['triplets']
        }
        
        self.print_clustering_report(results)
        return results
    
    def analyze_clustering_vs_degree(self, clustering_coeffs, degrees):
        """分析聚类系数与节点度的关系"""
        # 按度分组统计平均聚类系数
        degree_bins = {}
        for deg, cc in zip(degrees, clustering_coeffs):
            if deg not in degree_bins:
                degree_bins[deg] = []
            degree_bins[deg].append(cc)
        
        # 计算每个度的平均聚类系数
        degree_avg_cc = {}
        for deg, cc_list in degree_bins.items():
            degree_avg_cc[deg] = np.mean(cc_list)
        
        return degree_avg_cc
    
    def calculate_modularity(self):
        """计算网络的模块性（使用Louvain方法的简化版本）"""
        # 简化的社区检测：使用连通分量作为社区
        communities = self.find_connected_components()
        community_count = len(communities)
        
        if community_count <= 1:
            return {'modularity': 0, 'community_count': community_count}
        
        # 计算模块性（简化计算）
        total_edges = sum(len(neighbors) for neighbors in self.undirected_graph.values()) / 2
        if total_edges == 0:
            return {'modularity': 0, 'community_count': community_count}
        
        modularity = 0
        for community in communities:
            # 计算社区内的边数
            intra_edges = 0
            for node in community:
                for neighbor in self.undirected_graph[node]:
                    if neighbor in community:
                        intra_edges += 1
            intra_edges /= 2  # 每条边被计算两次
            
            # 计算社区节点的总度数
            community_degree = sum(len(self.undirected_graph[node]) for node in community)
            
            # 模块性贡献
            modularity += (intra_edges / total_edges) - (community_degree / (2 * total_edges)) ** 2
        
        return {'modularity': modularity, 'community_count': community_count}
    
    def find_connected_components(self):
        """找到网络中的所有连通分量"""
        visited = set()
        components = []
        
        def bfs_component(start_id):
            component = set()
            queue = deque([start_id])
            visited.add(start_id)
            
            while queue:
                current_id = queue.popleft()
                component.add(current_id)
                
                for neighbor_id in self.undirected_graph[current_id]:
                    if neighbor_id not in visited:
                        visited.add(neighbor_id)
                        queue.append(neighbor_id)
            
            return component
        
        for node_id in self.undirected_graph.keys():
            if node_id not in visited:
                component = bfs_component(node_id)
                components.append(component)
        
        return components
    
    def print_clustering_report(self, results):
        """打印聚类分析报告"""
        print("\n" + "=" * 60)
        print("网络聚类特性分析报告")
        print("=" * 60)
        
        print(f"1. 聚类系数:")
        print(f"   • 平均局部聚类系数: {results['average_clustering']:.4f}")
        print(f"   • 加权聚类系数: {results['weighted_clustering']:.4f}")
        
        print(f"\n2. 传递性:")
        print(f"   • 全局传递性: {results['transitivity']:.4f}")
        print(f"   • 三角形数量: {int(results['triangles_count'])}")
        print(f"   • 连通三元组数量: {int(results['triplets_count'])}")
        
        print(f"\n3. 度相关性:")
        print(f"   • 度同配性系数: {results['assortativity']:.4f}")
        assort_type = "同配" if results['assortativity'] > 0 else "异配"
        print(f"   • 网络类型: {assort_type}网络")
        
        print(f"\n4. 社区结构:")
        print(f"   • 模块性: {results['modularity']:.4f}")
        print(f"   • 社区数量: {results['community_count']}")
        
        # 解释结果
        print(f"\n5. 结果解释:")
        self.interpret_clustering_results(results)
    
    def interpret_clustering_results(self, results):
        """解释聚类分析结果"""
        avg_cc = results['average_clustering']
        transitivity = results['transitivity']
        assortativity = results['assortativity']
        modularity = results['modularity']
        
        # 聚类系数解释
        if avg_cc > 0.3:
            cc_interpret = "高聚类 - 典型的社交网络特征，朋友圈高度重叠"
        elif avg_cc > 0.1:
            cc_interpret = "中等聚类 - 有一定的社区结构"
        else:
            cc_interpret = "低聚类 - 网络结构相对随机"
        
        # 传递性解释
        if transitivity > 0.2:
            trans_interpret = "高传递性 - 朋友的朋友很可能也是朋友"
        else:
            trans_interpret = "低传递性 - 三角关系不常见"
        
        # 同配性解释
        if assortativity > 0.1:
            assort_interpret = "同配网络 - 度高的节点倾向于连接度高的节点"
        elif assortativity < -0.1:
            assort_interpret = "异配网络 - 度高的节点倾向于连接度低的节点"
        else:
            assort_interpret = "中性网络 - 度连接无明显偏好"
        
        # 模块性解释
        if modularity > 0.3:
            mod_interpret = "强社区结构 - 网络有明显的分组"
        elif modularity > 0.1:
            mod_interpret = "中等社区结构"
        else:
            mod_interpret = "弱社区结构 - 网络相对均匀"
        
        print(f"   • 聚类系数: {cc_interpret}")
        print(f"   • 传递性: {trans_interpret}")
        print(f"   • 度相关性: {assort_interpret}")
        print(f"   • 模块性: {mod_interpret}")
    
    def create_clustering_visualizations(self, clustering_results, output_dir='output'):
        """创建聚类参数的可视化图表"""
        os.makedirs(output_dir, exist_ok=True)
        
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 创建多子图
        fig = plt.figure(figsize=(16, 12))
        
        # 1. 聚类系数分布直方图
        ax1 = plt.subplot(2, 3, 1)
        clustering_coeffs = clustering_results.get('all_coefficients', [])
        if clustering_coeffs:
            ax1.hist(clustering_coeffs, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            ax1.set_xlabel('局部聚类系数')
            ax1.set_ylabel('节点数量')
            ax1.set_title('局部聚类系数分布')
            ax1.grid(True, alpha=0.3)
        
        # 2. 聚类系数 vs 度
        ax2 = plt.subplot(2, 3, 2)
        cc_vs_degree = clustering_results.get('clustering_distribution', {})
        if cc_vs_degree:
            degrees = list(cc_vs_degree.keys())
            avg_cc = list(cc_vs_degree.values())
            ax2.scatter(degrees, avg_cc, alpha=0.6, color='coral')
            ax2.set_xlabel('节点度')
            ax2.set_ylabel('平均聚类系数')
            ax2.set_title('聚类系数 vs 节点度')
            ax2.set_xscale('log')
            ax2.set_yscale('log')
            ax2.grid(True, alpha=0.3)
        
        # 3. 度分布
        ax3 = plt.subplot(2, 3, 3)
        degrees = [len(self.undirected_graph[node]) for node in self.undirected_graph]
        if degrees:
            ax3.hist(degrees, bins=30, alpha=0.7, color='lightgreen', edgecolor='black')
            ax3.set_xlabel('节点度')
            ax3.set_ylabel('节点数量')
            ax3.set_title('度分布')
            ax3.set_yscale('log')
            ax3.grid(True, alpha=0.3)
        
        # 4. 聚类参数雷达图
        ax4 = plt.subplot(2, 3, 4, polar=True)
        metrics = ['聚类系数', '传递性', '同配性', '模块性']
        values = [
            clustering_results['average_clustering'],
            clustering_results['transitivity'],
            max(0, clustering_results['assortativity'] + 0.5),  # 调整到0-1范围
            max(0, clustering_results['modularity'] + 0.5)      # 调整到0-1范围
        ]
        
        angles = np.linspace(0, 2*np.pi, len(metrics), endpoint=False).tolist()
        values += values[:1]  # 闭合雷达图
        angles += angles[:1]
        
        ax4.plot(angles, values, 'o-', linewidth=2)
        ax4.fill(angles, values, alpha=0.25)
        ax4.set_xticks(angles[:-1])
        ax4.set_xticklabels(metrics)
        ax4.set_title('网络聚类参数雷达图')
        
        # 5. 参数对比条形图
        ax5 = plt.subplot(2, 3, 5)
        parameters = ['平均聚类\n系数', '加权聚类\n系数', '传递性', '同配性', '模块性']
        values = [
            clustering_results['average_clustering'],
            clustering_results['weighted_clustering'],
            clustering_results['transitivity'],
            clustering_results['assortativity'],
            clustering_results['modularity']
        ]
        
        colors = ['skyblue', 'lightcoral', 'lightgreen', 'gold', 'violet']
        bars = ax5.bar(parameters, values, color=colors, alpha=0.7)
        ax5.set_ylabel('参数值')
        ax5.set_title('聚类参数对比')
        ax5.grid(True, alpha=0.3)
        
        # 添加数值标签
        for bar, value in zip(bars, values):
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{value:.3f}', ha='center', va='bottom')
        
        # 6. 网络属性总结
        ax6 = plt.subplot(2, 3, 6)
        ax6.axis('off')
        
        summary_text = f"""
网络聚类特性总结

基本参数:
• 平均聚类系数: {clustering_results['average_clustering']:.4f}
• 加权聚类系数: {clustering_results['weighted_clustering']:.4f}
• 传递性: {clustering_results['transitivity']:.4f}
• 度同配性: {clustering_results['assortativity']:.4f}
• 模块性: {clustering_results['modularity']:.4f}

网络特征:
• 三角形数量: {int(clustering_results['triangles_count'])}
• 连通三元组: {int(clustering_results['triplets_count'])}
• 社区数量: {clustering_results['community_count']}
• 节点总数: {len(self.undirected_graph)}
"""
        
        ax6.text(0.1, 0.9, summary_text, transform=ax6.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        # 保存图片
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'blog_clustering_analysis_{timestamp}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✓ 聚类分析图表已保存: {output_path}")
        
        plt.show()
        
        return output_path

def main():
    """主函数"""
    print("=" * 60)
    print("博客网络聚类参数分析工具")
    print("=" * 60)
    
    # 创建分析器实例
    analyzer = BlogClusteringAnalyzer()
    
    # 从数据库加载数据
    if not analyzer.load_data_from_db():
        return
    
    # 分析聚类特性
    clustering_results = analyzer.analyze_clustering_properties()
    
    # 生成可视化图表
    print("\n生成聚类分析可视化图表...")
    analyzer.create_clustering_visualizations(clustering_results)
    
    print("\n" + "=" * 60)
    print("聚类分析完成！")
    print("=" * 60)

if __name__ == '__main__':
    main()
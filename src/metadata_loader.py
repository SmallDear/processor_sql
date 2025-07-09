"""
元数据加载器 - 简化版本
程序启动时加载JSON到内存，然后提供无参数方法获取数据
"""

import json
import os
from typing import Dict


# 全局变量用于缓存元数据
_global_metadata_cache = {}
_is_initialized = False


def init_metadata(json_file_path: str = "metadata_config_template.json"):
    """
    初始化元数据（加载JSON文件到内存）
    
    Args:
        json_file_path: JSON元数据文件路径
        
    Returns:
        bool: 是否加载成功
    """
    global _global_metadata_cache, _is_initialized
    
    # 检查文件是否存在
    if not os.path.exists(json_file_path):
        print(f"❌ 元数据文件不存在: {json_file_path}")
        _is_initialized = False
        return False
    
    try:
        print(f"📖 正在初始化元数据: {json_file_path}")
        
        with open(json_file_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # 验证格式
        if not isinstance(metadata, dict):
            print(f"❌ 元数据文件格式错误，应该是字典格式: {json_file_path}")
            _is_initialized = False
            return False
        
        # 保留所有表元数据（包括下划线开头的字段）
        filtered_metadata = {
            key: value for key, value in metadata.items() 
            if isinstance(value, list)
        }
        
        # 统计信息
        total_tables = len(filtered_metadata)
        total_columns = sum(len(cols) for cols in filtered_metadata.values())
        
        # 存储到全局缓存
        _global_metadata_cache = filtered_metadata
        _is_initialized = True
        
        print(f"✅ 成功初始化元数据: {total_tables} 个表，{total_columns} 个字段")
        return True
        
    except Exception as e:
        print(f"❌ 初始化元数据失败: {json_file_path}, 错误: {e}")
        _is_initialized = False
        return False


def get_metadata() -> Dict:
    """
    获取内存中的元数据（无参数）
    
    Returns:
        Dict: 元数据字典，格式为 {"table_name": ["col1", "col2", ...]}
    """
    global _global_metadata_cache, _is_initialized
    
    if not _is_initialized:
        print("⚠️  元数据未初始化，尝试自动初始化...")
        if init_metadata():
            return _global_metadata_cache
        else:
            return {}
    
    return _global_metadata_cache


def is_metadata_loaded() -> bool:
    """
    检查元数据是否已加载
    
    Returns:
        bool: 是否已加载
    """
    global _is_initialized
    return _is_initialized


def get_metadata_info() -> Dict:
    """
    获取元数据统计信息
    
    Returns:
        Dict: 统计信息
    """
    global _global_metadata_cache, _is_initialized
    
    if not _is_initialized:
        return {"loaded": False, "tables": 0, "columns": 0}
    
    total_tables = len(_global_metadata_cache)
    total_columns = sum(len(cols) for cols in _global_metadata_cache.values())
    
    return {
        "loaded": True,
        "tables": total_tables,
        "columns": total_columns,
        "sample_tables": list(_global_metadata_cache.keys())[:5]
    }


def clear_metadata():
    """清空元数据缓存"""
    global _global_metadata_cache, _is_initialized
    _global_metadata_cache.clear()
    _is_initialized = False
    print("🧹 已清空元数据缓存")


# 兼容旧版本的方法
def get_global_metadata(json_file_path: str) -> Dict:
    """
    兼容旧版本的方法（已弃用，建议使用 init_metadata + get_metadata）
    
    Args:
        json_file_path: JSON元数据文件路径
        
    Returns:
        Dict: 元数据字典
    """
    print("⚠️  get_global_metadata 方法已弃用，建议使用 init_metadata + get_metadata")
    if init_metadata(json_file_path):
        return get_metadata()
    return {}


if __name__ == "__main__":
    import time
    import sys
    
    print("=== 元数据加载器服务启动 ===")
    
    # 自动初始化默认的元数据文件
    default_metadata_file = "../metadata_config_template.json"
    if not os.path.exists(default_metadata_file):
        default_metadata_file = "metadata_config_template.json"
    
    if os.path.exists(default_metadata_file):
        print(f"🔄 正在初始化默认元数据文件: {default_metadata_file}")
        if init_metadata(default_metadata_file):
            info = get_metadata_info()
            print(f"✅ 元数据服务已启动: {info['tables']} 个表，{info['columns']} 个字段")
            print("🚀 元数据已加载到内存，其他程序现在可以调用 get_metadata() 获取数据")
            print("💡 使用 Ctrl+C 停止服务")
            
            # 保持程序运行，让其他程序可以调用
            try:
                while True:
                    time.sleep(60)  # 每60秒检查一次
                    if is_metadata_loaded():
                        print(f"📊 元数据服务运行中... ({time.strftime('%Y-%m-%d %H:%M:%S')})")
                    else:
                        print("⚠️  元数据丢失，尝试重新加载...")
                        init_metadata(default_metadata_file)
            except KeyboardInterrupt:
                print("\n🛑 接收到停止信号，正在关闭元数据服务...")
                clear_metadata()
                print("✅ 元数据服务已停止")
                sys.exit(0)
        else:
            print("❌ 元数据初始化失败，服务启动失败")
            sys.exit(1)
    else:
        print(f"❌ 找不到元数据文件: {default_metadata_file}")
        print("💡 请确保元数据文件存在，或使用 init_metadata(file_path) 手动初始化")
        sys.exit(1) 
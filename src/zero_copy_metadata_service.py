#!/usr/bin/env python3
"""
零拷贝共享内存元数据服务 - 多文件版本
支持加载文件夹下所有JSON文件，每个文件对应一个独立的共享内存
"""

import json
import os
import sys
import time
import signal
import atexit
import pickle
import glob
from typing import Dict, List, Optional, Tuple
from multiprocessing import shared_memory

# 服务配置
DEFAULT_METADATA_DIR = "../src/metadata_json/"

class ZeroCopyMetadataService:
    """零拷贝元数据服务 - 支持多文件"""
    
    def __init__(self):
        self.shared_memories: Dict[str, shared_memory.SharedMemory] = {}
        self.data_sizes: Dict[str, int] = {}
        self.metadata_files: Dict[str, str] = {}
        self.is_creator = False
        
    def find_metadata_files(self, directory: str) -> List[Tuple[str, str]]:
        """
        查找指定目录下的所有JSON文件
        
        Returns:
            List[Tuple[str, str]]: [(文件名(不含后缀), 完整路径), ...]
        """
        if not os.path.exists(directory):
            print(f"❌ 目录不存在: {directory}")
            return []
        
        json_files = []
        pattern = os.path.join(directory, "*.json")
        
        for file_path in glob.glob(pattern):
            if os.path.isfile(file_path):
                filename = os.path.basename(file_path)
                name_without_ext = os.path.splitext(filename)[0]
                json_files.append((name_without_ext, os.path.abspath(file_path)))
        
        return json_files
    
    def load_metadata_from_file(self, file_path: str) -> Optional[Dict]:
        """从单个JSON文件加载元数据"""
        try:
            print(f"📖 加载元数据: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 过滤出表元数据
            metadata = {k: v for k, v in data.items() if isinstance(v, list)}
            
            table_count = len(metadata)
            column_count = sum(len(cols) for cols in metadata.values())
            
            print(f"   ✅ 表: {table_count} 个，字段: {column_count} 个")
            return metadata
            
        except Exception as e:
            print(f"   ❌ 加载失败: {e}")
            return None
    
    def create_shared_memory_for_file(self, name: str, metadata: Dict) -> bool:
        """为单个文件创建共享内存"""
        try:
            # 序列化数据
            serialized_data = pickle.dumps(metadata)
            data_size = len(serialized_data)
            
            # 计算内存大小（留20%余量）
            memory_size = int(data_size * 1.2)
            
            # 直接使用文件名作为共享内存名称
            shm_name = name
            
            print(f"   💾 创建共享内存: {shm_name} ({memory_size} 字节)")
            
            # 清理可能存在的旧内存
            self.cleanup_existing_memory(shm_name)
            
            # 创建新的共享内存
            shm = shared_memory.SharedMemory(
                name=shm_name,
                create=True,
                size=memory_size
            )
            
            # 写入数据
            shm.buf[:data_size] = serialized_data
            
            # 保存到管理字典
            self.shared_memories[name] = shm
            self.data_sizes[name] = data_size
            
            print(f"   ✅ 成功创建: {shm_name}")
            return True
            
        except Exception as e:
            print(f"   ❌ 创建共享内存失败: {e}")
            return False
    
    def cleanup_existing_memory(self, shm_name: str):
        """清理可能存在的共享内存"""
        try:
            temp_shm = shared_memory.SharedMemory(name=shm_name)
            temp_shm.unlink()
            temp_shm.close()
            print(f"   🗑️  清理旧内存: {shm_name}")
        except FileNotFoundError:
            pass  # 不存在，正常
        except Exception as e:
            print(f"   ⚠️  清理旧内存时出错: {e}")
    
    def start_service(self, metadata_dir: Optional[str] = None) -> bool:
        """启动服务"""
        print("=" * 60)
        print("🚀 零拷贝元数据服务启动 - 多文件版本")
        print("=" * 60)
        
        # 注册清理处理器
        self.register_cleanup()
        
        # 确定元数据目录
        if metadata_dir and os.path.exists(metadata_dir):
            target_dir = metadata_dir
        elif os.path.exists(DEFAULT_METADATA_DIR):
            target_dir = DEFAULT_METADATA_DIR
        else:
            print(f"❌ 找不到元数据目录: {DEFAULT_METADATA_DIR}")
            return False
        
        print(f"📁 扫描目录: {target_dir}")
        
        # 查找所有JSON文件
        json_files = self.find_metadata_files(target_dir)
        
        if not json_files:
            print(f"❌ 目录中没有找到JSON文件")
            return False
        
        print(f"📋 找到 {len(json_files)} 个JSON文件")
        
        # 处理每个JSON文件
        success_count = 0
        
        for name, file_path in json_files:
            print(f"\n🔄 处理文件: {name}")
            
            # 加载元数据
            metadata = self.load_metadata_from_file(file_path)
            if not metadata:
                print(f"   ⚠️  跳过文件: {name}")
                continue
            
            # 创建共享内存
            if self.create_shared_memory_for_file(name, metadata):
                self.metadata_files[name] = file_path
                success_count += 1
            else:
                print(f"   ❌ 处理失败: {name}")
        
        if success_count > 0:
            self.is_creator = True
            print(f"\n✅ 服务启动成功!")
            print(f"📊 成功处理: {success_count}/{len(json_files)} 个文件")
            
            # 显示所有成功的文件
            print(f"\n📋 可用的元数据文件:")
            for name in sorted(self.metadata_files.keys()):
                table_count = len(self.get_metadata_direct(name))
                print(f"   • {name} ({table_count} 个表)")
            
            return True
        else:
            print(f"\n❌ 没有成功处理任何文件")
            return False
    
    def get_metadata_direct(self, name: str) -> Dict:
        """直接从内存中获取元数据（服务内部使用）"""
        if name not in self.shared_memories:
            return {}
        
        try:
            shm = self.shared_memories[name]
            data_size = self.data_sizes[name]
            
            # 读取数据
            data_bytes = bytes(shm.buf[:data_size])
            metadata = pickle.loads(data_bytes)
            
            return metadata if isinstance(metadata, dict) else {}
            
        except Exception as e:
            print(f"❌ 读取内存数据失败: {e}")
            return {}
    
    def register_cleanup(self):
        """注册清理处理器"""
        def signal_handler(signum, frame):
            print(f"\n🛑 收到信号 {signum}，清理内存...")
            self.cleanup()
            sys.exit(0)
        
        def exit_handler():
            self.cleanup()
        
        # 注册信号处理器
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            if hasattr(signal, 'SIGBREAK'):
                signal.signal(signal.SIGBREAK, signal_handler)
        except Exception as e:
            print(f"⚠️  信号处理器注册失败: {e}")
        
        # 注册退出处理器
        atexit.register(exit_handler)
        print("✅ 清理处理器已注册")
    
    def keep_running(self):
        """保持服务运行"""
        print(f"\n🔄 服务运行中... (按 Ctrl+C 停止)")
        print(f"💡 可用的元数据文件: {', '.join(sorted(self.metadata_files.keys()))}")
        
        try:
            while True:
                time.sleep(60)  # 每分钟检查一次
                current_time = time.strftime('%H:%M:%S')
                print(f"📊 服务正常运行 ({current_time}) - {len(self.shared_memories)} 个共享内存")
        except KeyboardInterrupt:
            print("\n🛑 收到停止信号")
        except Exception as e:
            print(f"\n❌ 服务异常: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        if not self.shared_memories:
            return
        
        print(f"\n🧹 清理 {len(self.shared_memories)} 个共享内存...")
        
        for name, shm in self.shared_memories.items():
            try:
                if self.is_creator:
                    shm.unlink()
                    print(f"🗑️  删除共享内存: {name}")
                
                shm.close()
                print(f"🔒 关闭连接: {name}")
                
            except Exception as e:
                print(f"⚠️  清理 {name} 时出错: {e}")
        
        self.shared_memories.clear()
        self.data_sizes.clear()
        self.metadata_files.clear()
        self.is_creator = False
        print("✅ 清理完成")

# ============= 客户端接口 =============

def get_metadata(shared_memory_name: str) -> Dict:
    """
    零拷贝获取元数据 - 主要接口
    
    Args:
        shared_memory_name: 共享内存名称（对应JSON文件名，不含后缀）
    
    Returns:
        Dict: 元数据字典 {"table_name": ["col1", "col2", ...]}
    """
    try:
        # 连接共享内存
        shm = shared_memory.SharedMemory(name=shared_memory_name)
        
        # 直接读取全部数据
        full_data = bytes(shm.buf)
        metadata = pickle.loads(full_data)
        
        shm.close()
        
        # 验证数据有效性
        if isinstance(metadata, dict) and metadata:
            return metadata
        else:
            return {}
        
    except FileNotFoundError:
        return {}
    except Exception as e:
        return {}

def is_service_running(shared_memory_name: str) -> bool:
    """
    检查服务是否运行
    
    Args:
        shared_memory_name: 共享内存名称
    
    Returns:
        bool: 服务是否运行
    """
    try:
        shm = shared_memory.SharedMemory(name=shared_memory_name)
        shm.close()
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return False



def get_service_status(shared_memory_name: str) -> Dict:
    """
    获取服务状态
    
    Args:
        shared_memory_name: 共享内存名称
    
    Returns:
        Dict: 服务状态信息
    """
    if not is_service_running(shared_memory_name):
        return {"running": False, "error": f"服务未运行: {shared_memory_name}"}
    
    try:
        metadata = get_metadata(shared_memory_name)
        if metadata:
            return {
                "running": True,
                "file_name": shared_memory_name,
                "table_count": len(metadata),
                "column_count": sum(len(cols) for cols in metadata.values()),
                "memory_name": shared_memory_name
            }
        else:
            return {"running": False, "error": f"数据获取失败: {shared_memory_name}"}
    except Exception as e:
        return {"running": False, "error": f"状态检查失败: {e}"}

# ============= 兼容性接口 =============

def is_metadata_loaded(shared_memory_name: str = "metadata_config_template") -> bool:
    """兼容性接口 - 检查默认元数据是否加载"""
    return is_service_running(shared_memory_name)

# ============= 主程序 =============

if __name__ == "__main__":
    service = ZeroCopyMetadataService()
    
    # 支持命令行参数
    metadata_dir = sys.argv[1] if len(sys.argv) > 1 else None
    
    # 启动服务
    if service.start_service(DEFAULT_METADATA_DIR):
        service.keep_running()
    else:
        print("❌ 服务启动失败")
        sys.exit(1) 
#!/usr/bin/env python3
"""
零拷贝共享内存元数据服务 - 最终版
启动即加载，调用简单，自动清理，高性能零拷贝访问
"""

import json
import os
import sys
import time
import signal
import atexit
import pickle
from typing import Dict, Optional
from multiprocessing import shared_memory

# 服务配置
SHARED_MEMORY_NAME = "zero_copy_metadata"
DEFAULT_METADATA_FILE = "metadata_config_template.json"

class ZeroCopyMetadataService:
    """零拷贝元数据服务"""
    
    def __init__(self):
        self.shm: Optional[shared_memory.SharedMemory] = None
        self.data_size = 0
        self.is_creator = False
        self.metadata_file = ""
        
    def find_metadata_file(self) -> Optional[str]:
        """智能查找元数据文件"""
        search_paths = [
            DEFAULT_METADATA_FILE,
            f"../{DEFAULT_METADATA_FILE}",
            f"src/{DEFAULT_METADATA_FILE}",
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                return os.path.abspath(path)
        return None
    
    def load_metadata(self, file_path: str) -> Optional[Dict]:
        """加载JSON元数据"""
        try:
            print(f"📖 加载元数据: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 过滤出表元数据
            metadata = {k: v for k, v in data.items() if isinstance(v, list)}
            
            table_count = len(metadata)
            column_count = sum(len(cols) for cols in metadata.values())
            
            print(f"✅ 加载成功: {table_count} 个表，{column_count} 个字段")
            return metadata
            
        except Exception as e:
            print(f"❌ 加载失败: {e}")
            return None
    
    def start_service(self, metadata_file: Optional[str] = None) -> bool:
        """启动服务"""
        print("=" * 60)
        print("🚀 零拷贝元数据服务启动")
        print("=" * 60)
        
        # 注册清理处理器
        self.register_cleanup()
        
        # 查找元数据文件
        if metadata_file and os.path.exists(metadata_file):
            self.metadata_file = os.path.abspath(metadata_file)
        else:
            found_file = self.find_metadata_file()
            if found_file:
                self.metadata_file = found_file
            else:
                print(f"❌ 找不到元数据文件: {DEFAULT_METADATA_FILE}")
                return False
        
        # 加载元数据
        metadata = self.load_metadata(self.metadata_file)
        if not metadata:
            return False
        
        # 序列化数据
        try:
            serialized_data = pickle.dumps(metadata)
            self.data_size = len(serialized_data)
            
            # 计算内存大小（留20%余量）
            memory_size = int(self.data_size * 1.2)
            
            print(f"💾 创建共享内存: {memory_size} 字节")
            
            # 清理可能存在的旧内存
            self.cleanup_existing()
            
            # 创建新的共享内存
            self.shm = shared_memory.SharedMemory(
                name=SHARED_MEMORY_NAME,
                create=True,
                size=memory_size
            )
            self.is_creator = True
            
            # 写入数据
            self.shm.buf[:self.data_size] = serialized_data
            
            print(f"✅ 服务启动成功!")
            print(f"🔗 共享内存: {SHARED_MEMORY_NAME}")
            print(f"📊 数据大小: {self.data_size} 字节")
            print(f"📋 表数量: {len(metadata)} 个")
            
            return True
            
        except Exception as e:
            print(f"❌ 创建共享内存失败: {e}")
            return False
    
    def cleanup_existing(self):
        """清理可能存在的共享内存"""
        try:
            temp_shm = shared_memory.SharedMemory(name=SHARED_MEMORY_NAME)
            temp_shm.unlink()
            temp_shm.close()
            print("🗑️  清理旧的共享内存")
        except FileNotFoundError:
            pass  # 不存在，正常
        except Exception as e:
            print(f"⚠️  清理旧内存时出错: {e}")
    
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
        print("\n🔄 服务运行中... (按 Ctrl+C 停止)")
        
        try:
            while True:
                time.sleep(60)  # 每分钟检查一次
                current_time = time.strftime('%H:%M:%S')
                print(f"📊 服务正常运行 ({current_time})")
        except KeyboardInterrupt:
            print("\n🛑 收到停止信号")
        except Exception as e:
            print(f"\n❌ 服务异常: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        if not self.shm:
            return
        
        try:
            if self.is_creator:
                self.shm.unlink()
                print(f"🗑️  删除共享内存: {SHARED_MEMORY_NAME}")
            
            self.shm.close()
            print("🔒 关闭共享内存连接")
            
        except Exception as e:
            print(f"⚠️  清理时出错: {e}")
        finally:
            self.shm = None
            self.is_creator = False

# ============= 客户端接口 =============

def get_metadata() -> Dict:
    """
    零拷贝获取元数据 - 主要接口
    
    Returns:
        Dict: 元数据字典 {"table_name": ["col1", "col2", ...]}
    """
    try:
        # 连接共享内存
        shm = shared_memory.SharedMemory(name=SHARED_MEMORY_NAME)
        
        # 尝试不同的数据大小来读取
        # 由于我们知道数据不会超过内存大小，可以从小到大尝试
        for size in [3000, 4000, 5000, 6000, 8000, 10000]:
            if size > len(shm.buf):
                continue
                
            try:
                # 读取数据
                data_bytes = bytes(shm.buf[:size])
                metadata = pickle.loads(data_bytes)
                
                # 验证数据有效性
                if isinstance(metadata, dict) and metadata:
                    shm.close()
                    return metadata
                    
            except (pickle.UnpicklingError, EOFError):
                continue
        
        # 如果固定大小都失败，尝试读取全部
        try:
            full_data = bytes(shm.buf)
            # 找到实际数据结束位置（pickle数据通常以特定字节结束）
            for end_pos in range(len(full_data), 0, -100):
                try:
                    metadata = pickle.loads(full_data[:end_pos])
                    if isinstance(metadata, dict) and metadata:
                        shm.close()
                        return metadata
                except:
                    continue
        except Exception:
            pass
        
        shm.close()
        print("❌ 无法解析共享内存数据")
        return {}
        
    except FileNotFoundError:
        print(f"❌ 服务未运行: {SHARED_MEMORY_NAME}")
        print("💡 请先运行: python zero_copy_metadata_service.py")
        return {}
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return {}

def is_service_running() -> bool:
    """检查服务是否运行"""
    try:
        shm = shared_memory.SharedMemory(name=SHARED_MEMORY_NAME)
        shm.close()
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return False

def get_service_status() -> Dict:
    """获取服务状态"""
    if not is_service_running():
        return {"running": False, "error": "服务未运行"}
    
    try:
        metadata = get_metadata()
        if metadata:
            return {
                "running": True,
                "table_count": len(metadata),
                "column_count": sum(len(cols) for cols in metadata.values()),
                "memory_name": SHARED_MEMORY_NAME
            }
        else:
            return {"running": False, "error": "数据获取失败"}
    except Exception as e:
        return {"running": False, "error": f"状态检查失败: {e}"}

# ============= 兼容性接口 =============

def is_metadata_loaded() -> bool:
    """兼容性接口"""
    return is_service_running()

# ============= 主程序 =============

if __name__ == "__main__":
    service = ZeroCopyMetadataService()
    
    # 支持命令行参数
    metadata_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # 启动服务
    if service.start_service(metadata_file):
        service.keep_running()
    else:
        print("❌ 服务启动失败")
        sys.exit(1) 
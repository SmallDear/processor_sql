"""
批量重新解析失败文件工具（简化版）
========================

该程序直接读取包含文件路径的文本文件，
逐个调用lineage_analysis方法处理这些文件。

使用方法：
python batch_reprocess_failed_files.py <文件路径列表>
"""

import sys
from lineage_sql_enhanced import lineage_analysis


def determine_db_type_from_extension(file_path):
    """根据文件扩展名确定数据库类型"""
    if file_path.lower().endswith(('.hql', '.hive')):
        return 'hive'
    else:
        return 'oracle'


def batch_reprocess_files(failed_files_list_path):
    """
    批量重新解析文件（简化版）
    """
    print(f"批量处理文件列表: {failed_files_list_path}")
    
    all_results = []
    
    # 读取文件路径列表
    with open(failed_files_list_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            file_path = line.strip()
            if not file_path or file_path.startswith('#'):  # 跳过空行和注释
                continue
                
            print(f"\n[{line_num}] 处理: {file_path}")
            
            try:
                # 确定数据库类型
                db_type = determine_db_type_from_extension(file_path)
                
                # 调用lineage_analysis
                result = lineage_analysis(file=file_path, db_type=db_type)
                
                # 添加结果
                all_results.append(f"-- 文件 {line_num}: {file_path}")
                all_results.append(result)
                all_results.append("")
                
                print("✅ 完成")
                
            except Exception as e:
                error_result = f"-- 文件 {line_num}: {file_path}\n-- 处理失败: {e}"
                all_results.append(error_result)
                all_results.append("")
                print(f"❌ 失败: {e}")
    
    return "\n".join(all_results)


def main():
    """主程序入口"""
    if len(sys.argv) != 2:
        print("使用方法: python batch_reprocess_failed_files.py <文件路径列表>")
        return
    
    file_list_path = sys.argv[1]
    
    try:
        result = batch_reprocess_files(file_list_path)
        print("\n=== 批量处理结果 ===")
        print(result)
    except Exception as e:
        print(f"处理失败: {e}")


if __name__ == "__main__":
    main() 
"""
基本测试用例
"""

def test_import():
    """测试是否可以正确导入包"""
    import src
    assert src.__version__ == "0.1.0" 
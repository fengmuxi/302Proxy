"""
Cython 编译辅助脚本
用法: python build_cython.py [clean|build|docker]
"""
import os
import sys
import shutil
import subprocess

# 需要编译的核心模块
CORE_MODULES = [
    "config",
    "config_store",
    "proxy_core",
    "admin_console",
    "auto_ban_monitor",
    "email_notifier",
    "email_templates",
    "ip_ban_manager",
    "geo_service",
    "ip_result_cache",
    "offline_geoip_sync",
]

def clean():
    """清理编译产物"""
    print("清理编译产物...")
    dirs_to_clean = ["build", "dist", "__pycache__"]
    files_to_clean = ["*.so", "*.pyd", "*.c", "*.h"]
    
    for d in dirs_to_clean:
        if os.path.exists(d):
            shutil.rmtree(d)
            print(f"  删除目录: {d}")
    
    for module in CORE_MODULES:
        for ext in [".c", ".h", ".so", ".pyd"]:
            f = f"{module}{ext}"
            if os.path.exists(f):
                os.remove(f)
                print(f"  删除文件: {f}")
    
    print("清理完成")

def build():
    """编译 Cython 模块"""
    print("开始编译 Cython 模块...")
    
    # 检查 Cython 是否安装
    try:
        import Cython
        print(f"Cython 版本: {Cython.__version__}")
    except ImportError:
        print("错误: 未安装 Cython，请运行: pip install cython")
        sys.exit(1)
    
    # 检查 C 编译器
    try:
        result = subprocess.run(["gcc", "--version"], capture_output=True)
        if result.returncode != 0:
            raise Exception()
        print("GCC 编译器: 已安装")
    except:
        print("警告: 未检测到 GCC 编译器，请安装: apt-get install gcc (Linux) 或 MinGW (Windows)")
    
    # 执行编译
    cmd = [sys.executable, "setup_cython.py", "build_ext", "--inplace"]
    print(f"执行命令: {' '.join(cmd)}")
    
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("编译失败!")
        sys.exit(1)
    
    print("编译完成!")
    
    # 验证编译结果
    print("\n验证编译结果:")
    for module in CORE_MODULES:
        so_file = f"{module}.cpython-3*.so" if sys.platform != "win32" else f"{module}.pyd"
        if os.path.exists(f"{module}.cpython-310-x86_64-linux-gnu.so") or \
           os.path.exists(f"{module}.pyd"):
            print(f"  ✓ {module} - 编译成功")
        else:
            print(f"  ✗ {module} - 编译失败")

def docker_build():
    """使用 Docker 构建"""
    print("使用 Docker 构建...")
    
    # 先清理
    clean()
    
    # 构建 Docker 镜像
    cmd = ["docker", "build", "-t", "nginx302_proxy:cython", "-f", "Dockerfile.cython", "."]
    print(f"执行命令: {' '.join(cmd)}")
    
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("Docker 构建失败!")
        sys.exit(1)
    
    print("Docker 构建完成!")
    print("运行: docker run -p 8080:8080 nginx302_proxy:cython")

def show_help():
    """显示帮助信息"""
    print("""
Cython 编译辅助脚本

用法:
    python build_cython.py [命令]

命令:
    clean       清理编译产物
    build       编译 Cython 模块
    docker      使用 Docker 构建
    help        显示此帮助信息

示例:
    python build_cython.py clean      # 清理
    python build_cython.py build      # 编译
    python build_cython.py docker     # Docker 构建

注意:
    1. 编译前请确保已安装: pip install cython numpy
    2. Linux 需要 gcc: apt-get install gcc
    3. Windows 需要 Visual Studio Build Tools
""")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_help()
        sys.exit(0)
    
    cmd = sys.argv[1].lower()
    
    if cmd == "clean":
        clean()
    elif cmd == "build":
        build()
    elif cmd == "docker":
        docker_build()
    elif cmd in ["help", "--help", "-h"]:
        show_help()
    else:
        print(f"未知命令: {cmd}")
        show_help()
        sys.exit(1)

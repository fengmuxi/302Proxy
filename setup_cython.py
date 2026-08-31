"""
Cython 编译脚本 - 将 Python 核心模块编译成 C 扩展
"""
import os
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy

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

# 排除不需要编译的模块
EXCLUDE_MODULES = [
    "main",  # 入口文件不编译
    "tests",  # 测试文件不编译
]

extensions = []
for module in CORE_MODULES:
    if module not in EXCLUDE_MODULES:
        extensions.append(
            Extension(
                name=module,
                sources=[f"{module}.pyx" if os.path.exists(f"{module}.pyx") else f"{module}.py"],
                include_dirs=[numpy.get_include()],
                extra_compile_args=["-O2"],
            )
        )

setup(
    name="nginx302_proxy",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "nonecheck": False,
            "optimize.use_switch": True,
            "optimize.unpackMethodCalls": True,
        },
    ),
    zip_safe=False,
)

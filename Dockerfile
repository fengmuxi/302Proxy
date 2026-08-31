# =============================================================================
# 多阶段 Docker 构建 - Cython 编译版
# =============================================================================

# 阶段 1: 编译环境
FROM python:3.11-slim AS builder

LABEL maintainer="nginx302_proxy"
LABEL description="HTTP Reverse Proxy with 302 Redirect Support - Cython Build"

# 安装编译工具
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /build

# 复制依赖文件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安装 Cython 和编译工具
RUN pip install --no-cache-dir cython numpy setuptools

# 复制源代码
COPY *.py ./
COPY static/ ./static/

# 复制配置文件
COPY config.yaml.template .

# 执行 Cython 编译
RUN python build_cython.py build

# 阶段 2: 运行环境（最小化镜像）
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/* /root/.cache

# 复制编译后的文件
COPY --from=builder /build/*.so ./ 2>/dev/null || true
COPY --from=builder /build/*.pyd ./ 2>/dev/null || true
COPY --from=builder /build/*.cpython-*.so ./ 2>/dev/null || true
COPY --from=builder /build/*.py ./
COPY --from=builder /build/static/ ./static/
COPY --from=builder /build/config.yaml.template ./config.yaml

# 创建数据目录
RUN mkdir -p /app/log /app/data

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 暴露端口
EXPOSE 18686

# 启动命令
CMD ["python", "main.py", "-c", "config.yaml"]

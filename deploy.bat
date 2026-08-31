@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================
REM  302 Proxy Docker 部署脚本
REM  用法: deploy.bat [版本标签]
REM  示例: deploy.bat v1.0.0
REM        deploy.bat (默认使用 latest)
REM ============================================

set IMAGE_NAME=registry.cn-hangzhou.aliyuncs.com/fengmuxi-docker-images/302_proxy
set TAG=%1
if "%TAG%"=="" set TAG=latest

echo.
echo ========================================
echo  302 Proxy Docker 部署
echo  镜像: %IMAGE_NAME%:%TAG%
echo ========================================
echo.

REM 步骤1: 构建镜像
echo [1/3] 构建Docker镜像...
docker build -t %IMAGE_NAME%:%TAG% .
if %errorlevel% neq 0 (
    echo [错误] 镜像构建失败！
    pause
    exit /b 1
)
echo [完成] 镜像构建成功
echo.

REM 步骤2: 推送到阿里云仓库
echo [2/3] 推送到阿里云镜像仓库...
docker push %IMAGE_NAME%:%TAG%
if %errorlevel% neq 0 (
    echo [错误] 镜像推送失败！请确认已登录: docker login registry.cn-hangzhou.aliyuncs.com
    pause
    exit /b 1
)
echo [完成] 镜像推送成功
echo.

REM 步骤3: 生成服务器部署命令
echo [3/3] 服务器部署命令:
echo.
echo ========================================
echo  请在服务器上执行以下命令:
echo ========================================
echo.
echo   # 拉取新镜像
echo   docker pull %IMAGE_NAME%:%TAG%
echo.
echo   # 重启服务
echo   cd /path/to/302_proxy
echo   docker-compose down
echo   docker-compose up -d
echo.
echo   # 或者一行命令:
echo   docker-compose pull ^&^& docker-compose up -d
echo ========================================
echo.

REM 可选: 直接推送到服务器（需要配置SSH免密）
set /p PUSH_TO_SERVER="是否直接推送到服务器？(y/N): "
if /i "%PUSH_TO_SERVER%"=="y" (
    set /p SERVER_HOST="服务器地址: "
    set /p SERVER_PATH="服务器路径 [/opt/302_proxy]: "
    if "!SERVER_PATH!"=="" set SERVER_PATH=/opt/302_proxy
    
    echo.
    echo 正在推送到服务器...
    echo 请确保已配置SSH免密登录
    echo.
    
    REM 保存当前镜像为tar文件
    docker save %IMAGE_NAME%:%TAG% -o 302_proxy_%TAG%.tar
    
    REM 上传到服务器
    scp 302_proxy_%TAG%.tar !SERVER_HOST!:!SERVER_PATH!/302_proxy_%TAG%.tar
    
    REM 在服务器上加载并重启
    ssh !SERVER_HOST! "cd !SERVER_PATH! && docker load -i 302_proxy_%TAG%.tar && docker-compose down && docker-compose up -d && rm -f 302_proxy_%TAG%.tar"
    
    REM 清理本地tar文件
    del 302_proxy_%TAG%.tar
    
    echo [完成] 部署到服务器成功！
)

echo.
echo 部署完成！
pause

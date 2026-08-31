"""邮件模板模块 - 提供科技风格的HTML邮件模板"""

from datetime import datetime
from typing import Optional


def get_email_template(
    alert_type: str = "测试邮件",
    ip: str = "",
    current_count: int = 0,
    threshold: int = 0,
    window_seconds: int = 0,
    system_name: str = "代理监控系统",
    system_url: Optional[str] = None,
    smtp_host: str = "",
    smtp_port: int = 465,
    sender: str = "",
    is_test: bool = False,
) -> tuple:
    """
    生成统一的邮件模板
    
    返回: (subject, html_content, text_content)
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if is_test:
        subject = f"✅ 【{system_name}】邮件测试成功"
        icon = "✓"
        title = "邮件测试成功"
        subtitle = f"{system_name} - 配置验证"
        header_class = "header-success"
        banner_class = "success-banner"
        banner_title = "邮件服务配置正常"
        banner_subtitle = "您的邮件提醒功能已就绪"
    else:
        subject = f"⚠️ 【{system_name}】IP请求异常提醒 - {ip}"
        icon = "⚠️"
        title = "IP请求异常提醒"
        subtitle = f"{system_name} - 安全监控"
        header_class = "header-alert"
        banner_class = "alert-banner"
        banner_title = "检测到异常请求"
        banner_subtitle = ip
    
    # 信息卡片
    if is_test:
        info_card = f"""
        <div class="info-card">
            <h3>📧 邮件配置详情</h3>
            <div class="info-grid">
                <div class="info-item">
                    <span class="info-label">SMTP服务器</span>
                    <span class="info-value">{smtp_host}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">SMTP端口</span>
                    <span class="info-value">{smtp_port}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">发件邮箱</span>
                    <span class="info-value">{sender}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">测试时间</span>
                    <span class="info-value">{now}</span>
                </div>
            </div>
        </div>
        """
    else:
        # 异常详情
        info_card = f"""
        <div class="info-card">
            <h3>🔍 异常详情</h3>
            <div class="info-grid">
                <div class="info-item">
                    <span class="info-label">异常类型</span>
                    <span class="info-value">{alert_type}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">当前计数</span>
                    <span class="info-value highlight">{current_count}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">阈值限制</span>
                    <span class="info-value">{threshold}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">监测窗口</span>
                    <span class="info-value">{window_seconds}秒</span>
                </div>
            </div>
        </div>
        
        <!-- 系统状态概览 -->
        <div class="status-box">
            <h4>📊 系统状态概览</h4>
            <div class="status-grid">
                <div class="status-item">
                    <span class="status-label">监测时间窗口</span>
                    <span class="status-value">{window_seconds}秒</span>
                </div>
                <div class="status-item">
                    <span class="status-label">触发时间</span>
                    <span class="status-value">{now}</span>
                </div>
                <div class="status-item">
                    <span class="status-label">当前阈值</span>
                    <span class="status-value">{threshold}</span>
                </div>
                <div class="status-item">
                    <span class="status-label">当前计数</span>
                    <span class="status-value highlight">{current_count}</span>
                </div>
            </div>
        </div>
        """
    
    # 处理建议
    if is_test:
        suggestion = ""
    elif "请求频率" in alert_type:
        suggestion = """
        <div class="suggestion-box">
            <h4>📋 处理建议</h4>
            <ul>
                <li>检查该IP是否为正常业务访问</li>
                <li>如确认为恶意请求，建议手动封禁该IP</li>
                <li>检查是否存在DDoS攻击或爬虫行为</li>
                <li>考虑调整请求频率阈值以适应正常业务需求</li>
            </ul>
        </div>
        """
    else:
        suggestion = """
        <div class="suggestion-box">
            <h4>📋 处理建议</h4>
            <ul>
                <li>检查该IP请求的资源是否存在</li>
                <li>确认是否为扫描器或恶意爬虫</li>
                <li>检查URL路径是否已变更</li>
                <li>如确认为恶意行为，建议封禁该IP</li>
            </ul>
        </div>
        """
    
    # 页脚
    footer_content = f"""
    <div class="footer">
        <p>此邮件由 {system_name} 自动发送</p>
        <p>请勿直接回复此邮件</p>
        {"<p><a href='" + system_url + "'>访问系统管理面板</a></p>" if system_url else ""}
    </div>
    """
    
    html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>{subject}</title>
    <style>
        /* 基础重置 */
        body, table, td, p, a, li, blockquote {{
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
        }}
        table, td {{
            mso-table-lspace: 0pt;
            mso-table-rspace: 0pt;
        }}
        img {{
            -ms-interpolation-mode: bicubic;
            border: 0;
            height: auto;
            line-height: 100%;
            outline: none;
            text-decoration: none;
        }}
        body {{
            margin: 0;
            padding: 0;
            width: 100% !important;
            height: 100% !important;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            font-family: 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
        }}
        
        /* 主容器 */
        .email-container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        /* 卡片样式 */
        .card {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            backdrop-filter: blur(10px);
        }}
        
        /* 头部样式 - 警告 */
        .header-alert {{
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 50%, #a93226 100%);
            padding: 30px;
            text-align: center;
            position: relative;
            overflow: hidden;
        }}
        
        /* 头部样式 - 成功 */
        .header-success {{
            background: linear-gradient(135deg, #27ae60 0%, #229954 50%, #1e8449 100%);
            padding: 30px;
            text-align: center;
            position: relative;
            overflow: hidden;
        }}
        
        .header-alert::before, .header-success::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: pulse 4s ease-in-out infinite;
        }}
        @keyframes pulse {{
            0%, 100% {{ transform: scale(1); opacity: 0.5; }}
            50% {{ transform: scale(1.1); opacity: 0.8; }}
        }}
        .header h1 {{
            margin: 0;
            color: #ffffff;
            font-size: 24px;
            font-weight: 600;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
            position: relative;
            z-index: 1;
        }}
        .header .subtitle {{
            color: rgba(255, 255, 255, 0.9);
            font-size: 14px;
            margin-top: 8px;
            position: relative;
            z-index: 1;
        }}
        
        /* 内容区域 */
        .content {{
            padding: 30px;
        }}
        
        /* 警告横幅 */
        .alert-banner {{
            background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%);
            border-left: 4px solid #e74c3c;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 24px;
        }}
        .alert-banner .alert-title {{
            color: #c62828;
            font-size: 16px;
            font-weight: 600;
            margin: 0 0 8px 0;
        }}
        .alert-banner .alert-ip {{
            color: #b71c1c;
            font-size: 20px;
            font-weight: 700;
            font-family: 'Consolas', 'Monaco', monospace;
        }}
        
        /* 成功横幅 */
        .success-banner {{
            background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
            border-left: 4px solid #27ae60;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 24px;
            text-align: center;
        }}
        .success-banner .success-icon {{
            font-size: 48px;
            margin-bottom: 8px;
        }}
        .success-banner .success-title {{
            color: #2e7d32;
            font-size: 18px;
            font-weight: 600;
            margin: 0;
        }}
        .success-banner .success-subtitle {{
            color: #4caf50;
            font-size: 14px;
            margin-top: 4px;
        }}
        
        /* 信息卡片 */
        .info-card {{
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .info-card h3 {{
            color: #2c3e50;
            font-size: 16px;
            margin: 0 0 16px 0;
            padding-bottom: 12px;
            border-bottom: 2px solid #e74c3c;
        }}
        .info-card:has(+ .status-box) h3 {{
            border-bottom: 2px solid #e74c3c;
        }}
        
        /* 信息网格 */
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }}
        .info-item {{
            background: #ffffff;
            border-radius: 8px;
            padding: 16px;
            border: 1px solid #e0e0e0;
            transition: all 0.3s ease;
        }}
        .info-item:hover {{
            border-color: #e74c3c;
            box-shadow: 0 4px 12px rgba(231, 76, 60, 0.15);
        }}
        .info-label {{
            display: block;
            color: #7f8c8d;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
        }}
        .info-value {{
            display: block;
            color: #2c3e50;
            font-size: 14px;
            font-weight: 600;
            word-break: break-all;
        }}
        .info-value.highlight {{
            color: #e74c3c;
            font-size: 20px;
        }}
        
        /* 处理建议 */
        .suggestion-box {{
            background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .suggestion-box h4 {{
            color: #e65100;
            font-size: 16px;
            margin: 0 0 12px 0;
        }}
        .suggestion-box ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .suggestion-box li {{
            color: #bf360c;
            font-size: 14px;
            line-height: 1.8;
        }}
        
        /* 系统状态 */
        .status-box {{
            background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .status-box h4 {{
            color: #1565c0;
            font-size: 16px;
            margin: 0 0 16px 0;
        }}
        .status-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }}
        .status-item {{
            background: #ffffff;
            border-radius: 8px;
            padding: 12px;
            text-align: center;
        }}
        .status-label {{
            display: block;
            color: #64b5f6;
            font-size: 11px;
            text-transform: uppercase;
            margin-bottom: 4px;
        }}
        .status-value {{
            display: block;
            color: #1976d2;
            font-size: 14px;
            font-weight: 600;
        }}
        .status-value.highlight {{
            color: #e74c3c;
            font-size: 18px;
        }}
        
        /* 页脚 */
        .footer {{
            background: #f5f5f5;
            padding: 20px 30px;
            text-align: center;
            border-top: 1px solid #e0e0e0;
        }}
        .footer p {{
            color: #9e9e9e;
            font-size: 12px;
            margin: 4px 0;
        }}
        .footer a {{
            color: #1976d2;
            text-decoration: none;
        }}
        .footer a:hover {{
            text-decoration: underline;
        }}
        
        /* 响应式设计 - 手机端优化 */
        @media only screen and (max-width: 600px) {{
            .email-container {{
                padding: 8px;
            }}
            .card {{
                border-radius: 12px;
            }}
            .header-alert, .header-success {{
                padding: 20px 16px;
            }}
            .header h1 {{
                font-size: 18px;
                margin: 0;
                white-space: nowrap;
            }}
            .header .subtitle {{
                font-size: 12px;
                margin-top: 4px;
                white-space: nowrap;
            }}
            .content {{
                padding: 16px;
            }}
            .alert-banner, .success-banner {{
                padding: 12px 14px;
                margin-bottom: 16px;
                border-radius: 6px;
            }}
            .alert-banner .alert-title {{
                font-size: 14px;
                margin: 0 0 6px 0;
                white-space: nowrap;
            }}
            .alert-banner .alert-ip {{
                font-size: 16px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}
            .success-banner .success-icon {{
                font-size: 36px;
                margin-bottom: 6px;
            }}
            .success-banner .success-title {{
                font-size: 16px;
                white-space: nowrap;
            }}
            .success-banner .success-subtitle {{
                font-size: 12px;
                white-space: nowrap;
            }}
            .info-card, .status-box, .suggestion-box {{
                padding: 14px;
                margin-bottom: 14px;
                border-radius: 8px;
            }}
            .info-card h3 {{
                font-size: 14px;
                margin: 0 0 12px 0;
                padding-bottom: 8px;
                white-space: nowrap;
            }}
            .status-box h4, .suggestion-box h4 {{
                font-size: 14px;
                margin: 0 0 10px 0;
                white-space: nowrap;
            }}
            .info-grid, .status-grid {{
                grid-template-columns: 1fr;
                gap: 10px;
            }}
            .info-item {{
                padding: 10px;
                border-radius: 6px;
            }}
            .info-label {{
                font-size: 10px;
                margin-bottom: 4px;
                white-space: nowrap;
            }}
            .info-value {{
                font-size: 13px;
                word-break: break-word;
            }}
            .info-value.highlight {{
                font-size: 18px;
            }}
            .status-item {{
                padding: 10px;
                border-radius: 6px;
            }}
            .status-label {{
                font-size: 10px;
                margin-bottom: 2px;
                white-space: nowrap;
            }}
            .status-value {{
                font-size: 13px;
                word-break: break-word;
            }}
            .status-value.highlight {{
                font-size: 16px;
            }}
            .suggestion-box ul {{
                padding-left: 16px;
            }}
            .suggestion-box li {{
                font-size: 13px;
                line-height: 1.6;
            }}
            .footer {{
                padding: 14px 16px;
            }}
            .footer p {{
                font-size: 11px;
                margin: 2px 0;
                white-space: nowrap;
            }}
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <div class="card">
            <!-- 头部 -->
            <div class="{header_class}">
                <h1>{icon} {title}</h1>
                <div class="subtitle">{subtitle}</div>
            </div>
            
            <!-- 内容 -->
            <div class="content">
                <!-- 横幅 -->
                <div class="{banner_class}">
                    {"" if is_test else '<div class="alert-title">' + banner_title + '</div>'}
                    {"" if is_test else '<div class="alert-ip">' + banner_subtitle + '</div>'}
                    {'<div class="success-icon">' + icon + '</div>' if is_test else ''}
                    {'<div class="success-title">' + banner_title + '</div>' if is_test else ''}
                    {'<div class="success-subtitle">' + banner_subtitle + '</div>' if is_test else ''}
                </div>
                
                <!-- 信息卡片 -->
                {info_card}
                
                <!-- 处理建议 -->
                {suggestion}
            </div>
            
            <!-- 页脚 -->
            {footer_content}
        </div>
    </div>
</body>
</html>
"""
    
    # 纯文本内容
    if is_test:
        text_content = f"""
{system_name} - 邮件测试成功

邮件服务配置正常
您的邮件提醒功能已就绪

配置详情:
SMTP服务器: {smtp_host}
SMTP端口: {smtp_port}
发件邮箱: {sender}
测试时间: {now}

此邮件由 {system_name} 测试发送
"""
    else:
        text_content = f"""
{system_name} - IP请求异常提醒

检测到异常请求
IP地址: {ip}
异常类型: {alert_type}
当前计数: {current_count}
阈值限制: {threshold}
监测窗口: {window_seconds}秒
触发时间: {now}

处理建议:
{"- 检查该IP是否为正常业务访问" if "请求频率" in alert_type else "- 检查该IP请求的资源是否存在"}
{"- 如确认为恶意请求，建议手动封禁该IP" if "请求频率" in alert_type else "- 确认是否为扫描器或恶意爬虫"}
{"- 检查是否存在DDoS攻击或爬虫行为" if "请求频率" in alert_type else "- 检查URL路径是否已变更"}
{"- 考虑调整请求频率阈值以适应正常业务需求" if "请求频率" in alert_type else "- 如确认为恶意行为，建议封禁该IP"}

此邮件由 {system_name} 自动发送
"""
    
    return subject, html_content, text_content


def get_ban_email_template(
    ip: str = "",
    reason: str = "请求频率超限",
    ban_duration_seconds: int = 3600,
    current_count: int = 0,
    threshold: int = 0,
    window_seconds: int = 60,
    system_name: str = "代理监控系统",
    system_url: Optional[str] = None,
) -> tuple:
    """
    生成IP封禁专用邮件模板（蓝色主题）
    
    返回: (subject, html_content, text_content)
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 格式化封禁时长
    if ban_duration_seconds >= 3600:
        ban_duration_str = f"{ban_duration_seconds // 3600}小时"
    elif ban_duration_seconds >= 60:
        ban_duration_str = f"{ban_duration_seconds // 60}分钟"
    else:
        ban_duration_str = f"{ban_duration_seconds}秒"
    
    subject = f"🔒 【{system_name}】IP已被封禁 - {ip}"
    
    # 页脚
    footer_content = f"""
    <div class="footer">
        <p>此邮件由 {system_name} 自动发送</p>
        <p>请勿直接回复此邮件</p>
        {"<p><a href='" + system_url + "'>访问系统管理面板</a></p>" if system_url else ""}
    </div>
    """
    
    html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>{subject}</title>
    <style>
        /* 基础重置 */
        body, table, td, p, a, li, blockquote {{
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
        }}
        table, td {{
            mso-table-lspace: 0pt;
            mso-table-rspace: 0pt;
        }}
        img {{
            -ms-interpolation-mode: bicubic;
            border: 0;
            height: auto;
            line-height: 100%;
            outline: none;
            text-decoration: none;
        }}
        body {{
            margin: 0;
            padding: 0;
            width: 100% !important;
            height: 100% !important;
            background: linear-gradient(135deg, #0d2137 0%, #1a3a5c 50%, #2c5282 100%);
            font-family: 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
        }}
        
        /* 主容器 */
        .email-container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        /* 卡片样式 */
        .card {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            backdrop-filter: blur(10px);
        }}
        
        /* 头部样式 - 蓝色主题 */
        .header-ban {{
            background: linear-gradient(135deg, #3498db 0%, #2980b9 50%, #2471a3 100%);
            padding: 30px;
            text-align: center;
            position: relative;
            overflow: hidden;
        }}
        .header-ban::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: pulse 4s ease-in-out infinite;
        }}
        @keyframes pulse {{
            0%, 100% {{ transform: scale(1); opacity: 0.5; }}
            50% {{ transform: scale(1.1); opacity: 0.8; }}
        }}
        .header-ban h1 {{
            margin: 0;
            color: #ffffff;
            font-size: 24px;
            font-weight: 600;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
            position: relative;
            z-index: 1;
        }}
        .header-ban .subtitle {{
            color: rgba(255, 255, 255, 0.9);
            font-size: 14px;
            margin-top: 8px;
            position: relative;
            z-index: 1;
        }}
        
        /* 内容区域 */
        .content {{
            padding: 30px;
        }}
        
        /* 封禁横幅 - 蓝色 */
        .ban-banner {{
            background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
            border-left: 4px solid #3498db;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 24px;
            text-align: center;
        }}
        .ban-banner .ban-icon {{
            font-size: 48px;
            margin-bottom: 8px;
        }}
        .ban-banner .ban-title {{
            color: #1565c0;
            font-size: 18px;
            font-weight: 600;
            margin: 0 0 8px 0;
        }}
        .ban-banner .ban-ip {{
            color: #1976d2;
            font-size: 24px;
            font-weight: 700;
            font-family: 'Consolas', 'Monaco', monospace;
        }}
        
        /* 信息卡片 */
        .info-card {{
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .info-card h3 {{
            color: #2c3e50;
            font-size: 16px;
            margin: 0 0 16px 0;
            padding-bottom: 12px;
            border-bottom: 2px solid #3498db;
        }}
        
        /* 信息网格 */
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }}
        .info-item {{
            background: #ffffff;
            border-radius: 8px;
            padding: 16px;
            border: 1px solid #e0e0e0;
            transition: all 0.3s ease;
        }}
        .info-item:hover {{
            border-color: #3498db;
            box-shadow: 0 4px 12px rgba(52, 152, 219, 0.15);
        }}
        .info-label {{
            display: block;
            color: #7f8c8d;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
            white-space: nowrap;
        }}
        .info-value {{
            display: block;
            color: #2c3e50;
            font-size: 14px;
            font-weight: 600;
            word-break: break-all;
        }}
        .info-value.highlight {{
            color: #3498db;
            font-size: 20px;
        }}
        .info-value.duration {{
            color: #e67e22;
            font-size: 18px;
        }}
        
        /* 封禁详情 */
        .ban-details {{
            background: linear-gradient(135deg, #fff8e1 0%, #ffecb3 100%);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .ban-details h4 {{
            color: #f57c00;
            font-size: 16px;
            margin: 0 0 12px 0;
        }}
        .ban-details ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .ban-details li {{
            color: #e65100;
            font-size: 14px;
            line-height: 1.8;
        }}
        
        /* 系统状态 */
        .status-box {{
            background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .status-box h4 {{
            color: #2e7d32;
            font-size: 16px;
            margin: 0 0 16px 0;
        }}
        .status-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }}
        .status-item {{
            background: #ffffff;
            border-radius: 8px;
            padding: 12px;
            text-align: center;
        }}
        .status-label {{
            display: block;
            color: #81c784;
            font-size: 11px;
            text-transform: uppercase;
            margin-bottom: 4px;
            white-space: nowrap;
        }}
        .status-value {{
            display: block;
            color: #388e3c;
            font-size: 14px;
            font-weight: 600;
        }}
        .status-value.highlight {{
            color: #3498db;
            font-size: 18px;
        }}
        
        /* 页脚 */
        .footer {{
            background: #f5f5f5;
            padding: 20px 30px;
            text-align: center;
            border-top: 1px solid #e0e0e0;
        }}
        .footer p {{
            color: #9e9e9e;
            font-size: 12px;
            margin: 4px 0;
        }}
        .footer a {{
            color: #1976d2;
            text-decoration: none;
        }}
        .footer a:hover {{
            text-decoration: underline;
        }}
        
        /* 响应式设计 - 手机端优化 */
        @media only screen and (max-width: 600px) {{
            .email-container {{
                padding: 8px;
            }}
            .card {{
                border-radius: 12px;
            }}
            .header-ban {{
                padding: 20px 16px;
            }}
            .header-ban h1 {{
                font-size: 18px;
                margin: 0;
                white-space: nowrap;
            }}
            .header-ban .subtitle {{
                font-size: 12px;
                margin-top: 4px;
                white-space: nowrap;
            }}
            .content {{
                padding: 16px;
            }}
            .ban-banner {{
                padding: 12px 14px;
                margin-bottom: 16px;
                border-radius: 6px;
            }}
            .ban-banner .ban-icon {{
                font-size: 36px;
                margin-bottom: 6px;
            }}
            .ban-banner .ban-title {{
                font-size: 16px;
                white-space: nowrap;
            }}
            .ban-banner .ban-ip {{
                font-size: 20px;
                white-space: nowrap;
            }}
            .info-card, .ban-details, .status-box {{
                padding: 14px;
                margin-bottom: 14px;
                border-radius: 8px;
            }}
            .info-card h3 {{
                font-size: 14px;
                margin: 0 0 12px 0;
                padding-bottom: 8px;
                white-space: nowrap;
            }}
            .ban-details h4, .status-box h4 {{
                font-size: 14px;
                margin: 0 0 10px 0;
                white-space: nowrap;
            }}
            .info-grid, .status-grid {{
                grid-template-columns: 1fr;
                gap: 10px;
            }}
            .info-item {{
                padding: 10px;
                border-radius: 6px;
            }}
            .info-label {{
                font-size: 10px;
                margin-bottom: 4px;
                white-space: nowrap;
            }}
            .info-value {{
                font-size: 13px;
                word-break: break-word;
            }}
            .info-value.highlight {{
                font-size: 18px;
            }}
            .info-value.duration {{
                font-size: 16px;
                word-break: break-word;
            }}
            .status-item {{
                padding: 10px;
                border-radius: 6px;
            }}
            .status-label {{
                font-size: 10px;
                margin-bottom: 2px;
                white-space: nowrap;
            }}
            .status-value {{
                font-size: 13px;
                word-break: break-word;
            }}
            .status-value.highlight {{
                font-size: 16px;
            }}
            .ban-details ul {{
                padding-left: 16px;
            }}
            .ban-details li {{
                font-size: 13px;
                line-height: 1.6;
            }}
            .footer {{
                padding: 14px 16px;
            }}
            .footer p {{
                font-size: 11px;
                margin: 2px 0;
                white-space: nowrap;
            }}
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <div class="card">
            <!-- 头部 -->
            <div class="header-ban">
                <h1>🔒 IP已被封禁</h1>
                <div class="subtitle">{system_name} - 安全防护</div>
            </div>
            
            <!-- 内容 -->
            <div class="content">
                <!-- 封禁横幅 -->
                <div class="ban-banner">
                    <div class="ban-icon">🚫</div>
                    <div class="ban-title">以下IP地址已被自动封禁</div>
                    <div class="ban-ip">{ip}</div>
                </div>
                
                <!-- 封禁详情 -->
                <div class="info-card">
                    <h3>📋 封禁详情</h3>
                    <div class="info-grid">
                        <div class="info-item">
                            <span class="info-label">封禁IP</span>
                            <span class="info-value">{ip}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">封禁原因</span>
                            <span class="info-value">{reason}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">封禁时长</span>
                            <span class="info-value duration">{ban_duration_str}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">触发时间</span>
                            <span class="info-value">{now}</span>
                        </div>
                    </div>
                </div>
                
                <!-- 触发条件 -->
                <div class="ban-details">
                    <h4>⚡ 触发条件</h4>
                    <div class="info-grid">
                        <div class="info-item">
                            <span class="info-label">监测时间窗口</span>
                            <span class="info-value">{window_seconds}秒</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">当前请求数</span>
                            <span class="info-value highlight">{current_count}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">请求阈值</span>
                            <span class="info-value">{threshold}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">超过阈值</span>
                            <span class="info-value highlight">{current_count - threshold if threshold > 0 else 0}</span>
                        </div>
                    </div>
                </div>
                
                <!-- 系统状态 -->
                <div class="status-box">
                    <h4>📊 系统状态概览</h4>
                    <div class="status-grid">
                        <div class="status-item">
                            <span class="status-label">封禁状态</span>
                            <span class="status-value">已生效</span>
                        </div>
                        <div class="status-item">
                            <span class="status-label">自动解封时间</span>
                            <span class="status-value">{ban_duration_str}后</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- 页脚 -->
            {footer_content}
        </div>
    </div>
</body>
</html>
"""
    
    # 纯文本内容
    text_content = f"""
{system_name} - IP已被封禁

封禁详情:
IP地址: {ip}
封禁原因: {reason}
封禁时长: {ban_duration_str}
触发时间: {now}

触发条件:
监测时间窗口: {window_seconds}秒
当前请求数: {current_count}
请求阈值: {threshold}
超过阈值: {current_count - threshold if threshold > 0 else 0}

系统状态:
封禁状态: 已生效
自动解封时间: {ban_duration_str}后

此邮件由 {system_name} 自动发送
"""
    
    return subject, html_content, text_content

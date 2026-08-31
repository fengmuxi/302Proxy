"""邮件提醒模块 - 当检测到IP请求异常时发送邮件通知管理员"""

import asyncio
import logging
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Set
from datetime import datetime

from config import EmailConfig
from email_templates import get_email_template, get_ban_email_template

logger = logging.getLogger(__name__)


class EmailNotifier:
    """邮件通知器"""

    def __init__(self, config: EmailConfig):
        self._config = config
        self._cooldown_cache: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    def update_config(self, config: EmailConfig) -> None:
        """更新配置"""
        self._config = config

    def _is_in_cooldown(self, ip: str) -> bool:
        """检查IP是否在冷却期内"""
        if ip not in self._cooldown_cache:
            return False
        cooldown_until = self._cooldown_cache[ip]
        return time.time() < cooldown_until

    def _set_cooldown(self, ip: str) -> None:
        """设置IP冷却期"""
        cooldown_seconds = self._config.alert_cooldown_minutes * 60
        self._cooldown_cache[ip] = time.time() + cooldown_seconds

    def _clean_cooldown_cache(self) -> None:
        """清理过期的冷却缓存"""
        current_time = time.time()
        expired_ips = [ip for ip, until in self._cooldown_cache.items() if current_time >= until]
        for ip in expired_ips:
            del self._cooldown_cache[ip]

    def _build_email_content(
        self,
        ip: str,
        alert_type: str,
        current_count: int,
        threshold: int,
        window_seconds: int,
    ) -> tuple:
        """构建邮件内容"""
        return get_email_template(
            ip=ip,
            alert_type=alert_type,
            current_count=current_count,
            threshold=threshold,
            window_seconds=window_seconds,
            system_name="代理监控系统",
            is_test=False,
        )

    def _build_ban_email_content(
        self,
        ip: str,
        reason: str,
        ban_duration_seconds: int,
        current_count: int,
        threshold: int,
        window_seconds: int,
    ) -> tuple:
        """构建封禁邮件内容"""
        return get_ban_email_template(
            ip=ip,
            reason=reason,
            ban_duration_seconds=ban_duration_seconds,
            current_count=current_count,
            threshold=threshold,
            window_seconds=window_seconds,
            system_name="代理监控系统",
        )

    def _get_from_header(self, config: Optional[EmailConfig] = None) -> str:
        """生成发件人头部，支持自定义名称（RFC 2047编码）"""
        from email.header import Header
        cfg = config or self._config
        if cfg.sender_name:
            # 对中文等非ASCII字符进行RFC 2047编码
            encoded_name = Header(cfg.sender_name, 'utf-8').encode()
            return f"{encoded_name} <{cfg.sender}>"
        return cfg.sender

    def _send_email_sync(
        self,
        subject: str,
        html_content: str,
        text_content: str,
    ) -> bool:
        """同步发送邮件"""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self._get_from_header()
            msg["To"] = self._config.recipients

            msg.attach(MIMEText(text_content, "plain", "utf-8"))
            msg.attach(MIMEText(html_content, "html", "utf-8"))

            recipients_list = [r.strip() for r in self._config.recipients.split(",") if r.strip()]

            # 根据端口号自动判断加密方式
            if self._config.smtp_port == 465:
                # 端口465：直接使用SSL加密
                server = smtplib.SMTP_SSL(self._config.smtp_host, self._config.smtp_port, timeout=30)
            elif self._config.smtp_port == 587:
                # 端口587：使用STARTTLS加密
                server = smtplib.SMTP(self._config.smtp_host, self._config.smtp_port, timeout=30)
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                # 其他端口：根据配置决定
                if self._config.smtp_ssl:
                    server = smtplib.SMTP_SSL(self._config.smtp_host, self._config.smtp_port, timeout=30)
                else:
                    server = smtplib.SMTP(self._config.smtp_host, self._config.smtp_port, timeout=30)
                    server.ehlo()
                    server.starttls()
                    server.ehlo()

            server.login(self._config.sender, self._config.password)
            server.sendmail(self._config.sender, recipients_list, msg.as_string())
            server.quit()
            
            logger.info(f"邮件发送成功: {subject}")
            return True
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False

    async def send_alert(
        self,
        ip: str,
        alert_type: str,
        current_count: int,
        threshold: int,
        window_seconds: int,
    ) -> bool:
        """发送异常提醒邮件"""
        if not self._config.enabled:
            return False

        async with self._lock:
            self._clean_cooldown_cache()
            
            if self._is_in_cooldown(ip):
                logger.debug(f"IP {ip} 在冷却期内，跳过邮件提醒")
                return False

            subject, html_content, text_content = self._build_email_content(
                ip, alert_type, current_count, threshold, window_seconds
            )
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(None, self._send_email_sync, subject, html_content, text_content)
            
            if success:
                self._set_cooldown(ip)
            
            return success

    async def send_ban_alert(
        self,
        ip: str,
        reason: str,
        ban_duration_seconds: int,
        current_count: int = 0,
        threshold: int = 0,
        window_seconds: int = 60,
    ) -> bool:
        """发送IP封禁提醒邮件（独立模板）"""
        if not self._config.enabled:
            return False

        async with self._lock:
            self._clean_cooldown_cache()
            
            if self._is_in_cooldown(ip):
                logger.debug(f"IP {ip} 在冷却期内，跳过封禁邮件提醒")
                return False

            subject, html_content, text_content = self._build_ban_email_content(
                ip, reason, ban_duration_seconds, current_count, threshold, window_seconds
            )
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(None, self._send_email_sync, subject, html_content, text_content)
            
            if success:
                self._set_cooldown(ip)
            
            return success

    def get_cooldown_status(self) -> Dict[str, float]:
        """获取冷却状态"""
        self._clean_cooldown_cache()
        current_time = time.time()
        return {ip: max(0, until - current_time) for ip, until in self._cooldown_cache.items()}

    def _build_test_email_content(self) -> tuple:
        """构建测试邮件内容"""
        subject = "【代理监控】邮件测试"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
        .header h1 {{ margin: 0; font-size: 20px; }}
        .content {{ background: #f9f9f9; padding: 20px; border: 1px solid #ddd; }}
        .info-row {{ margin: 10px 0; padding: 8px; background: white; border-radius: 4px; }}
        .label {{ font-weight: bold; color: #555; }}
        .value {{ color: #333; }}
        .success {{ color: #27ae60; font-weight: bold; }}
        .footer {{ text-align: center; padding: 15px; color: #888; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>✅ 邮件测试成功</h1>
        </div>
        <div class="content">
            <div class="info-row">
                <span class="label">状态：</span>
                <span class="value success">邮件服务配置正常</span>
            </div>
            <div class="info-row">
                <span class="label">发送时间：</span>
                <span class="value">{now}</span>
            </div>
            <div class="info-row">
                <span class="label">SMTP服务器：</span>
                <span class="value">{self._config.smtp_host}:{self._config.smtp_port}</span>
            </div>
            <div class="info-row">
                <span class="label">发件邮箱：</span>
                <span class="value">{self._config.sender}</span>
            </div>
        </div>
        <div class="footer">
            <p>此邮件由代理监控系统测试发送</p>
        </div>
    </div>
</body>
</html>
"""
        text_content = f"""
邮件测试成功

状态：邮件服务配置正常
发送时间：{now}
SMTP服务器：{self._config.smtp_host}:{self._config.smtp_port}
发件邮箱：{self._config.sender}

此邮件由代理监控系统测试发送
"""
        return subject, html_content, text_content

    def send_test_email_sync(self, config: Optional[EmailConfig] = None, template_type: str = "alert") -> tuple:
        """同步发送测试邮件，返回 (success, message)
        
        template_type: "alert" - 异常请求模板, "ban" - 封禁模板
        """
        test_config = config or self._config
        try:
            # 根据模板类型使用对应的模板，填充测试数据
            if template_type == "ban":
                subject, html_content, text_content = get_ban_email_template(
                    ip="192.168.1.100",
                    reason="请求频率超限",
                    ban_duration_seconds=3600,
                    current_count=150,
                    threshold=100,
                    window_seconds=60,
                    system_name="代理监控系统",
                )
            else:
                subject, html_content, text_content = get_email_template(
                    ip="192.168.1.100",
                    alert_type="请求频率超限",
                    current_count=150,
                    threshold=100,
                    window_seconds=60,
                    system_name="代理监控系统",
                    is_test=False,
                )
            
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self._get_from_header(test_config)
            msg["To"] = test_config.recipients

            msg.attach(MIMEText(text_content, "plain", "utf-8"))
            msg.attach(MIMEText(html_content, "html", "utf-8"))

            recipients_list = [r.strip() for r in test_config.recipients.split(",") if r.strip()]
            if not recipients_list:
                return False, "未配置收件邮箱"

            # 根据端口号自动判断加密方式
            if test_config.smtp_port == 465:
                # 端口465：直接使用SSL加密
                server = smtplib.SMTP_SSL(test_config.smtp_host, test_config.smtp_port, timeout=30)
            elif test_config.smtp_port == 587:
                # 端口587：使用STARTTLS加密
                server = smtplib.SMTP(test_config.smtp_host, test_config.smtp_port, timeout=30)
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                # 其他端口：根据配置决定
                if test_config.smtp_ssl:
                    server = smtplib.SMTP_SSL(test_config.smtp_host, test_config.smtp_port, timeout=30)
                else:
                    server = smtplib.SMTP(test_config.smtp_host, test_config.smtp_port, timeout=30)
                    server.ehlo()
                    server.starttls()
                    server.ehlo()

            server.login(test_config.sender, test_config.password)
            server.sendmail(test_config.sender, recipients_list, msg.as_string())
            server.quit()
            
            return True, "测试邮件发送成功"
        except smtplib.SMTPAuthenticationError:
            return False, "SMTP认证失败，请检查发件邮箱和密码/授权码"
        except smtplib.SMTPConnectError:
            return False, "无法连接到SMTP服务器，请检查服务器地址和端口"
        except smtplib.SMTPException as e:
            return False, f"SMTP错误：{str(e)}"
        except Exception as e:
            return False, f"发送失败：{str(e)}"

    async def send_test_email(self, config: Optional[EmailConfig] = None, template_type: str = "alert") -> tuple:
        """异步发送测试邮件"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.send_test_email_sync, config, template_type)

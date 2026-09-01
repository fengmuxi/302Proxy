/**
 * api.js - API请求和认证模块
 * 包含：API请求封装、认证逻辑、密码加密
 */

import { applyAuthState, setAuthError } from './components.js';
import { state } from './state.js';

// ===== RSA 密码加密功能 =====

let cachedPublicKey = null;

export async function getPublicKey() {
  if (cachedPublicKey) return cachedPublicKey;
  try {
    const result = await apiFetch("/_admin/api/auth/public-key");
    if (result && result.public_key) {
      cachedPublicKey = result.public_key;
      return cachedPublicKey;
    }
  } catch (e) {
    console.error("获取公钥失败:", e);
  }
  return null;
}

export async function encryptPassword(password, publicKeyPem) {
  if (!crypto?.subtle) return null;

  const pemHeader = "-----BEGIN PUBLIC KEY-----";
  const pemFooter = "-----END PUBLIC KEY-----";
  const pemContents = publicKeyPem
    .replace(pemHeader, "")
    .replace(pemFooter, "")
    .replace(/\s/g, "");

  const binaryString = atob(pemContents);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }

  const publicKey = await crypto.subtle.importKey(
    "spki",
    bytes.buffer,
    { name: "RSA-OAEP", hash: "SHA-256" },
    false,
    ["encrypt"]
  );

  const encoded = new TextEncoder().encode(password);
  const encrypted = await crypto.subtle.encrypt(
    { name: "RSA-OAEP" },
    publicKey,
    encoded
  );

  return btoa(String.fromCharCode(...new Uint8Array(encrypted)));
}

// ===== API 请求 =====

export async function apiFetch(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const text = await response.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { error: text };
    }
  }

  if (response.status === 401) {
    if (!url.includes("/_admin/api/auth/login")) {
      applyAuthState({
        enabled: true,
        authenticated: false,
        username: "",
      }, state);
      setAuthError(data.error || "登录状态已失效，请重新登录。");
    }
    throw new Error(data.error || "未登录或登录已失效。");
  }

  if (!response.ok) {
    throw new Error(data.error || data.message || text || "请求失败");
  }
  return data;
}

// ===== 认证相关 =====

export async function loadAuthStatus() {
  try {
    const data = await apiFetch("/_admin/api/auth/status");
    return data;
  } catch (error) {
    return { enabled: false, authenticated: false, username: "" };
  }
}

export async function submitLogin(state, showToast) {
  const username = document.getElementById("auth_username")?.value || "";
  const password = document.getElementById("auth_password")?.value || "";
  
  if (!username || !password) {
    setAuthError("请输入用户名和密码");
    return false;
  }

  try {
    const publicKey = await getPublicKey();
    let encryptedPassword = password;
    let encrypted = false;

    if (publicKey) {
      const result = await encryptPassword(password, publicKey);
      if (result) {
        encryptedPassword = result;
        encrypted = true;
      }
    }

    const data = await apiFetch("/_admin/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password: encryptedPassword, encrypted }),
    });
    
    setAuthError("");
    applyAuthState(data, state);
    showToast("登录成功");
    return true;
  } catch (error) {
    setAuthError(error.message);
    showToast(error.message, true);
    return false;
  }
}

export async function performLogout(state, showToast) {
  try {
    await apiFetch("/_admin/api/auth/logout", { method: "POST" });
    applyAuthState({ enabled: true, authenticated: false, username: "" }, state);
    showToast("已退出登录");
  } catch (error) {
    showToast(error.message, true);
  }
}

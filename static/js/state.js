/**
 * state.js - 全局共享状态模块
 */

export const state = {
  auth: {
    enabled: false,
    authenticated: false,
    username: "",
  },
  rules: [],
  routeGroups: [],
  geoSources: [],
  routeLogs: [],
  routeLogSettings: null,
  bannedIps: [],
  logFiles: [],
  backups: [],
  activeModule: "overview",
  logCurrentPage: 1,
  logTotalPages: 1,
  logPageSize: 10,
  banCurrentPage: 1,
  banTotalPages: 1,
  banPageSize: 20,
  routeFilter: { keyword: "", status: "", isDefault: "" },
  rulesFilter: { keyword: "", status: "", host: "" },
  logAutoScroll: true,
  logLastScrollTop: 0,
  logLastLineCount: 0,
  appLogFile: "",
  appLogKeyword: "",
};

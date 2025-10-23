function detectPlatform(): "win32" | "darwin" | "linux" | null {
  if (typeof process !== "undefined" && process.platform) {
    if (process.platform === "win32" || process.platform === "darwin" || process.platform === "linux") {
      return process.platform;
    }
  }
  if (typeof navigator !== "undefined") {
    const userAgent = navigator.userAgent.toLowerCase();
    if (userAgent.includes("windows")) {
      return "win32";
    }
    if (userAgent.includes("mac")) {
      return "darwin";
    }
    if (userAgent.includes("linux")) {
      return "linux";
    }
  }
  return null;
}

function resolveEnvPath(): string | null {
  const fromProcess = typeof process !== "undefined" ? process.env?.GRANITECTL_PATH : undefined;
  if (fromProcess && fromProcess.trim()) {
    return fromProcess.trim();
  }
  const fromImportMeta = typeof import.meta !== "undefined" ? (import.meta as Record<string, unknown>).env : undefined;
  if (fromImportMeta && typeof (fromImportMeta as Record<string, unknown>).GRANITECTL_PATH === "string") {
    const value = (fromImportMeta as Record<string, string>).GRANITECTL_PATH;
    if (value.trim()) {
      return value.trim();
    }
  }
  return null;
}

export function getDefaultGraniteCtlPath(): string {
  const platform = detectPlatform();
  const suffix = platform === "win32" ? ".exe" : "";
  return `../engine/granitectl${suffix}`;
}

export function getConfiguredGraniteCtlPath(): string | null {
  return resolveEnvPath();
}

export function resolveGraniteCtlPath(): string {
  return getConfiguredGraniteCtlPath() ?? getDefaultGraniteCtlPath();
}

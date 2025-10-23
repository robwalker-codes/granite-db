import { call, type Result } from "../tauri/gateway";

export interface GraniteCtlInfo {
  path: string;
  source: string;
  version?: string;
  exists: boolean;
  error?: string;
}

export async function fetchGraniteCtlInfo(): Promise<Result<GraniteCtlInfo>> {
  return call<GraniteCtlInfo>("granitectl_info");
}

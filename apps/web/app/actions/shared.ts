import { NextRequest } from "next/server.js";

import { requestOrigin } from "../../request-origin";
import { userFacingErrorMessage } from "../lib/api";

export function backToSettings(request: NextRequest) {
  const referer = request.headers.get("referer");
  return cleanActionUrl(request, referer || "/?view=settings");
}

export function appUrl(request: NextRequest, path: string) {
  return cleanActionUrl(request, path.startsWith("/") ? path : "/");
}

export function apiErrorMessage(error: unknown, fallback = "操作失败") {
  return userFacingErrorMessage(error, fallback);
}

export function actionErrorUrl(request: NextRequest, target: string | URL, error: unknown, fallback = "操作失败") {
  const url = new URL(target, request.url);
  url.searchParams.set("action_error", apiErrorMessage(error, fallback));
  return url;
}

export function cleanActionUrl(request: NextRequest, target: string | URL) {
  const internalUrl = new URL(request.url);
  const requestUrl = new URL(`${internalUrl.pathname}${internalUrl.search}`, `${requestOrigin(request)}/`);
  let url = new URL(target, requestUrl);
  if (url.origin !== requestUrl.origin) {
    url = new URL("/", requestUrl);
  }
  url.searchParams.delete("action_error");
  url.searchParams.delete("action_result");
  url.searchParams.delete("action_message");
  return url;
}

import { NextRequest, NextResponse } from "next/server.js";

import { requestOrigin } from "./request-origin";

const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

export function proxy(request: NextRequest) {
  if (SAFE_METHODS.has(request.method)) {
    return NextResponse.next();
  }

  const source = request.headers.get("origin") || request.headers.get("referer");
  try {
    if (!source || new URL(source).origin !== requestOrigin(request)) {
      return new NextResponse("Forbidden", { status: 403 });
    }
  } catch {
    return new NextResponse("Forbidden", { status: 403 });
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/actions/:path*", "/api/:path*"]
};

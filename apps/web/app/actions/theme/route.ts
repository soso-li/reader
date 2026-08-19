import { NextRequest, NextResponse } from "next/server";

import { cleanActionUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const theme = String(form.get("theme") ?? "system");
  const target = cleanActionUrl(request, request.headers.get("referer") || "/?view=settings");
  const response = NextResponse.redirect(target, 303);

  if (theme === "light" || theme === "dark") {
    response.cookies.set("reader-theme", theme, {
      path: "/",
      maxAge: 60 * 60 * 24 * 365,
      sameSite: "lax"
    });
  } else {
    response.cookies.set("reader-theme", "", {
      path: "/",
      maxAge: 0,
      sameSite: "lax"
    });
  }

  return response;
}

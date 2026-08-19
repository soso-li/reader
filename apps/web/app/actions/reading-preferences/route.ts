import { NextRequest, NextResponse } from "next/server";

import { cleanActionUrl } from "../shared";
import { normalizeReadingPreference } from "../../reading-preferences";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const name = String(form.get("name") ?? "");
  const value = normalizeReadingPreference(name, String(form.get("value") ?? ""));
  const target = cleanActionUrl(request, request.headers.get("referer") || "/?view=settings&settings_section=general");
  const response = NextResponse.redirect(target, 303);

  if (value) {
    response.cookies.set(name, value, {
      path: "/",
      maxAge: 60 * 60 * 24 * 365,
      sameSite: "lax"
    });
  }

  return response;
}

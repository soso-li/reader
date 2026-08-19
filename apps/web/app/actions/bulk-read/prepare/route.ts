import { NextRequest, NextResponse } from "next/server";

import { apiFetch, userFacingErrorMessage } from "../../../lib/api";
import type { BulkReadPrepared, BulkReadScope } from "../../../bulk-read";

export async function POST(request: NextRequest) {
  try {
    const scope = (await request.json()) as BulkReadScope;
    const prepared = await apiFetch<BulkReadPrepared>(
      "/user-state/bulk-read/prepare",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(scope)
      }
    );
    return NextResponse.json(prepared);
  } catch (error) {
    return NextResponse.json(
      { error: userFacingErrorMessage(error, "准备批量已读失败") },
      { status: 400 }
    );
  }
}

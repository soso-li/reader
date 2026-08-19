import { createHash } from "node:crypto";

import sharp from "sharp";

import { apiRequest } from "../../lib/api";

export const runtime = "nodejs";

const MAX_IMAGE_BYTES = 12 * 1024 * 1024;
const TRIM_THRESHOLD = 32;
const SAFE_IMAGE_TYPES = new Set(["image/avif", "image/gif", "image/jpeg", "image/png", "image/webp"]);

export async function GET(request: Request) {
  const src = new URL(request.url).searchParams.get("src");
  if (!src) return new Response("缺少图片地址", { status: 400 });

  let imageUrl: URL;
  try {
    imageUrl = new URL(src);
  } catch {
    return new Response("图片地址无效", { status: 400 });
  }

  if (imageUrl.protocol !== "http:" && imageUrl.protocol !== "https:") {
    return new Response("不支持的图片地址", { status: 400 });
  }

  try {
    const cacheKey = createHash("sha256").update(src).digest("hex");
    const upstream = await apiRequest(`/images/article/${cacheKey}`, {
      headers: { "X-Reader-Image-Source": src },
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(3_000)])
    });
    if (!upstream.ok) {
      await upstream.body?.cancel();
      return unavailableImage();
    }

    let contentType = upstream.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() || "image/jpeg";
    if (!SAFE_IMAGE_TYPES.has(contentType) && contentType !== "application/octet-stream") return unavailableImage();

    const buffer = await readLimitedBody(upstream, MAX_IMAGE_BYTES);
    if (!buffer) return new Response("图片超过 12MB 限制", { status: 413 });
    if (contentType === "application/octet-stream") {
      const detectedContentType = contentTypeForFormat((await sharp(buffer, { animated: false }).metadata()).format);
      if (!detectedContentType) return unavailableImage();
      contentType = detectedContentType;
    }

    try {
      return await trimLightBorder(buffer, contentType);
    } catch {
      return imageResponse(buffer, contentType);
    }
  } catch {
    return unavailableImage();
  }
}

async function readLimitedBody(response: Response, limit: number) {
  const declaredLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > limit) {
    await response.body?.cancel();
    return null;
  }
  if (!response.body) return Buffer.alloc(0);

  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let length = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > limit) {
      await reader.cancel();
      return null;
    }
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks, length);
}

function unavailableImage() {
  return new Response("图片不可用", { status: 502 });
}

async function trimLightBorder(buffer: Buffer, contentType: string) {
  const input = sharp(buffer, { animated: false });
  const metadata = await input.metadata();
  if (!metadata.width || !metadata.height) return imageResponse(buffer, contentType);

  // ponytail: trims only obvious light image mattes; add per-source rules if real artwork gets cropped.
  const corner = await sharp(buffer, { animated: false }).ensureAlpha().extract({ left: 0, top: 0, width: 1, height: 1 }).raw().toBuffer();
  if (!isLightOrTransparent(corner)) return imageResponse(buffer, contentType);

  const trimmed = await sharp(buffer, { animated: false }).trim({ threshold: TRIM_THRESHOLD }).toBuffer({ resolveWithObject: true });
  const widthRatio = trimmed.info.width / metadata.width;
  const heightRatio = trimmed.info.height / metadata.height;
  const trimmedEnough = widthRatio <= 0.96 || heightRatio <= 0.96;
  const stillUseful = widthRatio >= 0.45 && heightRatio >= 0.45;

  if (!trimmedEnough || !stillUseful) return imageResponse(buffer, contentType);
  return imageResponse(trimmed.data, contentTypeForFormat(trimmed.info.format) || contentType);
}

function isLightOrTransparent(pixel: Buffer) {
  const [r, g, b, a] = pixel;
  return a < 24 || (a > 230 && r > 235 && g > 235 && b > 235);
}

function imageResponse(body: Buffer, contentType: string) {
  return new Response(new Uint8Array(body), {
    headers: {
      "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
      "Content-Type": contentType,
      "X-Content-Type-Options": "nosniff"
    }
  });
}

function contentTypeForFormat(format: string | undefined) {
  if (format === "jpg" || format === "jpeg") return "image/jpeg";
  if (format === "webp") return "image/webp";
  if (format === "gif") return "image/gif";
  if (format === "png") return "image/png";
  if (format === "avif") return "image/avif";
  return null;
}

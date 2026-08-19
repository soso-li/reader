type OriginRequest = {
  url: string;
};

export function requestOrigin(request: OriginRequest) {
  const configuredUrl = process.env.READER_DEPLOY_URL?.trim();
  return new URL(configuredUrl || request.url).origin;
}

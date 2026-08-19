export type UrlValues = Record<string, string | number | boolean | null | undefined>;

export function queryString(values: UrlValues) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== null && value !== undefined && value !== "") params.set(key, String(value));
  }
  return params.toString();
}

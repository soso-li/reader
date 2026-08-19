export function displaySourceName(value: string) {
  const name = value.trim();
  if (!/^https?:\/\//i.test(name)) return name;
  try {
    return new URL(name).hostname.replace(/^www\./i, "") || name;
  } catch {
    return name;
  }
}

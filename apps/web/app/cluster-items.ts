type TimedClusterItem = { id: number; published_at: string | null };

export function clusterItemsByTime<T extends TimedClusterItem>(items: T[] | undefined) {
  return [...(items ?? [])].sort((a, b) => {
    const aTime = a.published_at ? Date.parse(a.published_at) : Number.NaN;
    const bTime = b.published_at ? Date.parse(b.published_at) : Number.NaN;
    const aValid = Number.isFinite(aTime);
    const bValid = Number.isFinite(bTime);
    if (aValid && bValid && aTime !== bTime) return aTime - bTime;
    if (aValid !== bValid) return aValid ? -1 : 1;
    return a.id - b.id;
  });
}

import { readFile } from 'node:fs/promises';
import path from 'node:path';

const cache = new Map();

export function clearManifestCache() {
  cache.clear();
}

export async function load(coldStorageRoot, tenantId) {
  if (cache.has(tenantId)) return cache.get(tenantId);

  const manifestPath = path.join(coldStorageRoot, tenantId, 'manifest.json');
  const data = JSON.parse(await readFile(manifestPath, 'utf-8'));
  cache.set(tenantId, data);
  return data;
}

export function filesInRange(manifest, startTs, endTs) {
  return manifest.files.filter((f) => f.endTs >= startTs && f.startTs <= endTs);
}

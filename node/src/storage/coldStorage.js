import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';

export class ColdStorage {
  constructor(rootDir) {
    this.rootDir = rootDir;
  }

  resolve(relativePath) {
    return path.join(this.rootDir, relativePath);
  }

  async getManifest(tenantId) {
    const p = path.join(this.rootDir, tenantId, 'manifest.json');
    return JSON.parse(await readFile(p, 'utf-8'));
  }

  async getBloom(tenantId, segmentName) {
    const p = path.join(this.rootDir, tenantId, '_bloom', `${segmentName}.bloom.json`);
    return JSON.parse(await readFile(p, 'utf-8'));
  }

  async listTenants() {
    const entries = await readdir(this.rootDir, { withFileTypes: true });
    return entries.filter((e) => e.isDirectory() && e.name.startsWith('tenant-')).map((e) => e.name);
  }
}

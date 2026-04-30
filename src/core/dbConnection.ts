import fs from 'node:fs';
import path from 'node:path';
import { open, type Database } from 'sqlite';
import sqlite3 from 'sqlite3';
import { initSchema } from './dbSchema.js';

let dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>> | null = null;

function getDbPath(): string {
  const dir = path.join(process.cwd(), 'data');
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'app.db');
}

export async function getDb(): Promise<Database<sqlite3.Database, sqlite3.Statement>> {
  if (!dbPromise) {
    dbPromise = open({
      filename: getDbPath(),
      driver: sqlite3.Database
    });
  }

  const db = await dbPromise;
  await initSchema(db);
  return db;
}

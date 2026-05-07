import { renameSync, existsSync, rmSync } from "node:fs";
import path from "node:path";
import type { RecordingRow, PlaudRawRecording } from "@applaud/shared";
import { getDb, rowToRecording, type RecordingDbRow } from "../db.js";
import { folderName, recordingPaths } from "./layout.js";
import { loadConfig } from "../config.js";
import { logger } from "../logger.js";
import { emit } from "./events.js";

/** Soft-deleted recordings are purged from disk and DB after this delay. */
export const SOFT_DELETE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

function mainListWhereSql(): string {
  if (loadConfig().importPlaudDeleted) {
    return "user_deleted_at IS NULL";
  }
  return "user_deleted_at IS NULL AND is_trash = 0";
}

/** Rows that still need cloud asset work (used by the poller). */
function assetWorkWhereSql(): string {
  const cfg = loadConfig();
  const trashOk = cfg.importPlaudDeleted ? "" : " AND is_trash = 0";
  // Plaud-trashed files (is_trash=1): only sync audio when import is enabled — never transcript/summary.
  return `user_deleted_at IS NULL${trashOk}
       AND (
         audio_downloaded_at IS NULL
         OR (is_trash = 0 AND (
              transcript_downloaded_at IS NULL
              OR (summary_downloaded_at IS NULL AND plaud_is_summary = 1)
            ))
       )`;
}

export function isSyncIgnoredId(id: string): boolean {
  const row = getDb()
    .prepare<[string], { c: number }>("SELECT 1 AS c FROM sync_ignore WHERE id = ?")
    .get(id);
  return (row?.c ?? 0) > 0;
}

export function clearSyncIgnore(): number {
  const r = getDb().prepare("DELETE FROM sync_ignore").run();
  return r.changes;
}

export interface SyncBlocklistRow {
  id: string;
  ignoredAt: number;
}

export function listSyncIgnoreRows(): SyncBlocklistRow[] {
  const rows = getDb()
    .prepare<[], { id: string; ignored_at: number }>(
      "SELECT id, ignored_at FROM sync_ignore ORDER BY ignored_at DESC",
    )
    .all();
  return rows.map((r) => ({ id: r.id, ignoredAt: r.ignored_at }));
}

export function upsertFromPlaud(item: PlaudRawRecording): RecordingRow {
  const db = getDb();
  const existing = db
    .prepare<[string], RecordingDbRow>("SELECT * FROM recordings WHERE id = ?")
    .get(item.id);
  if (existing) {
    if (existing.filename !== item.filename) {
      renameRecordingFolder(existing, item.filename);
    }
    const trashVal = item.is_trash ? 1 : 0;
    const summVal = item.is_summary ? 1 : 0;
    if (existing.is_trash !== trashVal || existing.plaud_is_summary !== summVal) {
      db.prepare("UPDATE recordings SET is_trash = ?, plaud_is_summary = ? WHERE id = ?").run(trashVal, summVal, item.id);
    }
    return rowToRecording(
      db.prepare<[string], RecordingDbRow>("SELECT * FROM recordings WHERE id = ?").get(item.id)!,
    );
  }

  const cfg = loadConfig();
  if (!cfg.recordingsDir) throw new Error("recordingsDir not configured");
  const folder = folderName(item.start_time, item.filename, item.id);
  const paths = recordingPaths(cfg.recordingsDir, folder);

  db.prepare(
    `INSERT INTO recordings (
      id, filename, start_time, end_time, duration_ms, filesize_bytes, serial_number,
      folder, audio_path, transcript_path, summary_path, metadata_path,
      audio_downloaded_at, transcript_downloaded_at, webhook_audio_fired_at,
      webhook_transcript_fired_at, is_trash, last_error, metadata_json,
      user_deleted_at, user_purge_at, plaud_is_summary, summary_downloaded_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL, ?, NULL)`,
  ).run(
    item.id,
    item.filename,
    item.start_time,
    item.end_time,
    item.duration,
    item.filesize,
    item.serial_number,
    folder,
    paths.audioPath,
    paths.transcriptJsonPath,
    paths.summaryMdPath,
    paths.metadataPath,
    item.is_trash ? 1 : 0,
    item.is_summary ? 1 : 0,
  );

  return rowToRecording(
    db
      .prepare<[string], RecordingDbRow>("SELECT * FROM recordings WHERE id = ?")
      .get(item.id)!,
  );
}

function renameRecordingFolder(row: RecordingDbRow, newFilename: string): void {
  const cfg = loadConfig();
  if (!cfg.recordingsDir) return;

  const newFolder = folderName(row.start_time, newFilename, row.id);
  if (newFolder === row.folder) {
    getDb().prepare("UPDATE recordings SET filename = ? WHERE id = ?").run(newFilename, row.id);
    emit("recording_renamed", { recordingId: row.id });
    return;
  }

  const oldAbs = path.join(cfg.recordingsDir, row.folder);
  const newAbs = path.join(cfg.recordingsDir, newFolder);

  if (existsSync(oldAbs)) {
    renameSync(oldAbs, newAbs);
    logger.info({ id: row.id, oldFolder: row.folder, newFolder }, "renamed recording folder");
  }

  const newPaths = recordingPaths(cfg.recordingsDir, newFolder);
  getDb()
    .prepare(
      `UPDATE recordings
         SET filename = ?, folder = ?, audio_path = ?, transcript_path = ?, summary_path = ?, metadata_path = ?
       WHERE id = ?`,
    )
    .run(
      newFilename,
      newFolder,
      newPaths.audioPath,
      newPaths.transcriptJsonPath,
      newPaths.summaryMdPath,
      newPaths.metadataPath,
      row.id,
    );
  emit("recording_renamed", { recordingId: row.id });
}

export function markAudioDownloaded(id: string, sizeBytes: number): void {
  const now = Date.now();
  getDb()
    .prepare(
      "UPDATE recordings SET audio_downloaded_at = ?, filesize_bytes = ?, last_error = NULL WHERE id = ?",
    )
    .run(now, sizeBytes, id);
}

export function markTranscriptDownloaded(id: string, transcriptText?: string): void {
  const now = Date.now();
  getDb()
    .prepare(
      "UPDATE recordings SET transcript_downloaded_at = ?, transcript_text = ?, last_error = NULL WHERE id = ?",
    )
    .run(now, transcriptText ?? null, id);
}

export function markSummaryDownloaded(id: string): void {
  getDb()
    .prepare("UPDATE recordings SET summary_downloaded_at = ?, last_error = NULL WHERE id = ?")
    .run(Date.now(), id);
}

export function markWebhookFired(id: string, event: "audio_ready" | "transcript_ready"): void {
  const col = event === "audio_ready" ? "webhook_audio_fired_at" : "webhook_transcript_fired_at";
  getDb().prepare(`UPDATE recordings SET ${col} = ? WHERE id = ?`).run(Date.now(), id);
}

export function recordError(id: string, message: string): void {
  getDb()
    .prepare("UPDATE recordings SET last_error = ? WHERE id = ?")
    .run(message.slice(0, 500), id);
}

export function clearError(id: string): void {
  getDb().prepare("UPDATE recordings SET last_error = NULL WHERE id = ?").run(id);
}

export function listRecordingRows(
  opts: { limit?: number; offset?: number; search?: string; includeInactive?: boolean } = {},
): { total: number; totalBytes: number; items: RecordingRow[] } {
  const db = getDb();
  const limit = Math.min(opts.limit ?? 100, 500);
  const offset = Math.max(opts.offset ?? 0, 0);
  const search = opts.search?.trim();
  const activeOnly = !opts.includeInactive;

  let aggRow: { c: number; b: number } | undefined;
  let rows: RecordingDbRow[];
  if (search) {
    const like = `%${search}%`;
    const ml = mainListWhereSql();
    if (activeOnly) {
      aggRow = db
        .prepare<[string, string], { c: number; b: number }>(
          `SELECT COUNT(*) AS c, COALESCE(SUM(filesize_bytes), 0) AS b FROM recordings WHERE (${ml}) AND (filename LIKE ? OR transcript_text LIKE ?)`,
        )
        .get(like, like);
      rows = db
        .prepare<[string, string, number, number], RecordingDbRow>(
          `SELECT * FROM recordings WHERE (${ml}) AND (filename LIKE ? OR transcript_text LIKE ?) ORDER BY start_time DESC LIMIT ? OFFSET ?`,
        )
        .all(like, like, limit, offset);
    } else {
      aggRow = db
        .prepare<[string, string], { c: number; b: number }>(
          "SELECT COUNT(*) AS c, COALESCE(SUM(filesize_bytes), 0) AS b FROM recordings WHERE filename LIKE ? OR transcript_text LIKE ?",
        )
        .get(like, like);
      rows = db
        .prepare<[string, string, number, number], RecordingDbRow>(
          "SELECT * FROM recordings WHERE filename LIKE ? OR transcript_text LIKE ? ORDER BY start_time DESC LIMIT ? OFFSET ?",
        )
        .all(like, like, limit, offset);
    }
  } else if (activeOnly) {
    const ml = mainListWhereSql();
    aggRow = db
      .prepare<[], { c: number; b: number }>(
        `SELECT COUNT(*) AS c, COALESCE(SUM(filesize_bytes), 0) AS b FROM recordings WHERE ${ml}`,
      )
      .get();
    rows = db
      .prepare<[number, number], RecordingDbRow>(
        `SELECT * FROM recordings WHERE ${ml} ORDER BY start_time DESC LIMIT ? OFFSET ?`,
      )
      .all(limit, offset);
  } else {
    aggRow = db.prepare<[], { c: number; b: number }>("SELECT COUNT(*) AS c, COALESCE(SUM(filesize_bytes), 0) AS b FROM recordings").get();
    rows = db
      .prepare<[number, number], RecordingDbRow>(
        "SELECT * FROM recordings ORDER BY start_time DESC LIMIT ? OFFSET ?",
      )
      .all(limit, offset);
  }
  return {
    total: aggRow?.c ?? 0,
    totalBytes: aggRow?.b ?? 0,
    items: rows.map(rowToRecording),
  };
}

export function listSoftDeletedRows(
  opts: { limit?: number; offset?: number } = {},
): { total: number; totalBytes: number; items: RecordingRow[] } {
  const db = getDb();
  const limit = Math.min(opts.limit ?? 100, 500);
  const offset = Math.max(opts.offset ?? 0, 0);
  const aggRow = db
    .prepare<[], { c: number; b: number }>(
      "SELECT COUNT(*) AS c, COALESCE(SUM(filesize_bytes), 0) AS b FROM recordings WHERE user_deleted_at IS NOT NULL",
    )
    .get();
  const rows = db
    .prepare<[number, number], RecordingDbRow>(
      "SELECT * FROM recordings WHERE user_deleted_at IS NOT NULL ORDER BY user_deleted_at DESC LIMIT ? OFFSET ?",
    )
    .all(limit, offset);
  return {
    total: aggRow?.c ?? 0,
    totalBytes: aggRow?.b ?? 0,
    items: rows.map(rowToRecording),
  };
}

export function getRecordingById(id: string): RecordingRow | null {
  const row = getDb()
    .prepare<[string], RecordingDbRow>("SELECT * FROM recordings WHERE id = ?")
    .get(id);
  return row ? rowToRecording(row) : null;
}

/** Permanently remove the DB row (after files removed). Used by purge only. */
export function removeRecordingRow(id: string): void {
  getDb().prepare("DELETE FROM recordings WHERE id = ?").run(id);
}

export function softDeleteRecording(id: string): void {
  const now = Date.now();
  getDb()
    .prepare("UPDATE recordings SET user_deleted_at = ?, user_purge_at = ? WHERE id = ?")
    .run(now, now + SOFT_DELETE_RETENTION_MS, id);
}

export function restoreRecording(id: string): void {
  getDb()
    .prepare("UPDATE recordings SET user_deleted_at = NULL, user_purge_at = NULL WHERE id = ?")
    .run(id);
}

/** @returns false if disk removal was required and failed (DB row kept). */
function purgeOneSoftDeletedRow(row: RecordingDbRow, now: number): boolean {
  const cfg = loadConfig();
  if (cfg.recordingsDir) {
    const folderAbs = path.join(cfg.recordingsDir, row.folder);
    try {
      rmSync(folderAbs, { recursive: true, force: true });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error(
        { err, id: row.id, folderAbs },
        "purge: failed to remove folder; retaining DB row to avoid orphan data on disk",
      );
      getDb()
        .prepare("UPDATE recordings SET last_error = ? WHERE id = ?")
        .run(`Purge failed (disk): ${msg}`.slice(0, 500), row.id);
      return false;
    }
  }
  try {
    getDb().prepare("INSERT OR REPLACE INTO sync_ignore (id, ignored_at) VALUES (?, ?)").run(row.id, now);
  } catch (err) {
    logger.warn({ err, id: row.id }, "purge: failed to insert sync_ignore");
  }
  removeRecordingRow(row.id);
  logger.info({ id: row.id }, "soft-delete purge complete; id added to sync_ignore");
  return true;
}

export function purgeExpiredSoftDeletes(): void {
  const now = Date.now();
  const rows = getDb()
    .prepare<[number], RecordingDbRow>(
      "SELECT * FROM recordings WHERE user_deleted_at IS NOT NULL AND user_purge_at IS NOT NULL AND user_purge_at <= ?",
    )
    .all(now) as RecordingDbRow[];

  for (const row of rows) {
    purgeOneSoftDeletedRow(row, now);
  }
}

/** Permanently remove a soft-deleted row immediately (same as scheduled purge). */
export function purgeSoftDeletedRecordingNow(
  id: string,
): "ok" | "not_found" | "not_in_trash" | "disk_error" {
  const row = getDb()
    .prepare<[string], RecordingDbRow>("SELECT * FROM recordings WHERE id = ?")
    .get(id);
  if (!row) return "not_found";
  if (row.user_deleted_at == null) return "not_in_trash";
  return purgeOneSoftDeletedRow(row, Date.now()) ? "ok" : "disk_error";
}

export function countPendingTranscripts(): number {
  const row = getDb()
    .prepare<[], { c: number }>(
      `SELECT COUNT(*) AS c FROM recordings WHERE audio_downloaded_at IS NOT NULL AND transcript_downloaded_at IS NULL
       AND user_deleted_at IS NULL AND is_trash = 0`,
    )
    .get();
  return row?.c ?? 0;
}

export function countPendingSummaries(): number {
  const row = getDb()
    .prepare<[], { c: number }>(
      `SELECT COUNT(*) AS c FROM recordings WHERE audio_downloaded_at IS NOT NULL AND transcript_downloaded_at IS NOT NULL
       AND summary_downloaded_at IS NULL AND plaud_is_summary = 1 AND user_deleted_at IS NULL AND is_trash = 0`,
    )
    .get();
  return row?.c ?? 0;
}

export function countErrorsLast24h(): number {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  const row = getDb()
    .prepare<[number, number], { c: number }>(
      "SELECT COUNT(*) AS c FROM recordings WHERE last_error IS NOT NULL AND (audio_downloaded_at > ? OR transcript_downloaded_at > ?)",
    )
    .get(cutoff, cutoff);
  return row?.c ?? 0;
}

export function findRecordingsNeedingAssets(): RecordingRow[] {
  const w = assetWorkWhereSql();
  const rows = getDb()
    .prepare<[], RecordingDbRow>(`SELECT * FROM recordings WHERE ${w} ORDER BY start_time DESC`)
    .all();
  return rows.map(rowToRecording);
}

export function findRecentTranscribedRecordings(cutoffMs: number): RecordingRow[] {
  const rows = getDb()
    .prepare<[number], RecordingDbRow>(
      `SELECT * FROM recordings
       WHERE transcript_downloaded_at IS NOT NULL
         AND user_deleted_at IS NULL
         AND is_trash = 0
         AND start_time > ?
       ORDER BY start_time DESC`,
    )
    .all(cutoffMs);
  return rows.map(rowToRecording);
}

/** Throttled Plaud-trash rows to check for transcript/summary (does not drive pending counts). */
export const PLAUD_TRASH_ASSET_PROBE_INTERVAL_MS = 6 * 60 * 60 * 1000;
/** Max trash rows probed per scheduled poll (Phase 3). */
export const PLAUD_TRASH_ASSET_PROBE_SCHEDULED_LIMIT = 25;
/** Sync Now: higher than scheduled poll, but bounded so one manual sync cannot fan out without limit. */
export const PLAUD_TRASH_ASSET_PROBE_MANUAL_SYNC_LIMIT = 100;

export function findPlaudTrashAssetProbeCandidates(probeOlderThan: number, limit: number): RecordingRow[] {
  if (!loadConfig().importPlaudDeleted) return [];
  const rows = getDb()
    .prepare<[number, number], RecordingDbRow>(
      `SELECT * FROM recordings
       WHERE user_deleted_at IS NULL
         AND is_trash = 1
         AND audio_downloaded_at IS NOT NULL
         AND (
           transcript_downloaded_at IS NULL
           OR (summary_downloaded_at IS NULL AND plaud_is_summary = 1)
         )
         AND (trash_asset_probe_at IS NULL OR trash_asset_probe_at < ?)
       ORDER BY trash_asset_probe_at ASC, start_time DESC
       LIMIT ?`,
    )
    .all(probeOlderThan, limit);
  return rows.map(rowToRecording);
}

export function markTrashAssetProbed(id: string): void {
  getDb().prepare("UPDATE recordings SET trash_asset_probe_at = ? WHERE id = ?").run(Date.now(), id);
}

/** Clears probe timers so the next Phase-3 pass can consider all eligible Plaud-trash rows (e.g. after Sync Now). */
export function resetPlaudTrashAssetProbeTimestamps(): number {
  const r = getDb()
    .prepare(
      `UPDATE recordings SET trash_asset_probe_at = NULL
       WHERE user_deleted_at IS NULL
         AND is_trash = 1
         AND audio_downloaded_at IS NOT NULL
         AND (
           transcript_downloaded_at IS NULL
           OR (summary_downloaded_at IS NULL AND plaud_is_summary = 1)
         )`,
    )
    .run();
  return r.changes;
}

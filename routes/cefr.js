/**
 * routes/cefr.js — CEFR Vocabulary API for Sottotitoli
 * 
 * Add this file to your sottotitoli-websocket repo.
 * It provides REST endpoints for querying the Words-CEFR-Dataset SQLite DB.
 * 
 * Usage in your main server file:
 *   import cefrRouter from './routes/cefr.js';
 *   app.use('/api/cefr', cefrRouter);
 */

import { Router } from 'express';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const router = Router();

// ── Open DB once at startup (read-only, in-memory cache) ────────────────
const DB_PATH = join(__dirname, '..', 'word_cefr_minified.db');

let db;
try {
  db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
  console.log('[cefr] DB opened (readonly):', DB_PATH);
} catch (err) {
  console.error('[cefr] FATAL: Could not open CEFR database:', err.message);
  console.error('[cefr] Make sure word_cefr_minified.db exists in the repo root.');
  db = null;
}

// Performance pragmas (non-fatal — skip silently on readonly constraint)
try { if (db) db.pragma('journal_mode = WAL'); } catch (_) {}
try { if (db) db.pragma('cache_size = -64000'); } catch (_) {}

// ── Precompiled statements ──────────────────────────────────────────────

const stmtWord = db
  ? db.prepare(`
      SELECT w.word, pt.tag, pt.description AS pos_desc, wp.level,
             wp.frequency_count, w2.word AS lemma, w3.word AS stem
      FROM word_pos wp
      JOIN words w ON wp.word_id = w.word_id
      JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
      LEFT JOIN words w2 ON wp.lemma_word_id = w2.word_id
      LEFT JOIN words w3 ON w.stem_word_id = w3.word_id
      WHERE w.word = ?
      ORDER BY wp.frequency_count DESC
    `)
  : null;

const stmtCategories = db
  ? db.prepare(`SELECT * FROM categories ORDER BY category_title`)
  : null;

const stmtCategoryWords = db
  ? db.prepare(`
      SELECT w.word, pt.tag, wp.level, wp.frequency_count, c.category_title
      FROM word_pos wp
      JOIN words w ON wp.word_id = w.word_id
      JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
      JOIN word_categories wc ON wp.word_pos_id = wc.word_pos_id
      JOIN categories c ON wc.category_id = c.category_id
      WHERE c.category_id = ?
      ORDER BY wp.frequency_count DESC
      LIMIT 300
    `)
  : null;

const stmtWordFamily = db
  ? db.prepare(`
      SELECT w.word, pt.tag, wp.level, wp.frequency_count
      FROM words target
      JOIN words w ON (w.stem_word_id = target.stem_word_id OR w.word_id = target.stem_word_id)
      JOIN word_pos wp ON wp.word_id = w.word_id
      JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
      WHERE target.word = ? AND target.stem_word_id IS NOT NULL
      UNION ALL
      SELECT w.word, pt.tag, wp.level, wp.frequency_count
      FROM words target
      JOIN words w ON w.stem_word_id = target.word_id
      JOIN word_pos wp ON wp.word_id = w.word_id
      JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
      WHERE target.word = ? AND target.stem_word_id IS NULL
      ORDER BY wp.frequency_count DESC
    `)
  : null;

// ── GET /api/cefr/word?w=happy ──────────────────────────────────────────
router.get('/word', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });

  const word = (req.query.w || '').toLowerCase().trim();
  if (!word || word.length > 50) {
    return res.json({ found: false, word });
  }

  const rows = stmtWord.all(word);
  if (!rows.length) {
    return res.json({ found: false, word });
  }

  // Build response with all POS entries
  const results = rows.map(r => ({
    pos: r.tag,
    posDescription: r.pos_desc,
    level: r.level,
    cefr: ['', 'A1', 'A2', 'B1', 'B2', 'C1', 'C2'][Math.round(r.level)] || '?',
    frequency: r.frequency_count,
    lemma: r.lemma || null,
    stem: r.stem || null
  }));

  res.json({
    found: true,
    word: rows[0].word,
    results
  });
});

// ── POST /api/cefr/batch  { words: ["apple","happy"] } ──────────────────
router.post('/batch', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });

  const words = (req.body.words || [])
    .map(w => w.toLowerCase().trim())
    .filter(w => w && w.length <= 50);
  const uniqueWords = [...new Set(words)];

  if (!uniqueWords.length) return res.json({});

  // Dynamic query (safe: words are validated strings, not raw SQL)
  const placeholders = uniqueWords.map(() => '?').join(',');
  const stmt = db.prepare(`
    SELECT w.word, pt.tag, wp.level
    FROM word_pos wp
    JOIN words w ON wp.word_id = w.word_id
    JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
    WHERE w.word IN (${placeholders})
  `);
  const rows = stmt.all(...uniqueWords);

  // Group by word — pick highest-frequency POS entry per word
  const result = {};
  for (const r of rows) {
    if (!result[r.word]) {
      result[r.word] = { level: r.level, pos: r.tag };
    }
  }

  res.json(result);
});

// ── GET /api/cefr/categories ────────────────────────────────────────────
router.get('/categories', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });
  res.json(stmtCategories.all());
});

// ── GET /api/cefr/category/:id ──────────────────────────────────────────
router.get('/category/:id', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });

  const id = parseInt(req.params.id);
  if (isNaN(id) || id < 1) return res.status(400).json({ error: 'Invalid category ID' });

  const rows = stmtCategoryWords.all(id);
  res.json(rows);
});

// ── GET /api/cefr/word-family?lemma=happy ───────────────────────────────
router.get('/word-family', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });

  const lemma = (req.query.lemma || '').toLowerCase().trim();
  if (!lemma) return res.json([]);

  const rows = stmtWordFamily.all(lemma, lemma);
  res.json(rows);
});

// ── POST /api/cefr/analyze  { text: "..." } ─────────────────────────────
router.post('/analyze', (req, res) => {
  if (!db) return res.status(503).json({ error: 'Database not available' });

  const text = (req.body.text || '').trim();
  if (!text || text.length > 50000) {
    return res.status(400).json({ error: 'Text required, max 50K characters' });
  }

  // Tokenize
  const tokens = text.toLowerCase().match(/\b[a-z]+\b/g) || [];
  if (!tokens.length) {
    return res.json({ totalWords: 0, uniqueWords: 0, avgLevel: null, cefrBand: null,
                      levelDistribution: {}, topicDistribution: {}, coverage: '0%' });
  }

  const uniqueTokens = [...new Set(tokens)];

  // Batch lookup
  const placeholders = uniqueTokens.map(() => '?').join(',');
  const stmt = db.prepare(`
    SELECT w.word, pt.tag, wp.level, wp.frequency_count,
           GROUP_CONCAT(c.category_title, '||') AS topics
    FROM word_pos wp
    JOIN words w ON wp.word_id = w.word_id
    JOIN pos_tags pt ON wp.pos_tag_id = pt.tag_id
    LEFT JOIN word_categories wc ON wp.word_pos_id = wc.word_pos_id
    LEFT JOIN categories c ON wc.category_id = c.category_id
    WHERE w.word IN (${placeholders})
    GROUP BY wp.word_pos_id
  `);
  const rows = stmt.all(...uniqueTokens);

  // Build lookup
  const lookup = {};
  for (const r of rows) {
    if (!lookup[r.word] || r.frequency_count > (lookup[r.word].frequency_count || 0)) {
      lookup[r.word] = r;
    }
  }

  // Count levels
  const levelCounts = { A1: 0, A2: 0, B1: 0, B2: 0, C1: 0, C2: 0, unknown: 0 };
  const topicCounts = {};
  let levelSum = 0, levelCount = 0;

  for (const t of tokens) {
    const entry = lookup[t];
    if (entry && entry.level != null) {
      const lvl = Math.round(entry.level);
      const band = ['', 'A1', 'A2', 'B1', 'B2', 'C1', 'C2'][lvl] || 'unknown';
      levelCounts[band]++;
      levelSum += entry.level;
      levelCount++;
      if (entry.topics) {
        for (const cat of entry.topics.split('||')) {
          if (cat) topicCounts[cat] = (topicCounts[cat] || 0) + 1;
        }
      }
    } else {
      levelCounts.unknown++;
    }
  }

  const avgLevel = levelCount > 0 ? +(levelSum / levelCount).toFixed(2) : null;

  res.json({
    totalWords: tokens.length,
    uniqueWords: uniqueTokens.length,
    avgLevel,
    cefrBand: avgLevel ? ['', 'A1', 'A2', 'B1', 'B2', 'C1', 'C2'][Math.round(avgLevel)] : null,
    levelDistribution: levelCounts,
    topicDistribution: topicCounts,
    coverage: tokens.length > 0 ? Math.round((levelCount / tokens.length) * 100) + '%' : '0%',
    // Raw word data for client-side blending
    wordData: Object.fromEntries(
      Object.entries(lookup).map(([word, entry]) => [word, { level: entry.level, frequency: entry.frequency_count }])
    )
  });
});

export default router;

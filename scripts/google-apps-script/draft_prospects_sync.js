/**
 * Google Apps Script — Sync 2027 prospect values from the prospect-board Sheet to Supabase.
 *
 * Companion to dynasty_values_sync.js, but DO NOT confuse the two. Read this before pasting:
 *
 *   dynasty_values_sync.js  → NFL players.  Maps dan_id through players.player_id.
 *                             DELETES ALL of dynasty_values, then re-inserts.
 *   draft_prospects_sync.js → college prospects. Keys on draft_prospects.dan_id directly.
 *                             Upserts. Never deletes anything.
 *
 * Two reasons the dynasty script cannot be reused here:
 *   1. Prospects are NOT in `players` (player_id = Sportradar UUID, NFL only), so every
 *      dan_id lookup would miss and zero rows would sync.
 *   2. Its deleteAllDynastyValues() would still fire first, emptying the LIVE dynasty_values
 *      table that DTVC serves. Silent, total, and not obviously connected to the sheet you ran it from.
 *
 * SETUP (one-time):
 *   1. In the prospect-board Sheet, go to Extensions → Apps Script
 *   2. Paste this entire file into Code.gs (replace any existing content)
 *   3. Gear icon (Project Settings) → Script Properties → Add:
 *        - SUPABASE_URL → https://twfzcrodldvhpfaykasj.supabase.co
 *        - SUPABASE_KEY → (your SUPABASE_SERVICE_ROLE_KEY from .env)
 *   4. Save, close, and reload the Sheet
 *   5. A "Prospects" menu appears in the toolbar
 *
 * PREREQUISITE:
 *   scripts/draft_prospects/schema.sql must be applied first. This script only UPDATES rows
 *   that already exist; it will not create a prospect from scratch (that would insert a row
 *   with a NULL name and fail the NOT NULL constraint). Any dan_id in the sheet with no
 *   matching row is reported back to you rather than written.
 *
 * USAGE:
 *   - "Prospects" → "Sync Prospect Values" pushes Value / SF_Value for every row that has one
 *   - "Prospects" → "Preview Sync (no writes)" shows what would change, and writes nothing
 *   - First run asks for authorization — click through to allow
 *
 * ENTRY POINTS:
 *   Both public functions take no arguments and do exactly what their name says, so running
 *   either from the Apps Script editor's Run dropdown is safe. The shared implementation is
 *   runSync_(), whose trailing underscore makes it private: Apps Script hides it from the Run
 *   dropdown and refuses to invoke it from a menu or trigger. That is deliberate. It is the
 *   only function here that can write, and it requires an explicit boolean to do so, so there
 *   is no way to reach a live write by picking the wrong name off a list.
 *
 * SHEET FORMAT (only these three headers matter; column order is irrelevant):
 *   dan_id | Value | SF_Value
 */

// ── Config ──────────────────────────────────────────────────────────────────

var TABLE = 'draft_prospects';
var DRAFT_YEAR = 2027;

// ── Menu ────────────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Prospects')
    .addItem('Sync Prospect Values', 'syncProspectValues')
    .addItem('Preview Sync (no writes)', 'previewProspectValues')
    .addToUi();
}

// ── Entry points ────────────────────────────────────────────────────────────
// Both take no arguments. Neither can do the wrong thing if run from the editor.

function previewProspectValues() {
  runSync_(true);
}

function syncProspectValues() {
  runSync_(false);
}

// ── Main sync implementation ────────────────────────────────────────────────
// Private (trailing underscore): not offered in the Run dropdown, not callable from a
// menu item. dryRun must be passed explicitly, so it can never arrive as undefined.

function runSync_(dryRun) {
  if (dryRun !== true && dryRun !== false) {
    throw new Error('runSync_ requires an explicit boolean. Call previewProspectValues() or syncProspectValues().');
  }

  var props = PropertiesService.getScriptProperties();
  var supabaseUrl = props.getProperty('SUPABASE_URL');
  var supabaseKey = props.getProperty('SUPABASE_KEY');

  if (!supabaseUrl || !supabaseKey) {
    SpreadsheetApp.getUi().alert(
      'Missing Script Properties!\n\n' +
      'Go to Extensions → Apps Script → Project Settings → Script Properties\n' +
      'and add SUPABASE_URL and SUPABASE_KEY.'
    );
    return;
  }

  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var data = sheet.getDataRange().getValues();
  var headers = data[0].map(function(h) { return h.toString().trim(); });

  // Exact-match headers. Note the sheet may also carry lowercase sf_value / onqb_value
  // scaffold columns from the research export; indexOf is case-sensitive so the capitalized
  // Value / SF_Value that Dan fills in are the ones picked up here.
  var colIdx = {};
  ['dan_id', 'name', 'Value', 'SF_Value'].forEach(function(col) {
    colIdx[col] = headers.indexOf(col);
  });

  if (colIdx['dan_id'] < 0 || colIdx['Value'] < 0) {
    SpreadsheetApp.getUi().alert('Sheet must have "dan_id" and "Value" columns.');
    return;
  }

  // ── 1. Which prospects actually exist in the table? ─────────────────────
  var known = fetchProspectIds(supabaseUrl, supabaseKey);

  if (known.length === 0) {
    SpreadsheetApp.getUi().alert(
      'No rows found in ' + TABLE + '.\n\n' +
      'Apply scripts/draft_prospects/schema.sql (DDL + seed) before syncing values.'
    );
    return;
  }

  var knownSet = {};
  known.forEach(function(id) { knownSet[id] = true; });

  // ── 2. Build rows to upsert ─────────────────────────────────────────────
  var rows = [];
  var unmatched = [];
  var skipped = 0;
  var now = new Date().toISOString();

  for (var i = 1; i < data.length; i++) {
    var danId = data[i][colIdx['dan_id']];
    if (!danId) continue;
    danId = danId.toString().trim();

    var value = data[i][colIdx['Value']];
    var sfValue = colIdx['SF_Value'] >= 0 ? data[i][colIdx['SF_Value']] : '';

    // A row with neither value set is simply not ready yet, not an error.
    if (isBlank(value) && isBlank(sfValue)) {
      skipped++;
      continue;
    }

    if (!knownSet[danId]) {
      var nm = colIdx['name'] >= 0 ? data[i][colIdx['name']] : '';
      unmatched.push(nm + ' (dan_id=' + danId + ')');
      continue;
    }

    var row = {
      dan_id: danId,
      values_updated_at: now,
      updated_at: now
    };
    if (!isBlank(value)) row.value = Number(value);
    if (!isBlank(sfValue)) row.sf_value = Number(sfValue);

    rows.push(row);
  }

  if (rows.length === 0) {
    SpreadsheetApp.getUi().alert('No rows to sync (all blank or unmatched).');
    return;
  }

  // ── 3. Report and stop, if previewing ───────────────────────────────────
  var summary =
    'Would update: ' + rows.length + ' prospects\n' +
    'Blank (skipped): ' + skipped + '\n' +
    'Unmatched dan_ids: ' + unmatched.length +
    (unmatched.length ? '\n\n' + unmatched.slice(0, 20).join('\n') : '');

  if (dryRun) {
    SpreadsheetApp.getUi().alert('PREVIEW: nothing was written.\n\n' + summary);
    return;
  }

  // ── 4. Confirm before writing. Same numbers the preview reports, so the
  //       write path is never less informed than the dry run.
  var answer = SpreadsheetApp.getUi().alert(
    'Sync to Supabase?',
    summary + '\n\nWrite these to ' + TABLE + ' now?',
    SpreadsheetApp.getUi().ButtonSet.YES_NO
  );
  if (answer !== SpreadsheetApp.getUi().Button.YES) {
    SpreadsheetApp.getActiveSpreadsheet().toast('Cancelled. Nothing written.', 'Supabase Sync', 3);
    return;
  }

  // ── 5. Upsert. No delete step, by design: every dan_id here was verified
  //       to exist above, so merge-duplicates resolves to a plain UPDATE.
  var result = batchUpsert(supabaseUrl, supabaseKey, rows);
  var written = result.written;

  // ── 6. Surface failures loudly. A rejected POST must never be reported as
  //       success: the whole point of this script is that you can trust the toast.
  if (result.errors.length > 0) {
    var detail = result.errors[0];
    var hint = '';
    if (detail.indexOf('row-level security') >= 0 || detail.indexOf('42501') >= 0) {
      hint =
        '\n\nThat is a row-level security rejection. ' + TABLE + ' has only a SELECT policy, so ' +
        'reads succeed with any valid key but writes need the service_role key, which bypasses RLS.\n\n' +
        'Check the SUPABASE_KEY Script Property holds SUPABASE_SERVICE_ROLE_KEY from .env, ' +
        'not SUPABASE_ANON_KEY. They sit next to each other in the file.';
    } else if (detail.indexOf('Invalid API key') >= 0) {
      hint =
        '\n\nThe key was rejected outright. Note the project uses the newer Supabase key format, ' +
        'not a legacy eyJ... JWT. Take the value from .env, not the dashboard.';
    }
    SpreadsheetApp.getUi().alert(
      'Sync FAILED. ' + written + ' of ' + rows.length + ' written.\n\n' + detail + hint
    );
    Logger.log('Sync failed: ' + result.errors.join(' | '));
    return;
  }

  // ── 7. Report ───────────────────────────────────────────────────────────
  var msg = 'Synced ' + written + ' of ' + rows.length + ' prospect values to Supabase.';
  if (skipped > 0) {
    msg += '\n' + skipped + ' rows skipped (no value entered yet).';
  }
  if (unmatched.length > 0) {
    msg += '\n\n' + unmatched.length + ' dan_ids in the sheet are not in ' + TABLE + ':\n';
    msg += unmatched.slice(0, 20).join('\n');
    if (unmatched.length > 20) {
      msg += '\n... and ' + (unmatched.length - 20) + ' more';
    }
    msg += '\n\nAdd them to the seed in scripts/draft_prospects/schema.sql and re-apply.';
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Synced ' + written + ' prospect values.',
    'Supabase Sync',
    5
  );
  Logger.log(msg);

  if (unmatched.length > 0) {
    SpreadsheetApp.getUi().alert(msg);
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function isBlank(v) {
  return v === '' || v === null || v === undefined;
}

function fetchProspectIds(url, key) {
  /**
   * GET every dan_id in draft_prospects for this draft year.
   * Paginates at 1000/request, same as the dynasty script.
   */
  var ids = [];
  var offset = 0;
  var limit = 1000;

  while (true) {
    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/' + TABLE + '?select=dan_id&draft_year=eq.' + DRAFT_YEAR +
        '&offset=' + offset + '&limit=' + limit,
      {
        method: 'get',
        headers: {
          'apikey': key,
          'Authorization': 'Bearer ' + key
        },
        muteHttpExceptions: true
      }
    );

    if (resp.getResponseCode() !== 200) {
      throw new Error('Failed to fetch prospect ids: ' + resp.getContentText());
    }

    var batch = JSON.parse(resp.getContentText());
    if (batch.length === 0) break;

    batch.forEach(function(p) { ids.push(p.dan_id); });

    if (batch.length < limit) break;
    offset += limit;
  }

  return ids;
}

function batchUpsert(url, key, rows) {
  /**
   * POST in batches of 100 with merge-duplicates, resolving on the dan_id PK.
   * Rows may have differing keys (value present, sf_value absent, etc.), and PostgREST
   * requires a uniform payload shape per request, so normalize each batch to the union
   * of its keys with nulls filled in.
   *
   * Returns {written, errors}. Errors are RETURNED, not swallowed: an earlier version
   * only Logger.log'd them, which let a fully rejected sync still toast "success" while
   * writing nothing. The caller is responsible for surfacing them.
   */
  var batchSize = 100;
  var written = 0;
  var errors = [];

  for (var i = 0; i < rows.length; i += batchSize) {
    var batch = normalizeKeys(rows.slice(i, i + batchSize));

    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/' + TABLE + '?on_conflict=dan_id',
      {
        method: 'post',
        headers: {
          'apikey': key,
          'Authorization': 'Bearer ' + key,
          'Content-Type': 'application/json',
          'Prefer': 'return=minimal,resolution=merge-duplicates'
        },
        payload: JSON.stringify(batch),
        muteHttpExceptions: true
      }
    );

    var code = resp.getResponseCode();
    if (code >= 300) {
      var body = resp.getContentText();
      Logger.log('Error upserting batch at row ' + i + ': HTTP ' + code + ' ' + body);
      errors.push('HTTP ' + code + ': ' + body);
    } else {
      written += batch.length;
    }
  }

  return { written: written, errors: errors };
}

function normalizeKeys(batch) {
  /**
   * Give every object in the batch the same key set, so PostgREST accepts the array.
   * A missing value/sf_value becomes an explicit null, which is what we mean: the sheet
   * cleared it.
   */
  var keys = {};
  batch.forEach(function(r) {
    Object.keys(r).forEach(function(k) { keys[k] = true; });
  });
  var allKeys = Object.keys(keys);

  return batch.map(function(r) {
    var out = {};
    allKeys.forEach(function(k) {
      out[k] = r.hasOwnProperty(k) ? r[k] : null;
    });
    return out;
  });
}

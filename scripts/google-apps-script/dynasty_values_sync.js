/**
 * Google Apps Script — Sync dynasty values from this Google Sheet to Supabase.
 *
 * SETUP (one-time):
 *   1. In your Google Sheet, go to Extensions → Apps Script
 *   2. Paste this entire file into Code.gs (replace any existing content)
 *   3. Click the gear icon (Project Settings) → Script Properties → Add:
 *        - SUPABASE_URL    → https://twfzcrodldvhpfaykasj.supabase.co
 *        - SUPABASE_KEY    → (your SUPABASE_SERVICE_ROLE_KEY from .env)
 *   4. Save, close, and reload the Google Sheet
 *   5. You'll see a new "Supabase" menu in the toolbar
 *
 * USAGE:
 *   - Click "Supabase" → "Sync Dynasty Values" to push all values
 *   - First run will ask for authorization — click through to allow
 *
 * SHEET FORMAT (expected columns):
 *   dan_id | Player | team | Position | Rookie | Value | SF_Value
 */

// ── Menu ────────────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Supabase')
    .addItem('Sync Dynasty Values', 'syncDynastyValues')
    .addToUi();
}

// ── Main sync function ──────────────────────────────────────────────────────

function syncDynastyValues() {
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

  // Find column indices
  var colIdx = {};
  ['dan_id', 'Player', 'team', 'Position', 'Rookie', 'Value', 'SF_Value'].forEach(function(col) {
    colIdx[col] = headers.indexOf(col);
  });

  if (colIdx['dan_id'] < 0 || colIdx['Value'] < 0) {
    SpreadsheetApp.getUi().alert('Sheet must have "dan_id" and "Value" columns.');
    return;
  }

  // ── 1. Fetch dan_id → player_id mapping from Supabase ──────────────────
  var mapping = fetchDanIdMapping(supabaseUrl, supabaseKey);
  var mappingCount = Object.keys(mapping).length;

  if (mappingCount === 0) {
    SpreadsheetApp.getUi().alert(
      'No players have dan_id set in Supabase.\n\n' +
      'Run match_dan_ids.py first to bootstrap the mapping.'
    );
    return;
  }

  // ── 2. Build rows to upsert ─────────────────────────────────────────────
  var rows = [];
  var unmapped = [];
  var now = new Date().toISOString();

  for (var i = 1; i < data.length; i++) {
    var danId = data[i][colIdx['dan_id']];
    if (!danId) continue;
    danId = danId.toString().trim();

    var value = data[i][colIdx['Value']];
    if (value === '' || value === null || value === undefined) continue;

    var playerId = mapping[danId];
    if (!playerId) {
      var name = colIdx['Player'] >= 0 ? data[i][colIdx['Player']] : '';
      unmapped.push(name + ' (dan_id=' + danId + ')');
      continue;
    }

    var row = {
      player_id: playerId,
      value: Number(value),
      updated_at: now
    };

    var sfValue = colIdx['SF_Value'] >= 0 ? data[i][colIdx['SF_Value']] : '';
    if (sfValue !== '' && sfValue !== null && sfValue !== undefined) {
      row.sf_value = Number(sfValue);
    }

    rows.push(row);
  }

  if (rows.length === 0) {
    SpreadsheetApp.getUi().alert('No rows to sync (all unmatched or missing values).');
    return;
  }

  // ── 3. Delete existing dynasty_values, then insert ──────────────────────
  deleteAllDynastyValues(supabaseUrl, supabaseKey);
  var inserted = batchUpsert(supabaseUrl, supabaseKey, rows);

  // ── 4. Report ───────────────────────────────────────────────────────────
  var msg = 'Synced ' + inserted + ' of ' + rows.length + ' dynasty values to Supabase.';
  if (unmapped.length > 0) {
    msg += '\n\n' + unmapped.length + ' unmapped players (no dan_id match):\n';
    msg += unmapped.slice(0, 20).join('\n');
    if (unmapped.length > 20) {
      msg += '\n... and ' + (unmapped.length - 20) + ' more';
    }
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Synced ' + inserted + ' dynasty values.',
    'Supabase Sync',
    5
  );
  Logger.log(msg);

  if (unmapped.length > 0) {
    SpreadsheetApp.getUi().alert(msg);
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function fetchDanIdMapping(url, key) {
  /**
   * GET all players with dan_id set, return {dan_id: player_id} map.
   * Handles pagination (1000 rows per request).
   */
  var mapping = {};
  var offset = 0;
  var limit = 1000;

  while (true) {
    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/players?select=player_id,dan_id&dan_id=not.is.null&offset=' + offset + '&limit=' + limit,
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
      throw new Error('Failed to fetch player mapping: ' + resp.getContentText());
    }

    var batch = JSON.parse(resp.getContentText());
    if (batch.length === 0) break;

    batch.forEach(function(p) {
      mapping[p.dan_id] = p.player_id;
    });

    offset += limit;
  }

  return mapping;
}

function deleteAllDynastyValues(url, key) {
  /**
   * DELETE all rows from dynasty_values.
   * The filter "player_id=not.is.null" matches all rows (player_id is PK, never null).
   */
  var resp = UrlFetchApp.fetch(
    url + '/rest/v1/dynasty_values?player_id=not.is.null',
    {
      method: 'delete',
      headers: {
        'apikey': key,
        'Authorization': 'Bearer ' + key,
        'Prefer': 'return=minimal'
      },
      muteHttpExceptions: true
    }
  );

  if (resp.getResponseCode() >= 300) {
    throw new Error('Failed to delete existing values: ' + resp.getContentText());
  }
}

function batchUpsert(url, key, rows) {
  /**
   * POST rows in batches of 100 to avoid hitting request size limits.
   * All rows have identical keys so batch POST works.
   */
  var batchSize = 100;
  var inserted = 0;

  for (var i = 0; i < rows.length; i += batchSize) {
    var batch = rows.slice(i, i + batchSize);

    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/dynasty_values',
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

    if (resp.getResponseCode() >= 300) {
      Logger.log('Error inserting batch at row ' + i + ': ' + resp.getContentText());
    } else {
      inserted += batch.length;
    }
  }

  return inserted;
}

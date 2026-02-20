/**
 * Google Apps Script — Sync dynasty value change log to Supabase.
 *
 * SETUP (one-time):
 *   1. In your Change Log Google Sheet, go to Extensions → Apps Script
 *   2. Paste this entire file into Code.gs (replace any existing content)
 *   3. Click the gear icon (Project Settings) → Script Properties → Add:
 *        - SUPABASE_URL    → https://twfzcrodldvhpfaykasj.supabase.co
 *        - SUPABASE_KEY    → (your SUPABASE_SERVICE_ROLE_KEY from .env)
 *   4. Save, close, and reload the Google Sheet
 *   5. You'll see a new "Supabase" menu in the toolbar
 *
 * USAGE:
 *   - Click "Supabase" → "Sync Change Log" to push all history rows
 *   - First run will ask for authorization — click through to allow
 *
 * SHEET FORMAT (expected columns):
 *   Date | Player | Old | New | Comment
 *
 * Player names must match the dynasty values sheet (which maps to players in Supabase).
 */

// ── Menu ────────────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Supabase')
    .addItem('Sync Change Log', 'syncChangeLog')
    .addToUi();
}

// ── Main sync function ──────────────────────────────────────────────────────

function syncChangeLog() {
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
  ['Date', 'Player', 'Old', 'New', 'Comment'].forEach(function(col) {
    colIdx[col] = headers.indexOf(col);
  });

  if (colIdx['Player'] < 0 || colIdx['Date'] < 0) {
    SpreadsheetApp.getUi().alert('Sheet must have "Date" and "Player" columns.');
    return;
  }

  // ── 1. Fetch Player name → player_id mapping from Supabase ──────────────
  var nameMap = fetchPlayerNameMapping(supabaseUrl, supabaseKey);
  var nameCount = Object.keys(nameMap).length;

  if (nameCount === 0) {
    SpreadsheetApp.getUi().alert('No players found in Supabase.');
    return;
  }

  // ── 2. Build rows to insert ────────────────────────────────────────────
  var rows = [];
  var unmapped = [];

  for (var i = 1; i < data.length; i++) {
    var playerName = data[i][colIdx['Player']];
    var dateVal = data[i][colIdx['Date']];

    if (!playerName || !dateVal) continue;

    playerName = playerName.toString().trim();
    var normalized = normalizeName(playerName);
    var playerId = nameMap[normalized];

    if (!playerId) {
      if (unmapped.indexOf(playerName) === -1) {
        unmapped.push(playerName);
      }
      continue;
    }

    // Parse date — Sheets may give us a Date object or a string
    var dateStr;
    if (dateVal instanceof Date) {
      dateStr = formatDate(dateVal);
    } else {
      // Try parsing M/D/YYYY string
      var parts = dateVal.toString().split('/');
      if (parts.length === 3) {
        var d = new Date(parts[2], parts[0] - 1, parts[1]);
        dateStr = formatDate(d);
      } else {
        Logger.log('Bad date at row ' + (i + 1) + ': ' + dateVal);
        continue;
      }
    }

    var row = {
      player_id: playerId,
      date: dateStr
    };

    var oldVal = colIdx['Old'] >= 0 ? data[i][colIdx['Old']] : '';
    var newVal = colIdx['New'] >= 0 ? data[i][colIdx['New']] : '';
    var comment = colIdx['Comment'] >= 0 ? data[i][colIdx['Comment']] : '';

    if (oldVal !== '' && oldVal !== null && oldVal !== undefined) {
      row.old_value = Number(oldVal);
    }
    if (newVal !== '' && newVal !== null && newVal !== undefined) {
      row.new_value = Number(newVal);
    }
    if (comment) {
      row.comment = comment.toString().trim();
    }

    rows.push(row);
  }

  if (rows.length === 0) {
    SpreadsheetApp.getUi().alert('No rows to sync (all unmatched or empty).');
    return;
  }

  // ── 3. Delete existing history, then insert ────────────────────────────
  deleteAllHistory(supabaseUrl, supabaseKey);
  var inserted = batchInsert(supabaseUrl, supabaseKey, rows);

  // ── 4. Report ──────────────────────────────────────────────────────────
  var msg = 'Synced ' + inserted + ' of ' + rows.length + ' change log rows to Supabase.';
  if (unmapped.length > 0) {
    msg += '\n\n' + unmapped.length + ' unmatched players (not in Supabase):\n';
    msg += unmapped.slice(0, 20).join('\n');
    if (unmapped.length > 20) {
      msg += '\n... and ' + (unmapped.length - 20) + ' more';
    }
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Synced ' + inserted + ' change log rows.',
    'Supabase Sync',
    5
  );
  Logger.log(msg);

  if (unmapped.length > 0) {
    SpreadsheetApp.getUi().alert(msg);
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function normalizeName(name) {
  /** Lowercase, strip Jr/Sr/III/IV suffixes, remove punctuation. */
  if (!name) return '';
  name = name.toLowerCase().trim();
  name = name.replace(/\s+(jr\.?|sr\.?|ii|iii|iv|v)$/i, '');
  name = name.replace(/[."']/g, '');
  name = name.replace(/\s+/g, ' ');
  return name;
}

function formatDate(d) {
  /** Format a Date object as YYYY-MM-DD. */
  var year = d.getFullYear();
  var month = ('0' + (d.getMonth() + 1)).slice(-2);
  var day = ('0' + d.getDate()).slice(-2);
  return year + '-' + month + '-' + day;
}

function fetchPlayerNameMapping(url, key) {
  /**
   * GET all players from Supabase, return {normalized_name: player_id} map.
   * Handles pagination (1000 rows per request).
   */
  var mapping = {};
  var offset = 0;
  var limit = 1000;

  while (true) {
    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/players?select=player_id,first_name,last_name&offset=' + offset + '&limit=' + limit,
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
      throw new Error('Failed to fetch players: ' + resp.getContentText());
    }

    var batch = JSON.parse(resp.getContentText());
    if (batch.length === 0) break;

    batch.forEach(function(p) {
      var full = ((p.first_name || '') + ' ' + (p.last_name || '')).trim();
      mapping[normalizeName(full)] = p.player_id;
    });

    offset += limit;
  }

  return mapping;
}

function deleteAllHistory(url, key) {
  /**
   * DELETE all rows from dynasty_value_history.
   * The filter matches all rows (player_id is part of PK, never null).
   */
  var resp = UrlFetchApp.fetch(
    url + '/rest/v1/dynasty_value_history?player_id=not.is.null',
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
    throw new Error('Failed to delete existing history: ' + resp.getContentText());
  }
}

function batchInsert(url, key, rows) {
  /**
   * POST rows in batches of 100. All rows have identical keys so batch POST works.
   * Uses upsert (merge-duplicates) in case of duplicate (player_id, date) pairs.
   */
  var batchSize = 100;
  var inserted = 0;

  for (var i = 0; i < rows.length; i += batchSize) {
    var batch = rows.slice(i, i + batchSize);

    var resp = UrlFetchApp.fetch(
      url + '/rest/v1/dynasty_value_history',
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

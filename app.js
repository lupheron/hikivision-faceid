require('dotenv').config();
const express = require('express');
const axios = require('axios');
const Database = require('better-sqlite3');
const multer = require('multer');
const { google } = require('googleapis');

// ====== CONFIG — CHANGE THESE ======
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const ATTENDANCE_CHAT_ROUTING_JSON = process.env.ATTENDANCE_CHAT_ROUTING_JSON || '';
const PORT = Number(process.env.PORT || 8090);
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID || '';
const GOOGLE_SHEET_NAME = process.env.GOOGLE_SHEET_NAME || 'Events';
const GOOGLE_SHEET_SELECTED_ID = process.env.GOOGLE_SHEET_SELECTED_ID || '';
const GOOGLE_SHEET_SELECTED_NAME = process.env.GOOGLE_SHEET_SELECTED_NAME || 'Selected Team';
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || '';
const GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY = (process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY || '').replace(/\\n/g, '\n');
const DEBUG_WEBHOOKS = false;
const DIAG_EVENT_LINE = true;
const BUSINESS_TIMEZONE = 'Asia/Tashkent';
const BUSINESS_OFFSET = '+05:00';
const NO_SHOW_CHECK_INTERVAL_MS = 60000;
const BREAK_LIMIT_MIN = 60;
const ON_TIME_GRACE_MIN = 10;
const DIDNT_COME_AFTER_MIN = 120;
const VERY_LATE_AFTER_MIN = DIDNT_COME_AFTER_MIN + ON_TIME_GRACE_MIN;
const BOT_CHECKIN_NOTIFICATIONS_FROM_HHMM = '14:00';
// ===================================

// ====== TELEGRAM ATTENDANCE ROUTING (OPTIONAL) ======
// One bot token can send to multiple chats.
// Add groups here OR via ATTENDANCE_CHAT_ROUTING_JSON env var.
// Example:
// [
//   { chatId: '-1001111111111', employeeIds: ['001', '002'], employeeNames: ['Suxrob'] },
//   { chatId: '-1002222222222', employeeIds: ['003'], employeeNames: [] }
// ]
const ATTENDANCE_CHAT_ROUTING = [
    {
        chatId: '-1003963457390',
        employeeNames: [
            'Hasanboy',
            'Akbar Ramadan',
            'Farrux',
            'Lazizbek Leo',
            'Sardor',
            'Azimjon'
        ]
    }
];
// ====================================================

// ====== SHIFT SETUP (EDIT THIS BLOCK ONLY) ======
const SHIFT_RULES = {
    '5-2': {
        label: 'Shift 5-2',
        workStart: '17:00',
        workEnd: '02:00',
        validCheckInFrom: '13:00',
        validCheckInTo: '19:00',
        validCheckOutFrom: '01:50',
        validCheckOutTo: '10:00',
        checkOutDayOffset: 1,
        lateAllowableMin: 10
    },
    '6-3': {
        label: 'Shift 6-3',
        workStart: '18:00',
        workEnd: '03:00',
        validCheckInFrom: '14:00',
        validCheckInTo: '19:00',
        validCheckOutFrom: '02:50',
        validCheckOutTo: '11:00',
        checkOutDayOffset: 1,
        lateAllowableMin: 10
    },
    '7-4': {
        label: 'Shift 7-4',
        workStart: '19:00',
        workEnd: '04:00',
        validCheckInFrom: '15:00',
        validCheckInTo: '20:00',
        validCheckOutFrom: '03:50',
        validCheckOutTo: '12:00',
        checkOutDayOffset: 1,
        lateAllowableMin: 10
    }
};

// Map each employee to shift key.
// Example: '001': { name: 'Suxrob', shiftKey: '6-3' }
const EMPLOYEE_SHIFT_MAP = {
    '001': { name: 'Suxrob', shiftKey: '6-3' },
    '002': { name: 'Asadbek Odilov', shiftKey: '7-4' },
    '003': { name: 'Hasanboy', shiftKey: '5-2' },
    '004': { name: 'Akbar Ramadan', shiftKey: '5-2' },
    '0006': { name: 'Farrux', shiftKey: '5-2' },
    '7': { name: 'Fayzulloh Winston', shiftKey: '6-3' },
    '8': { name: 'Diyor Ethan', shiftKey: '6-3' },
    '9': { name: 'Fazliddin Fred', shiftKey: '6-3' },
    '10': { name: 'Asadbek Henry', shiftKey: '5-2' },
    '11': { name: 'Amirshoh Alex', shiftKey: '6-3' },
    '12': { name: 'Lazizbek Leo', shiftKey: '5-2' },
    '14': { name: 'Azizbek Tony', shiftKey: '5-2' },
    '19': { name: 'Jessica', shiftKey: '6-3' },
    '24': { name: 'Sardor', shiftKey: '5-2' },
    '27': { name: 'Nigora', shiftKey: '7-4' },
    '20': { name: 'Humidullo', shiftKey: '6-3' },
    '31': { name: 'Abdulloh', shiftKey: '6-3' },
    '28': { name: 'Azimjon', shiftKey: '5-2' },
    '32': { name: 'Zubayir', shiftKey: '6-3' },
    '036': { name: 'Odina', shiftKey: '6-3' }
};

// ====== EMPLOYEE SECRET KEYS ======
// Each employee uses their unique key in the Telegram bot to register and
// receive personal attendance notifications. Change 'mykey123' per employee.
const EMPLOYEE_SECRET_KEYS = {
    '001':  'spenceritdep',
    '002':  'isaac26@',
    '003':  'uks26@',
    '004':  'akbar26@',
    '0006': 'farrux26@',
    '7':    'fayzulloh26@',
    '8':    'diyor26@',
    '9':    'fazliddin26@',
    '10':   'asadbek26@',
    '11':   'amirshoh26@',
    '12':   'lazizbek26@',
    '14':   'azizbek26@',
    '19':   'jessica26@',
    '24':   'sardor26@',
    '27':   'nigora26@',
    '20':   'humidullo26@',
    '31':   'abdulloh26@',
    '28':   'azimjon26@',
    '32':   'zubayir26@',
    '036':  'odina26@'
};
// ==================================
// ================================================

const app = express();
const upload = multer();
const db = new Database('attendance.db');

db.exec(`
  CREATE TABLE IF NOT EXISTS attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id TEXT,
    employee_name TEXT,
    employee_gender TEXT,
    status TEXT,
    timestamp TEXT
  )
`);
db.exec(`
  CREATE TABLE IF NOT EXISTS daily_attendance (
    employee_id TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    shift_key TEXT NOT NULL,
    first_check_in_at TEXT,
    first_check_in_name TEXT,
    first_check_in_gender TEXT,
    first_check_in_late_min INTEGER,
    check_out_at TEXT,
    absent_notified INTEGER DEFAULT 0,
    PRIMARY KEY(employee_id, shift_date)
  )
`);
db.exec(`
  CREATE TABLE IF NOT EXISTS break_overtime_alerts (
    employee_id TEXT NOT NULL,
    break_out_at TEXT NOT NULL,
    alerted_at TEXT NOT NULL,
    PRIMARY KEY(employee_id, break_out_at)
  )
`);
db.exec(`
  CREATE TABLE IF NOT EXISTS registered_users (
    telegram_chat_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY(telegram_chat_id)
  )
`);
try {
    db.exec(`ALTER TABLE attendance ADD COLUMN employee_gender TEXT`);
} catch (err) {
    // Ignore duplicate column errors for existing databases.
    if (!String(err.message).toLowerCase().includes('duplicate column')) {
        throw err;
    }
}

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

async function sendTelegram(message) {
    if (!TELEGRAM_CHAT_ID) {
        console.warn('Telegram warning: TELEGRAM_CHAT_ID is empty, message skipped.');
        return;
    }
    try {
        await axios.post(
            `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
            { chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' }
        );
    } catch (err) {
        console.error('Telegram error:', err.message);
    }
}

function normalizeForCompare(value) {
    return String(value || '').trim().toLowerCase();
}

function safeArray(values) {
    if (!Array.isArray(values)) return [];
    return values.map((v) => String(v || '').trim()).filter(Boolean);
}

function parseRoutingGroupsFromEnv(raw) {
    if (!raw) return [];
    try {
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return parsed;
    } catch (err) {
        console.error('Routing parse error (ATTENDANCE_CHAT_ROUTING_JSON):', err.message);
        return [];
    }
}

function buildAttendanceRoutingGroups() {
    const merged = [
        ...(Array.isArray(ATTENDANCE_CHAT_ROUTING) ? ATTENDANCE_CHAT_ROUTING : []),
        ...parseRoutingGroupsFromEnv(ATTENDANCE_CHAT_ROUTING_JSON)
    ];
    return merged
        .map((group) => ({
            chatId: String(group?.chatId || '').trim(),
            employeeIds: safeArray(group?.employeeIds),
            employeeNames: safeArray(group?.employeeNames).map(normalizeForCompare)
        }))
        .filter((group) => group.chatId && (group.employeeIds.length > 0 || group.employeeNames.length > 0));
}

const attendanceRoutingGroups = buildAttendanceRoutingGroups();

function getMatchedAttendanceRoutingGroups(employeeId, employeeName) {
    const id = String(employeeId || '').trim();
    const name = normalizeForCompare(employeeName);
    return attendanceRoutingGroups.filter((group) => {
        const matchesId = id && group.employeeIds.includes(id);
        const matchesName = name && group.employeeNames.includes(name);
        return matchesId || matchesName;
    });
}

function resolveAttendanceChatIds(employeeId, employeeName) {
    const targetChatIds = new Set();

    for (const group of getMatchedAttendanceRoutingGroups(employeeId, employeeName)) {
        targetChatIds.add(group.chatId);
    }

    if (targetChatIds.size > 0) return Array.from(targetChatIds);
    // Fallback for non-routed employees is temporarily disabled.
    // Re-enable later by uncommenting the next line.
    return TELEGRAM_CHAT_ID ? [TELEGRAM_CHAT_ID] : [];
    return [];
}

function isSelectedAttendanceEmployee(employeeId, employeeName) {
    return getMatchedAttendanceRoutingGroups(employeeId, employeeName).length > 0;
}

function getGoogleSheetTargetForEmployee(employeeId, employeeName) {
    if (isSelectedAttendanceEmployee(employeeId, employeeName)) {
        return {
            spreadsheetId: GOOGLE_SHEET_SELECTED_ID || GOOGLE_SHEET_ID,
            sheetName: GOOGLE_SHEET_SELECTED_NAME
        };
    }
    return {
        spreadsheetId: GOOGLE_SHEET_ID,
        sheetName: GOOGLE_SHEET_NAME
    };
}

async function sendAttendanceTelegram(message, employeeId, employeeName) {
    const chatIds = resolveAttendanceChatIds(employeeId, employeeName);
    if (chatIds.length === 0) {
        console.warn(`Telegram warning: no target chat for ${employeeName || 'Unknown'} (${employeeId || 'Unknown'})`);
        return false;
    }
    let sentCount = 0;
    for (const chatId of chatIds) {
        try {
            await axios.post(
                `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
                { chat_id: chatId, text: message, parse_mode: 'HTML' }
            );
            sentCount += 1;
        } catch (err) {
            console.error(`Telegram error (chat ${chatId}):`, err.message);
        }
    }
    return sentCount > 0;
}

async function sendAttendanceTelegramByEmployeeId(message, employeeId) {
    const fallbackName = EMPLOYEE_SHIFT_MAP[String(employeeId)]?.name || '';
    return sendAttendanceTelegram(message, employeeId, fallbackName);
}

async function sendTelegramToChat(chatId, message) {
    if (!chatId) return;
    try {
        await axios.post(
            `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
            { chat_id: chatId, text: message, parse_mode: 'HTML' }
        );
    } catch (err) {
        console.error(`Telegram error (chat ${chatId}):`, err.message);
    }
}

// Send a personal DM to the employee if they have registered via the bot.
async function sendPersonalDm(employeeId, message) {
    if (!employeeId) return;
    const row = db.prepare('SELECT telegram_chat_id FROM registered_users WHERE employee_id = ?').get(String(employeeId));
    if (!row) return;
    await sendTelegramToChat(row.telegram_chat_id, message);
}

// ====== TELEGRAM BOT — PERSONAL REGISTRATION ======
// Tracks which users are waiting to enter their secret key after /start.
const pendingKeyEntry = new Set();
let pollingOffset = 0;

async function handleTelegramUpdate(update) {
    const msg = update.message || update.edited_message;
    if (!msg || !msg.text) return;

    const chatId = String(msg.chat.id);
    const text = msg.text.trim();

    if (text === '/start' || text.startsWith('/start ')) {
        pendingKeyEntry.add(chatId);
        await sendTelegramToChat(chatId,
            '👋 <b>Welcome to the Attendance Bot!</b>\n\n' +
            'To receive your personal attendance notifications, please enter your <b>secret key</b>:'
        );
        return;
    }

    if (text === '/mystatus') {
        const reg = db.prepare('SELECT employee_id FROM registered_users WHERE telegram_chat_id = ?').get(chatId);
        if (!reg) {
            await sendTelegramToChat(chatId, '❌ You are not registered yet.\n\nUse /start to register with your secret key.');
            return;
        }
        const empInfo = EMPLOYEE_SHIFT_MAP[reg.employee_id];
        const shiftInfo = empInfo ? SHIFT_RULES[empInfo.shiftKey] : null;
        await sendTelegramToChat(chatId,
            `✅ <b>You are registered!</b>\n\n` +
            `👤 Name: <b>${empInfo?.name || reg.employee_id}</b>\n` +
            `🆔 Employee ID: ${reg.employee_id}\n` +
            (shiftInfo ? `🏷 Shift: ${shiftInfo.label} (${shiftInfo.workStart}–${shiftInfo.workEnd})` : '')
        );
        return;
    }

    if (text === '/unregister') {
        const deleted = db.prepare('DELETE FROM registered_users WHERE telegram_chat_id = ?').run(chatId);
        if (deleted.changes > 0) {
            await sendTelegramToChat(chatId, '✅ You have been unregistered and will no longer receive personal notifications.');
        } else {
            await sendTelegramToChat(chatId, 'ℹ️ You were not registered.');
        }
        return;
    }

    // Secret key entry flow
    if (pendingKeyEntry.has(chatId)) {
        pendingKeyEntry.delete(chatId);
        const enteredKey = text;
        const matchedId = Object.entries(EMPLOYEE_SECRET_KEYS).find(([, key]) => key === enteredKey)?.[0];
        if (!matchedId) {
            await sendTelegramToChat(chatId,
                '❌ <b>Invalid key.</b>\n\nPlease ask your manager for the correct key, then use /start to try again.'
            );
            return;
        }
        db.prepare(`
            INSERT INTO registered_users (telegram_chat_id, employee_id, registered_at)
            VALUES (?, ?, ?)
            ON CONFLICT(telegram_chat_id) DO UPDATE SET
                employee_id = excluded.employee_id,
                registered_at = excluded.registered_at
        `).run(chatId, matchedId, new Date().toISOString());

        const empInfo = EMPLOYEE_SHIFT_MAP[matchedId];
        const shiftInfo = empInfo ? SHIFT_RULES[empInfo.shiftKey] : null;
        await sendTelegramToChat(chatId,
            `✅ <b>Registered successfully!</b>\n\n` +
            `👤 You are now linked as: <b>${empInfo?.name || matchedId}</b>\n` +
            (shiftInfo ? `🏷 Shift: ${shiftInfo.label} (${shiftInfo.workStart}–${shiftInfo.workEnd})\n` : '') +
            `\nYou will receive a personal message every time your attendance is recorded. ` +
            `Use /mystatus to check your registration or /unregister to remove it.`
        );
        console.log(`📲 Registered: ${empInfo?.name || matchedId} (${matchedId}) → chat ${chatId}`);
        return;
    }
}

async function startTelegramPolling() {
    if (!TELEGRAM_BOT_TOKEN) return;
    console.log('📲 Telegram bot polling started (personal registration active).');
    while (true) {
        try {
            const res = await axios.get(
                `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates`,
                { params: { offset: pollingOffset, timeout: 30, allowed_updates: ['message'] }, timeout: 35000 }
            );
            for (const update of (res.data.result || [])) {
                pollingOffset = update.update_id + 1;
                handleTelegramUpdate(update).catch((err) =>
                    console.error('Telegram update handler error:', err.message)
                );
            }
        } catch (err) {
            console.error('Telegram polling error:', err.message);
            await new Promise((r) => setTimeout(r, 5000));
        }
    }
}
// ==================================================

function normalizeGender(rawGender) {
    if (!rawGender) return 'Unknown';
    const value = String(rawGender).trim().toLowerCase();
    if (['male', 'm', 'man', '1'].includes(value)) return 'Male';
    if (['female', 'f', 'woman', '2', '0'].includes(value)) return 'Female';
    return 'Unknown';
}

function parseEventTime(evt) {
    const raw = evt.dateTime || evt.localTime || evt.sendTime || evt.time || evt.timestamp;
    if (!raw) return new Date();
    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function formatDateInZone(date) {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: BUSINESS_TIMEZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(date);
}

function formatDateTimeInZone(date) {
    return date.toLocaleString('en-GB', { timeZone: BUSINESS_TIMEZONE });
}

function addDaysToDateString(dateStr, days) {
    const [year, month, day] = dateStr.split('-').map(Number);
    const utcDate = new Date(Date.UTC(year, month - 1, day));
    utcDate.setUTCDate(utcDate.getUTCDate() + days);
    const yyyy = utcDate.getUTCFullYear();
    const mm = String(utcDate.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(utcDate.getUTCDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
}

function isSundayDateString(dateStr) {
    const [year, month, day] = String(dateStr).split('-').map(Number);
    const utcDate = new Date(Date.UTC(year, month - 1, day));
    return utcDate.getUTCDay() === 0;
}

function hhmmToMinutes(hhmm) {
    const [h, m] = String(hhmm).split(':').map(Number);
    return (h * 60) + m;
}

function getMinutesInZone(date) {
    const hhmm = new Intl.DateTimeFormat('en-GB', {
        timeZone: BUSINESS_TIMEZONE,
        hour12: false,
        hour: '2-digit',
        minute: '2-digit'
    }).format(date);
    return hhmmToMinutes(hhmm);
}

function makeShiftDateTime(shiftDate, hhmm, dayOffset = 0) {
    const datePart = addDaysToDateString(shiftDate, dayOffset);
    return new Date(`${datePart}T${hhmm}:00${BUSINESS_OFFSET}`);
}

function minutesBetween(later, earlier) {
    return Math.max(0, Math.round((later.getTime() - earlier.getTime()) / 60000));
}

function formatDuration(from, to) {
    const mins = Math.max(0, Math.round((to.getTime() - from.getTime()) / 60000));
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    return `${h}h ${m}m`;
}

function formatWorkedDuration(firstCheckInAtIso, checkOutAt, shiftDate, shift) {
    const shiftStart = makeShiftDateTime(shiftDate, shift.workStart, 0);
    let effectiveStart = shiftStart;

    if (firstCheckInAtIso) {
        const firstCheckInAt = new Date(firstCheckInAtIso);
        if (!Number.isNaN(firstCheckInAt.getTime()) && firstCheckInAt > shiftStart) {
            // Late arrival should reduce worked time.
            effectiveStart = firstCheckInAt;
        }
        // Early arrival should not increase worked time before shift start.
    }

    return formatDuration(effectiveStart, checkOutAt);
}

function getEmployeeShift(employeeId) {
    if (!employeeId) return null;
    return EMPLOYEE_SHIFT_MAP[String(employeeId)] || null;
}

function resolveShiftDateForEvent(eventTime, shift) {
    const today = formatDateInZone(eventTime);
    const yesterday = addDaysToDateString(today, -1);

    // Night-shift guard: after midnight but before next check-in window starts
    // should still belong to the previous shift date.
    if (shift.checkOutDayOffset > 0) {
        const nowMin = getMinutesInZone(eventTime);
        const checkInFromMin = hhmmToMinutes(shift.validCheckInFrom);
        if (nowMin < checkInFromMin) {
            const yesterdaySpanTo = makeShiftDateTime(yesterday, shift.validCheckOutTo, shift.checkOutDayOffset);
            if (eventTime <= yesterdaySpanTo) return yesterday;
        }
    }

    const candidates = [today, yesterday];
    for (const shiftDate of candidates) {
        const spanFrom = makeShiftDateTime(shiftDate, shift.validCheckInFrom, 0);
        const spanTo = makeShiftDateTime(shiftDate, shift.validCheckOutTo, shift.checkOutDayOffset);
        if (eventTime >= spanFrom && eventTime <= spanTo) return shiftDate;
    }
    return today;
}

function classifyPunch(eventTime, statusRaw, shift, shiftDate) {
    if (statusRaw === 'checkIn') return 'checkIn';
    if (statusRaw === 'checkOut') return 'checkOut';
    if (statusRaw === 'breakIn') return 'breakIn';
    if (statusRaw === 'breakOut') return 'breakOut';

    const checkInFrom = makeShiftDateTime(shiftDate, shift.validCheckInFrom, 0);
    const checkInTo = makeShiftDateTime(shiftDate, shift.validCheckInTo, 0);
    const checkOutFrom = makeShiftDateTime(shiftDate, shift.validCheckOutFrom, shift.checkOutDayOffset);
    const checkOutTo = makeShiftDateTime(shiftDate, shift.validCheckOutTo, shift.checkOutDayOffset);

    if (eventTime >= checkInFrom && eventTime <= checkInTo) return 'checkIn';
    if (eventTime >= checkOutFrom && eventTime <= checkOutTo) return 'checkOut';
    return 'access';
}

function getBreakDuration(employeeId) {
    const today = new Date().toISOString().slice(0, 10);
    const row = db.prepare(`
    SELECT timestamp FROM attendance
    WHERE employee_id = ? AND status = 'breakOut'
      AND DATE(timestamp) = ?
    ORDER BY timestamp DESC LIMIT 1
  `).get(employeeId, today);

    if (!row) return null;
    const diffMs = Date.now() - new Date(row.timestamp).getTime();
    const minutes = Math.floor(diffMs / 60000);
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return hours > 0 ? `${hours}h ${mins}m` : `${mins} minutes`;
}

const statusMap = {
    checkIn: { label: 'Check In', emoji: '✅' },
    checkOut: { label: 'Check Out', emoji: '🏁' },
    breakOut: { label: 'Break Out', emoji: '☕' },
    breakIn: { label: 'Break In', emoji: '🔙' }
};

const statusAliases = {
    checkin: 'checkIn',
    checkout: 'checkOut',
    breakin: 'breakIn',
    breakout: 'breakOut'
};
const recentEventCache = new Map();
let lastWebhookSnapshot = null;
let sheetsClientPromise = null;
const googleSheetHeaderEnsuredKeys = new Set();
const googleSheetTabEnsuredKeys = new Set();
const GOOGLE_SHEET_HEADER = [
    // You can rename these column titles as you like.
    'Time Local',
    'Employee id',
    'Employee Name',
    'Action',
    'Shift Time',
    'Shift Date',
    'Late Minutes',
    "Didn't Come"
];

function quoteSheetNameForRange(sheetName) {
    // Always quote sheet names to safely support spaces/non-English chars.
    const safe = String(sheetName || 'Sheet1').replace(/'/g, "''");
    return `'${safe}'`;
}

function getHeaderRange(sheetName) {
    const endCol = String.fromCharCode('A'.charCodeAt(0) + GOOGLE_SHEET_HEADER.length - 1);
    return `${quoteSheetNameForRange(sheetName)}!A1:${endCol}1`;
}

function buildSheetRow({
    timeLocal,
    employeeId,
    employeeName,
    action,
    shiftTime,
    shiftDate,
    lateMinutes,
    didntCome
}) {
    return [
        timeLocal || '',
        employeeId || 'unknown',
        employeeName || 'Unknown',
        action || '',
        shiftTime || '',
        shiftDate || '',
        Number.isFinite(lateMinutes) ? String(lateMinutes) : '',
        didntCome ? 'YES' : ''
    ];
}

function formatShiftTime(shift) {
    if (!shift) return '';
    return `${shift.workStart}-${shift.workEnd}`;
}

function isGoogleSheetsEnabled() {
    return Boolean(
        (GOOGLE_SHEET_ID || GOOGLE_SHEET_SELECTED_ID) &&
        GOOGLE_SERVICE_ACCOUNT_EMAIL &&
        GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY
    );
}

function buildSheetEnsureKey(spreadsheetId, sheetName) {
    return `${spreadsheetId}::${sheetName}`;
}

async function getGoogleSheetsClient() {
    if (!isGoogleSheetsEnabled()) return null;
    if (!sheetsClientPromise) {
        sheetsClientPromise = (async () => {
            const auth = new google.auth.JWT({
                email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
                key: GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY,
                scopes: ['https://www.googleapis.com/auth/spreadsheets']
            });
            await auth.authorize();
            return google.sheets({ version: 'v4', auth });
        })().catch((err) => {
            // Reset cached promise so future retries are possible.
            sheetsClientPromise = null;
            throw err;
        });
    }
    return sheetsClientPromise;
}

async function appendEventToGoogleSheet(rowValues, targetSheet = {}) {
    if (!isGoogleSheetsEnabled()) return;
    const spreadsheetId = targetSheet.spreadsheetId || GOOGLE_SHEET_ID;
    const sheetName = targetSheet.sheetName || GOOGLE_SHEET_NAME;
    if (!spreadsheetId) return;
    try {
        await ensureGoogleSheetHeader(spreadsheetId, sheetName);
        const sheets = await getGoogleSheetsClient();
        if (!sheets) return;
        await sheets.spreadsheets.values.append({
            spreadsheetId,
            range: `${quoteSheetNameForRange(sheetName)}!A2`,
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [rowValues]
            }
        });
    } catch (err) {
        console.error('Google Sheets append error:', err.message);
    }
}

async function ensureGoogleSheetHeader(spreadsheetId, sheetName) {
    if (!isGoogleSheetsEnabled() || !spreadsheetId || !sheetName) return;
    const key = buildSheetEnsureKey(spreadsheetId, sheetName);
    if (googleSheetHeaderEnsuredKeys.has(key)) return;
    try {
        await ensureGoogleSheetTab(spreadsheetId, sheetName);
        const sheets = await getGoogleSheetsClient();
        if (!sheets) return;
        const headerRes = await sheets.spreadsheets.values.get({
            spreadsheetId,
            range: getHeaderRange(sheetName)
        });
        const firstRow = headerRes.data.values && headerRes.data.values[0] ? headerRes.data.values[0] : [];
        const hasHeader = firstRow.join('|') === GOOGLE_SHEET_HEADER.join('|');
        if (!hasHeader) {
            await sheets.spreadsheets.values.update({
                spreadsheetId,
                range: getHeaderRange(sheetName),
                valueInputOption: 'RAW',
                requestBody: {
                    values: [GOOGLE_SHEET_HEADER]
                }
            });
        }
        googleSheetHeaderEnsuredKeys.add(key);
    } catch (err) {
        console.error('Google Sheets header setup error:', err.message);
    }
}

async function ensureGoogleSheetTab(spreadsheetId, sheetName) {
    if (!isGoogleSheetsEnabled() || !spreadsheetId || !sheetName) return;
    const key = buildSheetEnsureKey(spreadsheetId, sheetName);
    if (googleSheetTabEnsuredKeys.has(key)) return;
    try {
        const sheets = await getGoogleSheetsClient();
        if (!sheets) return;
        const meta = await sheets.spreadsheets.get({
            spreadsheetId,
            fields: 'sheets.properties.title'
        });
        const titles = (meta.data.sheets || []).map((s) => s.properties?.title).filter(Boolean);
        if (!titles.includes(sheetName)) {
            await sheets.spreadsheets.batchUpdate({
                spreadsheetId,
                requestBody: {
                    requests: [{ addSheet: { properties: { title: sheetName } } }]
                }
            });
            console.log(`📄 Created Google Sheet tab: ${sheetName}`);
        }
        googleSheetTabEnsuredKeys.add(key);
    } catch (err) {
        console.error('Google Sheets tab setup error:', err.message);
    }
}

function tryParseJson(raw) {
    if (typeof raw !== 'string') return raw;
    try {
        return JSON.parse(raw);
    } catch (err) {
        return raw;
    }
}

function extractXmlTag(xmlText, tagName) {
    if (typeof xmlText !== 'string') return null;
    const match = xmlText.match(new RegExp(`<${tagName}>([^<]*)</${tagName}>`, 'i'));
    return match ? match[1] : null;
}

function eventFromXml(xmlText) {
    if (typeof xmlText !== 'string' || !xmlText.includes('<')) return null;
    const employeeId =
        extractXmlTag(xmlText, 'employeeNoString') ||
        extractXmlTag(xmlText, 'employeeNo') ||
        extractXmlTag(xmlText, 'cardNo');
    const employeeName = extractXmlTag(xmlText, 'name') || extractXmlTag(xmlText, 'personName');
    const attendanceStatus =
        extractXmlTag(xmlText, 'attendanceStatus') ||
        extractXmlTag(xmlText, 'checkType') ||
        extractXmlTag(xmlText, 'status');
    const dateTime = extractXmlTag(xmlText, 'dateTime') || extractXmlTag(xmlText, 'localTime') || extractXmlTag(xmlText, 'sendTime');
    const majorEventType = extractXmlTag(xmlText, 'majorEventType');
    const minorEventType = extractXmlTag(xmlText, 'minorEventType');

    if (!employeeId && !employeeName && !attendanceStatus && !majorEventType && !minorEventType) return null;
    return {
        employeeNoString: employeeId,
        name: employeeName,
        attendanceStatus,
        dateTime,
        majorEventType,
        minorEventType
    };
}

function extractAccessEvent(data) {
    if (!data || typeof data !== 'object') return null;
    if (data.AccessControllerEvent) return tryParseJson(data.AccessControllerEvent);
    if (data.EventNotificationAlert && data.EventNotificationAlert.AccessControllerEvent) {
        return tryParseJson(data.EventNotificationAlert.AccessControllerEvent);
    }
    if (data.AcsEventInfo) return tryParseJson(data.AcsEventInfo);
    if (data.AcsEvent && data.AcsEvent.Info) return tryParseJson(data.AcsEvent.Info);
    if (data.event_log) {
        const parsed = tryParseJson(data.event_log);
        if (parsed && typeof parsed === 'object') return extractAccessEvent(parsed) || parsed.AccessControllerEvent || null;
    }
    if (typeof data.EventNotificationAlert === 'string') {
        const parsedAlert = tryParseJson(data.EventNotificationAlert);
        if (parsedAlert && typeof parsedAlert === 'object') return extractAccessEvent({ EventNotificationAlert: parsedAlert });
        return eventFromXml(data.EventNotificationAlert);
    }
    if (typeof data.AccessControllerEvent === 'string' && data.AccessControllerEvent.includes('<')) {
        return eventFromXml(data.AccessControllerEvent);
    }
    for (const [key, value] of Object.entries(data)) {
        if (typeof value === 'string' && (key.toLowerCase().includes('event') || key.toLowerCase().includes('alert'))) {
            const xmlEvent = eventFromXml(value);
            if (xmlEvent) return xmlEvent;
            const parsed = tryParseJson(value);
            if (parsed && typeof parsed === 'object') {
                const nested = extractAccessEvent(parsed);
                if (nested) return nested;
            }
        }
    }
    return null;
}

function normalizeAccessEventShape(evt) {
    if (!evt || typeof evt !== 'object') return evt;
    const nested = evt.AccessControllerEvent && typeof evt.AccessControllerEvent === 'object'
        ? evt.AccessControllerEvent
        : null;
    if (!nested) return evt;

    // Hikvision often wraps actual event details under AccessControllerEvent.
    // Keep outer metadata (dateTime/ip/etc) and merge inner details on top.
    return {
        ...evt,
        ...nested,
        dateTime: nested.dateTime || evt.dateTime,
        localTime: nested.localTime || evt.localTime,
        sendTime: nested.sendTime || evt.sendTime
    };
}

function isDuplicateEvent(evt, employeeId, normalizedStatus) {
    const eventKey = [
        employeeId || 'unknown',
        normalizedStatus || 'unknown',
        evt.dateTime || evt.localTime || evt.sendTime || evt.timestamp || ''
    ].join('|');
    const now = Date.now();
    const lastSeen = recentEventCache.get(eventKey);
    recentEventCache.set(eventKey, now);

    // Clean stale entries to keep memory tiny.
    for (const [key, ts] of recentEventCache) {
        if (now - ts > 120000) recentEventCache.delete(key);
    }

    return lastSeen && now - lastSeen < 15000;
}

async function handleEvent(data) {
    let evt = extractAccessEvent(data);
    if (!evt) {
        if (DEBUG_WEBHOOKS) {
            console.log('ℹ️ Received webhook without AccessControllerEvent. Top-level keys:', Object.keys(data || {}));
        }
        return;
    }

    if (typeof evt === 'string') {
        try {
            evt = JSON.parse(evt);
        } catch (e) {
            return;
        }
    }
    evt = normalizeAccessEventShape(evt);

    // Only care about events that have an employee attached (face/card auth)
    const employeeId =
        evt.employeeNoString ||
        evt.employeeNo ||
        evt.employeeID ||
        evt.cardNo ||
        evt.cardReaderNo ||
        evt.EmployeeInfo?.employeeNoString ||
        evt.EmployeeInfo?.employeeNo ||
        evt.UserInfo?.employeeNoString ||
        evt.UserInfo?.employeeNo ||
        evt.AccessControllerEvent?.employeeNoString ||
        evt.AccessControllerEvent?.employeeNo;
    const employeeName =
        evt.name ||
        evt.EmployeeInfo?.name ||
        evt.UserInfo?.name ||
        evt.personName ||
        evt.AccessControllerEvent?.name;
    const gender = normalizeGender(
        evt.gender ||
        evt.sex ||
        evt.personGender ||
        evt.employeeGender ||
        evt.EmployeeInfo?.gender ||
        evt.UserInfo?.gender
    );

    if (!employeeId && !employeeName) return; // silently drop door/heartbeat events

    if (DEBUG_WEBHOOKS) {
        console.log('\n========== EMPLOYEE EVENT ==========');
        console.log(JSON.stringify(evt, null, 2));
        console.log('====================================\n');
    }

    const statusRawOriginal =
        evt.attendanceStatus ||
        evt.status ||
        evt.checkType ||
        evt.AccessControllerEvent?.attendanceStatus ||
        evt.AccessControllerEvent?.status ||
        evt.AccessControllerEvent?.checkType;
    const statusRaw = statusAliases[String(statusRawOriginal || '').trim().toLowerCase()] || statusRawOriginal;
    const status = statusMap[statusRaw] || {
        label: statusRawOriginal || evt.minorEventType || evt.subEventType || evt.eventType || evt.label || 'Access Event',
        emoji: '📌'
    };
    const duplicateKeyStatus = statusRaw || status.label;
    if (DIAG_EVENT_LINE) {
        console.log(
            `📨 access-event | id=${employeeId || '-'} | name=${employeeName || '-'} | status=${statusRawOriginal || '-'} | major=${evt.majorEventType || '-'} | minor=${evt.minorEventType || evt.subEventType || '-'}`
        );
    }

    if (!employeeId && !employeeName) {
        if (DIAG_EVENT_LINE) {
            console.log('↳ ignored event without employee fields (likely non-attendance linkage)');
        }
        return;
    }

    if (isDuplicateEvent(evt, employeeId, duplicateKeyStatus)) {
        if (DEBUG_WEBHOOKS) {
            console.log(`↳ Duplicate event ignored for ${employeeId || 'unknown'} (${duplicateKeyStatus})`);
        }
        return;
    }
    const eventTime = parseEventTime(evt);
    const timeStr = formatDateTimeInZone(eventTime);
    const targetSheet = getGoogleSheetTargetForEmployee(employeeId, employeeName);
    const shiftInfo = getEmployeeShift(employeeId);
    const configuredShift = shiftInfo && SHIFT_RULES[shiftInfo.shiftKey] ? SHIFT_RULES[shiftInfo.shiftKey] : null;
    const shiftDate = configuredShift ? resolveShiftDateForEvent(eventTime, configuredShift) : formatDateInZone(eventTime);
    if (configuredShift && isSundayDateString(shiftDate)) {
        if (DIAG_EVENT_LINE) {
            console.log(`↳ sunday shift ignored | id=${employeeId || '-'} | name=${employeeName || '-'} | shiftDate=${shiftDate}`);
        }
        return;
    }
    const checkType = configuredShift ? classifyPunch(eventTime, statusRaw, configuredShift, shiftDate) : (statusRaw || 'access');

    db.prepare(`
      INSERT INTO attendance (employee_id, employee_name, employee_gender, status, timestamp)
      VALUES (?, ?, ?, ?, ?)
    `).run(employeeId || 'unknown', employeeName || 'unknown', gender, checkType, eventTime.toISOString());

    const baseMessage =
        `👤 Name: ${employeeName || 'Unknown'}\n` +
        `🆔 ID: ${employeeId || 'Unknown'}\n` +
        `🕒 Time: ${timeStr}`;

    if (checkType === 'breakIn') {
        // Break notifications are intentionally disabled for now.
        // const duration = getBreakDuration(employeeId);
        // let message = `🔙 <b>Break In</b>\n${baseMessage}`;
        // if (duration) message += `\n⏱ Break lasted: <b>${duration}</b>`;
        // await sendTelegram(message);
        // console.log(`✅ SENT: Break In — ${employeeName} (${employeeId})`);
        return;
    }
    if (checkType === 'breakOut') {
        // Break notifications are intentionally disabled for now.
        // await sendTelegram(`☕ <b>Break Out</b>\n${baseMessage}`);
        // console.log(`✅ SENT: Break Out — ${employeeName} (${employeeId})`);
        return;
    }

    if (!configuredShift) {
        // Unknown/unmapped users and generic access events are intentionally muted.
        // const fallbackStatus = statusMap[checkType] || status;
        // await sendTelegram(`${fallbackStatus.emoji} <b>${fallbackStatus.label}</b>\n${baseMessage}`);
        // console.log(`✅ SENT: ${fallbackStatus.label} — ${employeeName} (${employeeId})`);
        return;
    }

    const existingDay = db.prepare(`
      SELECT * FROM daily_attendance
      WHERE employee_id = ? AND shift_date = ?
    `).get(employeeId, shiftDate);

    if (checkType === 'checkIn') {
        const botCheckInNotificationsFromMin = hhmmToMinutes(BOT_CHECKIN_NOTIFICATIONS_FROM_HHMM);
        if (getMinutesInZone(eventTime) < botCheckInNotificationsFromMin) {
            if (DIAG_EVENT_LINE) {
                console.log(
                    `↳ early check-in ignored for bot notify | id=${employeeId || '-'} | name=${employeeName || '-'} | before=${BOT_CHECKIN_NOTIFICATIONS_FROM_HHMM}`
                );
            }
            return;
        }
        if (existingDay && existingDay.first_check_in_at) {
            return; // only first check-in should notify
        }
        const workStart = makeShiftDateTime(shiftDate, configuredShift.workStart, 0);
        const lateMin = minutesBetween(eventTime, workStart);
        const lateFlag = lateMin > ON_TIME_GRACE_MIN && lateMin <= VERY_LATE_AFTER_MIN;
        const didntComeFlag = lateMin > VERY_LATE_AFTER_MIN;

        db.prepare(`
          INSERT INTO daily_attendance (
            employee_id, shift_date, shift_key,
            first_check_in_at, first_check_in_name, first_check_in_gender, first_check_in_late_min
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(employee_id, shift_date) DO UPDATE SET
            shift_key = excluded.shift_key,
            first_check_in_at = COALESCE(daily_attendance.first_check_in_at, excluded.first_check_in_at),
            first_check_in_name = COALESCE(daily_attendance.first_check_in_name, excluded.first_check_in_name),
            first_check_in_gender = COALESCE(daily_attendance.first_check_in_gender, excluded.first_check_in_gender),
            first_check_in_late_min = COALESCE(daily_attendance.first_check_in_late_min, excluded.first_check_in_late_min)
        `).run(
            employeeId,
            shiftDate,
            shiftInfo.shiftKey,
            eventTime.toISOString(),
            employeeName || 'Unknown',
            gender,
            lateMin
        );

        let header = '✅ <b>On-Time Check In</b>';
        if (lateFlag) header = '⏰ <b>Late Check In</b>';
        if (didntComeFlag) header = '🚫 <b>Very Late</b>';
        let msg = `${header}\n🏷 Shift: ${configuredShift.label}\n${baseMessage}`;
        if (didntComeFlag) msg += `\n🚫 Marked as: <b>Did Not Come</b>\n⏱ Late by: <b>${lateMin} min</b>`;
        else if (lateFlag) msg += `\n🚨 Late by: <b>${lateMin} min</b>`;
        else msg += `\n🟢 On time (within ${ON_TIME_GRACE_MIN} min grace)`;
        const sent = await sendAttendanceTelegram(msg, employeeId, employeeName);
        await sendPersonalDm(employeeId, msg);
        if (sent) {
            await appendEventToGoogleSheet(buildSheetRow({
                timeLocal: timeStr,
                employeeId,
                employeeName,
                action: didntComeFlag ? `Did Not Come (Checked In >${VERY_LATE_AFTER_MIN}m)` : (lateFlag ? 'Late Check In' : 'On-Time Check In'),
                shiftTime: formatShiftTime(configuredShift),
                shiftDate,
                lateMinutes: lateMin,
                didntCome: didntComeFlag
            }), targetSheet);
            console.log(`✅ SENT: first check-in (${didntComeFlag ? 'did-not-come' : (lateFlag ? 'late' : 'on-time')}) — ${employeeName} (${employeeId})`);
        }
        return;
    }

    if (checkType === 'checkOut') {
        db.prepare(`
          INSERT INTO daily_attendance (employee_id, shift_date, shift_key, check_out_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(employee_id, shift_date) DO UPDATE SET
            shift_key = excluded.shift_key,
            check_out_at = excluded.check_out_at
        `).run(employeeId, shiftDate, shiftInfo.shiftKey, eventTime.toISOString());

        const dayRow = db.prepare(`
          SELECT first_check_in_at FROM daily_attendance
          WHERE employee_id = ? AND shift_date = ?
        `).get(employeeId, shiftDate);
        let msg = `🏁 <b>Check Out</b>\n🏷 Shift: ${configuredShift.label}\n${baseMessage}`;
        if (dayRow && dayRow.first_check_in_at) {
            msg += `\n⏱ Worked: <b>${formatWorkedDuration(dayRow.first_check_in_at, eventTime, shiftDate, configuredShift)}</b>`;
        }
        const sent = await sendAttendanceTelegram(msg, employeeId, employeeName);
        await sendPersonalDm(employeeId, msg);
        if (sent) {
            await appendEventToGoogleSheet(buildSheetRow({
                timeLocal: timeStr,
                employeeId,
                employeeName,
                action: 'Check Out',
                shiftTime: formatShiftTime(configuredShift),
                shiftDate
            }), targetSheet);
            console.log(`✅ SENT: check-out — ${employeeName} (${employeeId})`);
        }
        return;
    }
}

async function runNoShowCheck() {
    const now = new Date();
    const today = formatDateInZone(now);
    if (isSundayDateString(today)) return;

    for (const [employeeId, info] of Object.entries(EMPLOYEE_SHIFT_MAP)) {
        const shift = SHIFT_RULES[info.shiftKey];
        if (!shift) continue;

        const lateDeadline = makeShiftDateTime(today, shift.workStart, 0);
        lateDeadline.setMinutes(lateDeadline.getMinutes() + VERY_LATE_AFTER_MIN);
        const finalCheckInCutoff = makeShiftDateTime(today, shift.validCheckInTo, 0);
        const noShowAfter = lateDeadline > finalCheckInCutoff ? lateDeadline : finalCheckInCutoff;
        const checkOutCutoff = makeShiftDateTime(today, shift.validCheckOutTo, shift.checkOutDayOffset);
        if (!(now > noShowAfter && now < checkOutCutoff)) continue;

        const row = db.prepare(`
          SELECT first_check_in_at, absent_notified
          FROM daily_attendance
          WHERE employee_id = ? AND shift_date = ?
        `).get(employeeId, today);
        if (row && row.first_check_in_at) continue;
        if (row && row.absent_notified) continue;

        db.prepare(`
          INSERT INTO daily_attendance (employee_id, shift_date, shift_key, absent_notified)
          VALUES (?, ?, ?, 1)
          ON CONFLICT(employee_id, shift_date) DO UPDATE SET
            absent_notified = 1
        `).run(employeeId, today, info.shiftKey);

        const name = info.name || 'Unknown';
        const targetSheet = getGoogleSheetTargetForEmployee(employeeId, name);
        const noShowMsg =
            `🚫 <b>No Show Alert</b>\n` +
            `👤 Name: ${name}\n` +
            `🆔 ID: ${employeeId}\n` +
            `🏷 Shift: ${shift.label}\n` +
            `📅 Shift Date: ${today}\n` +
            `⏱ No check-in received within ${VERY_LATE_AFTER_MIN} minutes of shift start`;
        const sent = await sendAttendanceTelegramByEmployeeId(noShowMsg, employeeId);
        await sendPersonalDm(employeeId, noShowMsg);
        if (sent) {
            await appendEventToGoogleSheet(buildSheetRow({
                timeLocal: formatDateTimeInZone(now),
                employeeId,
                employeeName: name,
                action: 'Did Not Come (No Check In)',
                shiftTime: formatShiftTime(shift),
                shiftDate: today,
                didntCome: true
            }), targetSheet);
            console.log(`🚫 SENT: no-show alert — ${name} (${employeeId})`);
        }
    }
}

async function runBreakOvertimeCheck() {
    const now = new Date();
    const lookbackIso = new Date(now.getTime() - 36 * 60 * 60 * 1000).toISOString();
    const employeeRows = db.prepare(`
      SELECT DISTINCT employee_id
      FROM attendance
      WHERE employee_id IS NOT NULL
        AND employee_id != 'unknown'
        AND timestamp >= ?
    `).all(lookbackIso);

    for (const emp of employeeRows) {
        const employeeId = emp.employee_id;
        const lastBreakEvent = db.prepare(`
          SELECT employee_name, status, timestamp
          FROM attendance
          WHERE employee_id = ?
            AND status IN ('breakOut', 'breakIn')
          ORDER BY timestamp DESC
          LIMIT 1
        `).get(employeeId);

        if (!lastBreakEvent || lastBreakEvent.status !== 'breakOut') continue;
        const breakOutAt = new Date(lastBreakEvent.timestamp);
        const breakMinutes = minutesBetween(now, breakOutAt);
        if (breakMinutes < BREAK_LIMIT_MIN) continue;

        const alreadyAlerted = db.prepare(`
          SELECT 1
          FROM break_overtime_alerts
          WHERE employee_id = ? AND break_out_at = ?
        `).get(employeeId, lastBreakEvent.timestamp);
        if (alreadyAlerted) continue;

        db.prepare(`
          INSERT INTO break_overtime_alerts (employee_id, break_out_at, alerted_at)
          VALUES (?, ?, ?)
        `).run(employeeId, lastBreakEvent.timestamp, now.toISOString());

        const name = lastBreakEvent.employee_name || EMPLOYEE_SHIFT_MAP[String(employeeId)]?.name || 'Unknown';
        const sent = await sendAttendanceTelegramByEmployeeId(
            `🚨 <b>Break Time Exceeded</b>\n` +
            `👤 Name: ${name}\n` +
            `🆔 ID: ${employeeId}\n` +
            `⏱ Out on break for: <b>${breakMinutes} min</b>\n` +
            `⚠️ ${name} is out of time on break (limit: ${BREAK_LIMIT_MIN} min)`,
            employeeId
        );
        if (sent) {
            console.log(`🚨 SENT: break overtime alert — ${name} (${employeeId})`);
        }
    }
}

app.post('/hikvision/event', upload.any(), async (req, res) => {
    try {
        let data = {};
        const bodyKeys = req.body && typeof req.body === 'object' ? Object.keys(req.body) : [];
        if (DEBUG_WEBHOOKS) {
            console.log(`📥 Webhook hit: /hikvision/event | content-type=${req.headers['content-type'] || 'unknown'} | bodyKeys=${bodyKeys.join(',') || 'none'}`);
        }

        if (req.body && req.body.event_log) {
            data = tryParseJson(req.body.event_log);
        } else if (req.body && typeof req.body === 'object' && Object.keys(req.body).length > 0) {
            data = {
                ...req.body,
                AccessControllerEvent: tryParseJson(req.body.AccessControllerEvent)
            };
        }
        if ((!data || Object.keys(data).length === 0) && req.body && typeof req.body === 'object') {
            data = { ...req.body };
        }

        lastWebhookSnapshot = {
            at: new Date().toISOString(),
            contentType: req.headers['content-type'] || 'unknown',
            bodyKeys,
            topLevelKeys: Object.keys(data || {}),
            rawBodyPreview: Object.fromEntries(
                Object.entries(req.body || {}).slice(0, 8).map(([k, v]) => [k, typeof v === 'string' ? v.slice(0, 240) : v])
            ),
            files: (req.files || []).map((f) => ({
                fieldname: f.fieldname,
                originalname: f.originalname,
                mimetype: f.mimetype,
                size: f.size
            })),
            accessControllerEventType: typeof data?.AccessControllerEvent,
            accessControllerEventPreview: typeof data?.AccessControllerEvent === 'object'
                ? {
                    attendanceStatus: data.AccessControllerEvent.attendanceStatus,
                    status: data.AccessControllerEvent.status,
                    checkType: data.AccessControllerEvent.checkType,
                    employeeNoString: data.AccessControllerEvent.employeeNoString,
                    employeeNo: data.AccessControllerEvent.employeeNo,
                    name: data.AccessControllerEvent.name,
                    majorEventType: data.AccessControllerEvent.majorEventType,
                    minorEventType: data.AccessControllerEvent.minorEventType,
                    subEventType: data.AccessControllerEvent.subEventType,
                    nestedAccessControllerEvent: data.AccessControllerEvent.AccessControllerEvent
                }
                : data?.AccessControllerEvent,
            extractedAccessEvent: extractAccessEvent(data),
            normalizedAccessEvent: normalizeAccessEventShape(extractAccessEvent(data))
        };

        await handleEvent(data);
        res.status(200).send('OK');
    } catch (err) {
        console.error('Handler error:', err.message);
        res.status(200).send('OK');
    }
});

app.get('/', (req, res) => res.send('Hikvision listener is running'));
app.get('/debug-last-event', (req, res) => res.json(lastWebhookSnapshot || { message: 'No webhook received yet' }));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Listener running on http://0.0.0.0:${PORT}`);
    console.log('Waiting for attendance events...\n');
    if (attendanceRoutingGroups.length > 0) {
        const uniqueChats = new Set(attendanceRoutingGroups.map((g) => g.chatId));
        if (TELEGRAM_CHAT_ID) uniqueChats.add(TELEGRAM_CHAT_ID);
        for (const chatId of uniqueChats) {
            sendTelegramToChat(chatId, '🟢 Attendance bot started and listening for events');
        }
    } else {
        sendTelegram('🟢 Attendance bot started and listening for events');
    }
    if (isGoogleSheetsEnabled()) {
        const startupSheetTargets = [];
        if (GOOGLE_SHEET_ID) {
            startupSheetTargets.push({
                spreadsheetId: GOOGLE_SHEET_ID,
                sheetName: GOOGLE_SHEET_NAME
            });
        }
        if (attendanceRoutingGroups.length > 0) {
            const selectedTarget = {
                spreadsheetId: GOOGLE_SHEET_SELECTED_ID || GOOGLE_SHEET_ID,
                sheetName: GOOGLE_SHEET_SELECTED_NAME
            };
            const alreadyIncluded = startupSheetTargets.some(
                (item) => item.spreadsheetId === selectedTarget.spreadsheetId && item.sheetName === selectedTarget.sheetName
            );
            if (!alreadyIncluded && selectedTarget.spreadsheetId) {
                startupSheetTargets.push(selectedTarget);
            }
        }
        Promise.all(startupSheetTargets.map((target) => ensureGoogleSheetHeader(target.spreadsheetId, target.sheetName))).then(() => {
            console.log('📄 Google Sheets logging is active.');
        }).catch((err) => {
            console.error('Google Sheets startup check error:', err.message);
        });
    } else {
        console.log('📄 Google Sheets logging is disabled (env vars missing).');
    }
});


setInterval(() => {
    runNoShowCheck().catch((err) => {
        console.error('No-show check error:', err.message);
    });
}, NO_SHOW_CHECK_INTERVAL_MS);

startTelegramPolling().catch((err) => {
    console.error('Telegram polling fatal error:', err.message);
});

// Break overtime alert loop intentionally disabled for now.
// setInterval(() => {
//     runBreakOvertimeCheck().catch((err) => {
//         console.error('Break overtime check error:', err.message);
//     });
// }, NO_SHOW_CHECK_INTERVAL_MS);
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.setSocketIO = void 0;
const express_1 = require("express");
const axios_1 = __importDefault(require("axios"));
const axios_cookiejar_support_1 = require("axios-cookiejar-support");
const tough_cookie_1 = require("tough-cookie");
const cheerio = __importStar(require("cheerio"));
const supabase_js_1 = require("@supabase/supabase-js");
const router = (0, express_1.Router)();
let io;
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
const PROJECT_BASE_URL = 'https://project-5.at';
const MITARBEITER_URL = 'https://project-5.at/mitarbeiter.php';
const setSocketIO = (socketIO) => {
    io = socketIO;
};
exports.setSocketIO = setSocketIO;
let supabase = null;
function getSupabaseClient() {
    if (!supabase) {
        const supabaseUrl = process.env.SUPABASE_URL || '';
        const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
        if (!supabaseUrl || !supabaseKey) {
            throw new Error('SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt!');
        }
        supabase = (0, supabase_js_1.createClient)(supabaseUrl, supabaseKey);
    }
    return supabase;
}
const getBrowserHeaders = (referer) => {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language': 'de-DE,de;q=0.9',
        'DNT': '1',
        'Connection': 'keep-alive'
    };
    if (referer) {
        headers['Referer'] = referer;
    }
    return headers;
};
const getCookieString = (jar, url) => {
    try {
        const cookies = jar.getCookiesSync(url);
        if (cookies.length === 0)
            return null;
        return cookies.map((c) => `${c.key}=${c.value}`).join('; ');
    }
    catch {
        return null;
    }
};
const extractFormData = (html, username, password) => {
    const $ = cheerio.load(html);
    const form = $('form').first();
    const loginData = {};
    form.find('input[type="hidden"]').each((i, elem) => {
        const name = $(elem).attr('name');
        const value = $(elem).attr('value') || '';
        if (name)
            loginData[name] = value;
    });
    loginData['gpnr'] = username;
    loginData['passwort'] = password;
    return { data: loginData, action: form.attr('action') || null };
};
async function performLogin(client, jar, username, password) {
    try {
        const loginPageResp = await client.get(`${PROJECT_BASE_URL}/index.php`, {
            headers: getBrowserHeaders()
        });
        if (loginPageResp.status !== 200) {
            return { success: false, error: `Login-Seite: HTTP ${loginPageResp.status}` };
        }
        const { data: formData, action: formAction } = extractFormData(loginPageResp.data, username, password);
        const loginUrl = !formAction || formAction === ''
            ? `${PROJECT_BASE_URL}/login.php`
            : formAction.startsWith('http')
                ? formAction
                : PROJECT_BASE_URL + formAction;
        await client.post(loginUrl, new URLSearchParams(formData).toString(), {
            headers: {
                ...getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`),
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        });
        const cookieStr = getCookieString(jar, PROJECT_BASE_URL);
        if (cookieStr) {
            console.log('[FORMULARE] Login erfolgreich');
            return { success: true, cookieString: cookieStr };
        }
        else {
            return { success: false, error: 'Kein Cookie erhalten' };
        }
    }
    catch (error) {
        return { success: false, error: error.message };
    }
}
// Lade ALLE erledigte Formulare EINMAL (effizient!)
async function scrapeAllErledigteFormulare(client, cookieString) {
    try {
        const url = `${PROJECT_BASE_URL}/formularuebersicht_mitarbeiter.php?filter=erledigt&anzeigen=kunde_hat_geld`;
        console.log('[FORMULARE] Lade ALLE erledigte E-Formulare...');
        const response = await client.get(url, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': cookieString
            }
        });
        if (response.status !== 200) {
            console.error('[FORMULARE] HTTP Status:', response.status);
            return new Map();
        }
        console.log('[FORMULARE] HTML Länge:', response.data.length);
        const $ = cheerio.load(response.data);
        const formulareCounts = new Map();
        // Finde zuerst den Index der "Mitarbeiter"-Spalte
        let mitarbeiterColumnIndex = -1;
        $('table thead tr th, table tr:first th, table tr:first td').each((index, element) => {
            const headerText = $(element).text().trim().toLowerCase();
            console.log(`[FORMULARE] Header ${index}:`, headerText);
            if (headerText === 'mitarbeiter') {
                mitarbeiterColumnIndex = index;
                console.log('[FORMULARE] Mitarbeiter-Spalte gefunden bei Index:', index);
            }
        });
        // Fallback: Wenn kein Header gefunden, versuche Index 3 (typisch)
        if (mitarbeiterColumnIndex === -1) {
            console.log('[FORMULARE] Warnung: Header nicht gefunden, nutze Index 3');
            mitarbeiterColumnIndex = 3;
        }
        // Parse ALLE Zeilen - NUR die Mitarbeiter-Spalte!
        let rowCount = 0;
        $('table tbody tr, table tr').each((index, row) => {
            // Skip Header
            const hasHeaderCells = $(row).find('th').length > 0;
            if (hasHeaderCells || index === 0)
                return;
            const cells = $(row).find('td');
            if (cells.length === 0)
                return;
            // Hole NUR die Mitarbeiter-Spalte
            const mitarbeiterCell = cells.eq(mitarbeiterColumnIndex);
            const mitarbeiterName = mitarbeiterCell.text().trim();
            // Validiere dass es ein Name ist (nicht leer, nicht zu kurz)
            if (mitarbeiterName && mitarbeiterName.length > 3 && mitarbeiterName.includes(' ')) {
                const currentCount = formulareCounts.get(mitarbeiterName) || 0;
                formulareCounts.set(mitarbeiterName, currentCount + 1);
                rowCount++;
                // Log erste 20 zur Diagnose
                if (rowCount <= 20) {
                    console.log(`[FORMULARE] Zeile ${rowCount}: Mitarbeiter = "${mitarbeiterName}"`);
                }
            }
        });
        console.log('[FORMULARE] Gesamt Formulare gefunden:', rowCount);
        console.log('[FORMULARE] Eindeutige Mitarbeiter:', formulareCounts.size);
        // Log Top 5 Mitarbeiter mit meisten Formularen
        const sorted = Array.from(formulareCounts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5);
        console.log('[FORMULARE] Top 5 Mitarbeiter:');
        sorted.forEach(([name, count]) => {
            console.log(`  - ${name}: ${count} Formulare`);
        });
        return formulareCounts;
    }
    catch (error) {
        console.error('[FORMULARE] Fehler:', error.message);
        return new Map();
    }
}
const GPNR = '60235';
const PASSWORD = 'r87cucd';
// API Route
router.post('/check-formulare', async (req, res) => {
    const { userIds, socketId } = req.body;
    if (!userIds || !Array.isArray(userIds)) {
        return res.status(400).json({ success: false, error: 'userIds array erforderlich' });
    }
    const jar = new tough_cookie_1.CookieJar();
    const client = (0, axios_cookiejar_support_1.wrapper)(axios_1.default.create({
        jar,
        withCredentials: true,
        validateStatus: () => true,
        maxRedirects: 5,
        timeout: 30000
    }));
    try {
        // Login
        if (socketId && io) {
            io.to(socketId).emit('formulare-status', { message: 'Login wird durchgefuehrt...' });
        }
        const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
        if (!loginResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('formulare-error', { error: loginResult.error });
            }
            return res.status(401).json({ success: false, error: loginResult.error });
        }
        // Hole Mitarbeiter-Daten aus Supabase
        const supabaseClient = getSupabaseClient();
        const { data: mitarbeiterList, error: fetchError } = await supabaseClient
            .from('mitarbeiter_organigramm')
            .select('*')
            .in('user_id', userIds);
        if (fetchError || !mitarbeiterList) {
            return res.status(500).json({ success: false, error: 'Mitarbeiter nicht gefunden' });
        }
        // NEUE EFFIZIENTE METHODE: Lade ALLE Formulare EINMAL
        if (socketId && io) {
            io.to(socketId).emit('formulare-progress', {
                current: 1,
                total: 2,
                message: 'Lade alle erledigten E-Formulare...'
            });
        }
        const formulareCounts = await scrapeAllErledigteFormulare(client, loginResult.cookieString);
        console.log('[FORMULARE] Formular-Daten geladen. Verarbeite Mitarbeiter...');
        const results = [];
        for (const mitarbeiter of mitarbeiterList) {
            if (socketId && io) {
                io.to(socketId).emit('formulare-progress', {
                    current: results.length + 1,
                    total: mitarbeiterList.length,
                    message: `Verarbeite ${mitarbeiter.full_name}...`
                });
            }
            // Suche Mitarbeiter-Name in der Map (case-insensitive)
            let erledigteCount = 0;
            const searchName = mitarbeiter.full_name.toLowerCase();
            for (const [name, count] of formulareCounts.entries()) {
                if (name.toLowerCase() === searchName ||
                    name.toLowerCase().includes(searchName) ||
                    searchName.includes(name.toLowerCase())) {
                    erledigteCount += count;
                    console.log('[FORMULARE] Match:', mitarbeiter.full_name, '→', name, '=', count);
                }
            }
            console.log('[FORMULARE]', mitarbeiter.full_name, '→', erledigteCount, 'erledigte Formulare');
            // Speichere in Supabase
            await supabaseClient
                .from('mitarbeiter_organigramm')
                .update({
                erledigte_formulare: erledigteCount
            })
                .eq('user_id', mitarbeiter.user_id);
            results.push({
                user_id: mitarbeiter.user_id,
                full_name: mitarbeiter.full_name,
                erledigte_formulare: erledigteCount
            });
        }
        // Berechne Gesamt-Summe
        const totalErledigte = results.reduce((sum, r) => sum + r.erledigte_formulare, 0);
        // Update Manager mit Gesamt-Summe (erster in der Liste ist immer der Manager durch includeManager)
        if (results.length > 0) {
            const managerUserId = results[0].user_id;
            const supabaseClient = getSupabaseClient();
            await supabaseClient
                .from('mitarbeiter_organigramm')
                .update({ erledigte_formulare_gesamt: totalErledigte })
                .eq('user_id', managerUserId);
            console.log('[FORMULARE] Manager Gesamt-Summe aktualisiert:', totalErledigte);
        }
        if (socketId && io) {
            io.to(socketId).emit('formulare-complete', {
                total: results.length,
                totalErledigte: totalErledigte,
                results: results
            });
        }
        res.json({
            success: true,
            results: results,
            totalErledigte: totalErledigte
        });
    }
    catch (error) {
        console.error('[FORMULARE API] Fehler:', error.message);
        if (socketId && io) {
            io.to(socketId).emit('formulare-error', { error: error.message });
        }
        res.status(500).json({ success: false, error: error.message });
    }
});
// Neue Route: Scrape ALLE E-Formulare und gib als CSV zurück
router.post('/download-all', async (req, res) => {
    const { socketId } = req.body;
    const GPNR = '60235';
    const PASSWORD = 'r87cucd';
    const jar = new tough_cookie_1.CookieJar();
    const client = (0, axios_cookiejar_support_1.wrapper)(axios_1.default.create({
        jar,
        withCredentials: true,
        validateStatus: () => true,
        maxRedirects: 5,
        timeout: 30000
    }));
    try {
        if (socketId && io) {
            io.to(socketId).emit('formulare-download-status', { message: 'Login...' });
        }
        console.log('[FORMULARE-DOWNLOAD] Starte...');
        // Login
        const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
        if (!loginResult.success) {
            return res.status(401).json({ success: false, error: loginResult.error });
        }
        if (socketId && io) {
            io.to(socketId).emit('formulare-download-status', { message: 'Scrape Formulare...' });
        }
        // Scrape formularuebersicht_mitarbeiter.php
        const url = 'https://project-5.at/formularuebersicht_mitarbeiter.php';
        const response = await client.get(url, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': loginResult.cookieString
            }
        });
        if (response.status !== 200) {
            return res.status(500).json({ success: false, error: `HTTP ${response.status}` });
        }
        const $ = cheerio.load(response.data);
        const formulare = [];
        console.log('[FORMULARE-DOWNLOAD] Parse Tabelle...');
        // Parse Tabelle
        $('table tr').each((index, row) => {
            if (index === 0)
                return; // Skip header
            const cells = $(row).find('td');
            if (cells.length === 0)
                return;
            const formular = {
                id: cells.eq(0).text().trim(),
                datum: cells.eq(1).text().trim(),
                kategorie: cells.eq(2).text().trim(),
                mitarbeiter: cells.eq(3).text().trim(),
                name: cells.eq(4).text().trim(),
                plz: cells.eq(5).text().trim(),
                anbieter: cells.eq(6).text().trim(),
                firma: cells.eq(7).text().trim(),
                kdnr: cells.eq(8).text().trim(),
                wechsel: cells.eq(9).text().trim().includes('✓') ? 'Ja' : 'Nein',
                strom: cells.eq(10).text().trim().includes('✓') ? 'Ja' : 'Nein',
                gas: cells.eq(11).text().trim().includes('✓') ? 'Ja' : 'Nein',
                frist: cells.eq(12).text().trim().includes('✓') ? 'Ja' : 'Nein'
            };
            formulare.push(formular);
        });
        console.log('[FORMULARE-DOWNLOAD] Gefunden:', formulare.length);
        // Speichere in Supabase
        if (socketId && io) {
            io.to(socketId).emit('formulare-download-status', { message: 'Speichere in DB...' });
        }
        const supabase = getSupabaseClient();
        let savedCount = 0;
        for (const formular of formulare) {
            try {
                const { error } = await supabase
                    .from('energie_formulare_mitarbeiter')
                    .upsert({
                    id: parseInt(formular.id),
                    datum: formular.datum || null,
                    kategorie: formular.kategorie,
                    mitarbeiter: formular.mitarbeiter,
                    name: formular.name,
                    plz: formular.plz,
                    anbieter: formular.anbieter,
                    firma: formular.firma,
                    kdnr: formular.kdnr,
                    wechsel: formular.wechsel,
                    strom: formular.strom,
                    gas: formular.gas,
                    frist: formular.frist
                }, {
                    onConflict: 'id'
                });
                if (!error) {
                    savedCount++;
                }
            }
            catch (err) {
                console.error('[FORMULARE-DOWNLOAD] Fehler beim Speichern von Formular', formular.id, ':', err);
            }
        }
        console.log('[FORMULARE-DOWNLOAD] In DB gespeichert:', savedCount, '/', formulare.length);
        if (socketId && io) {
            io.to(socketId).emit('formulare-download-complete', {
                count: formulare.length,
                saved: savedCount
            });
        }
        // Erfolgs-Response
        res.json({
            success: true,
            total: formulare.length,
            saved: savedCount,
            message: `${savedCount} von ${formulare.length} Formularen in Supabase gespeichert`
        });
    }
    catch (error) {
        console.error('[FORMULARE-DOWNLOAD] Fehler:', error.message);
        if (socketId && io) {
            io.to(socketId).emit('formulare-download-error', { error: error.message });
        }
        res.status(500).json({ success: false, error: error.message });
    }
});
// Route: Scrape BK-Formulare und lade als CSV herunter
router.post('/download-bk', async (req, res) => {
    const { socketId } = req.body;
    const GPNR = '60235';
    const PASSWORD = 'r87cucd';
    const jar = new tough_cookie_1.CookieJar();
    const client = (0, axios_cookiejar_support_1.wrapper)(axios_1.default.create({
        jar,
        withCredentials: true,
        validateStatus: () => true,
        maxRedirects: 5,
        timeout: 30000
    }));
    try {
        if (socketId && io) {
            io.to(socketId).emit('bk-formulare-status', { message: 'Login...' });
        }
        console.log('[BK-FORMULARE] Starte...');
        // Login
        const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
        if (!loginResult.success) {
            return res.status(401).json({ success: false, error: loginResult.error });
        }
        if (socketId && io) {
            io.to(socketId).emit('bk-formulare-status', { message: 'Scrape BK-Formulare...' });
        }
        // Scrape formularuebersicht_bk_mitarbeiter.php
        const url = 'https://project-5.at/formularuebersicht_bk_mitarbeiter.php';
        const response = await client.get(url, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': loginResult.cookieString
            }
        });
        if (response.status !== 200) {
            return res.status(500).json({ success: false, error: `HTTP ${response.status}` });
        }
        const $ = cheerio.load(response.data);
        const formulare = [];
        console.log('[BK-FORMULARE] Parse Tabelle...');
        // Parse Tabelle
        $('table tr').each((index, row) => {
            if (index === 0)
                return; // Skip header
            const cells = $(row).find('td');
            if (cells.length === 0)
                return;
            const formular = {
                id: cells.eq(0).text().trim(),
                datum: cells.eq(1).text().trim(),
                kategorie: cells.eq(2).text().trim(),
                mitarbeiter: cells.eq(3).text().trim(),
                name: cells.eq(4).text().trim(),
                plz: cells.eq(5).text().trim(),
                anbieter: cells.eq(6).text().trim(),
                firma: cells.eq(7).text().trim(),
                kdnr: cells.eq(8).text().trim(),
                wechsel: cells.eq(9).text().trim(),
                strom: cells.eq(10).text().trim(),
                gas: cells.eq(11).text().trim(),
                frist: cells.eq(12).text().trim()
            };
            formulare.push(formular);
        });
        console.log('[BK-FORMULARE] Gefunden:', formulare.length);
        if (socketId && io) {
            io.to(socketId).emit('bk-formulare-complete', {
                count: formulare.length
            });
        }
        // Konvertiere zu CSV
        if (formulare.length > 0) {
            const headers = Object.keys(formulare[0]);
            const csvRows = [
                headers.join(','), // Header-Zeile
                ...formulare.map(row => headers.map(h => {
                    const value = row[h] || '';
                    // Escape Kommas und Anführungszeichen
                    return `"${String(value).replace(/"/g, '""')}"`;
                }).join(','))
            ];
            const csvContent = csvRows.join('\n');
            // Setze Content-Type für Download
            res.setHeader('Content-Type', 'text/csv; charset=utf-8');
            res.setHeader('Content-Disposition', `attachment; filename="bk-formulare-${new Date().toISOString().split('T')[0]}.csv"`);
            res.send('\uFEFF' + csvContent); // UTF-8 BOM für Excel
        }
        else {
            res.json({ success: false, error: 'Keine BK-Formulare gefunden' });
        }
    }
    catch (error) {
        console.error('[BK-FORMULARE] Fehler:', error.message);
        if (socketId && io) {
            io.to(socketId).emit('bk-formulare-error', { error: error.message });
        }
        res.status(500).json({ success: false, error: error.message });
    }
});
// Route: Hole Formulare-Statistiken aus Supabase (schnell!)
router.post('/stats', async (req, res) => {
    const { socketId } = req.body;
    try {
        const supabase = getSupabaseClient();
        // Zähle Formulare
        const { count, error } = await supabase
            .from('energie_formulare_mitarbeiter')
            .select('*', { count: 'exact', head: true });
        if (error) {
            console.error('[FORMULARE-STATS] Fehler:', error.message);
            return res.status(500).json({ success: false, error: error.message });
        }
        console.log('[FORMULARE-STATS] Gesamt Formulare:', count);
        if (socketId && io) {
            io.to(socketId).emit('formulare-stats-result', {
                success: true,
                totalFormulare: count || 0
            });
        }
        res.json({
            success: true,
            totalFormulare: count || 0
        });
    }
    catch (error) {
        console.error('[FORMULARE-STATS] Fehler:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});
exports.default = router;

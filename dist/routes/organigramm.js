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
// Supabase Client - wird lazy initialisiert
let supabase = null;
function getSupabaseClient() {
    if (!supabase) {
        const supabaseUrl = process.env.SUPABASE_URL || '';
        const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
        console.log('[SUPABASE] Initialisiere Client...');
        console.log('[SUPABASE] URL:', supabaseUrl ? 'vorhanden' : 'FEHLT!');
        console.log('[SUPABASE] Key:', supabaseKey ? 'vorhanden' : 'FEHLT!');
        if (!supabaseUrl || !supabaseKey) {
            throw new Error('SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt in .env!');
        }
        supabase = (0, supabase_js_1.createClient)(supabaseUrl, supabaseKey);
    }
    return supabase;
}
// TLS-Zertifikatsprüfung deaktivieren (für Entwicklung)
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
// ===== KONFIGURATION =====
const PROJECT_BASE_URL = 'https://project-5.at';
const ORGANIGRAMM_URL = 'https://project-5.at/mitarbeiter_organigramm.php';
// Socket.io Setter
const setSocketIO = (socketIO) => {
    io = socketIO;
};
exports.setSocketIO = setSocketIO;
// ===== HILFSFUNKTIONEN =====
const getBrowserHeaders = (referer) => {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'de-DE,de;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    };
    if (referer) {
        headers['Referer'] = referer;
        headers['Origin'] = new URL(referer).origin;
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
// ===== LOGIN-FUNKTION =====
async function performLogin(client, jar, username, password) {
    try {
        console.log('[ORGANIGRAMM] Login-Versuch für:', username);
        // Login-Seite laden
        const loginPageResp = await client.get(`${PROJECT_BASE_URL}/index.php`, {
            headers: getBrowserHeaders()
        });
        if (loginPageResp.status !== 200) {
            return { success: false, error: `Login-Seite: HTTP ${loginPageResp.status}` };
        }
        console.log('[ORGANIGRAMM] Login-Seite geladen');
        // Formular-Daten extrahieren
        const { data: formData, action: formAction } = extractFormData(loginPageResp.data, username, password);
        const loginUrl = !formAction || formAction === ''
            ? `${PROJECT_BASE_URL}/login.php`
            : formAction.startsWith('http')
                ? formAction
                : PROJECT_BASE_URL + formAction;
        console.log('[ORGANIGRAMM] Login-Request an:', loginUrl);
        // Login durchführen
        await client.post(loginUrl, new URLSearchParams(formData).toString(), {
            headers: {
                ...getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`),
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': `${PROJECT_BASE_URL}/index.php`,
                'Origin': PROJECT_BASE_URL
            }
        });
        // Cookie auslesen
        const cookieStr = getCookieString(jar, PROJECT_BASE_URL);
        if (cookieStr) {
            console.log('[ORGANIGRAMM] Login erfolgreich!');
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
// ===== LOGOUT-FUNKTION =====
async function performLogout(client, jar) {
    try {
        console.log('[ORGANIGRAMM] Logout durchführen...');
        await client.get(`${PROJECT_BASE_URL}/logout.php`, {
            headers: getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`)
        });
        jar.removeAllCookiesSync();
        console.log('[ORGANIGRAMM] Logout erfolgreich');
    }
    catch (error) {
        console.log('[ORGANIGRAMM] Logout-Fehler:', error.message);
    }
}
// ===== SCRAPING-FUNKTION FÜR ORGANIGRAMM =====
async function scrapeOrganigramm(client, cookieString, socketId) {
    try {
        if (socketId && io) {
            io.to(socketId).emit('organigramm-status', { message: 'Lade Organigramm-Daten...' });
        }
        console.log('[ORGANIGRAMM] Starte Scraping von:', ORGANIGRAMM_URL);
        const response = await client.get(ORGANIGRAMM_URL, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': cookieString
            }
        });
        if (response.status !== 200) {
            return { success: false, error: `HTTP ${response.status}` };
        }
        console.log('[ORGANIGRAMM] Seite erfolgreich geladen');
        console.log('[ORGANIGRAMM] HTML Länge:', response.data.length);
        // Parse HTML
        const $ = cheerio.load(response.data);
        const organigrammData = [];
        console.log('[ORGANIGRAMM] Analysiere Organigramm-Struktur...');
        console.log('[ORGANIGRAMM] Anzahl .user-card Elemente:', $('.user-card').length);
        console.log('[ORGANIGRAMM] Anzahl .org-chart Elemente:', $('.org-chart').length);
        // Extrahiere Statistiken
        const stats = {
            direkte: 0,
            gfGps: 0,
            vcGps: 0
        };
        $('p').each((i, elem) => {
            const text = $(elem).text();
            const direkteMatch = text.match(/Anzahl DIREKTE:\s*(\d+)/);
            const gfMatch = text.match(/Anzahl GF GPs\s*:\s*(\d+)/);
            const vcMatch = text.match(/Anzahl VC GPs\s*:\s*(\d+)/);
            if (direkteMatch)
                stats.direkte = parseInt(direkteMatch[1]);
            if (gfMatch)
                stats.gfGps = parseInt(gfMatch[1]);
            if (vcMatch)
                stats.vcGps = parseInt(vcMatch[1]);
        });
        console.log('[ORGANIGRAMM] Statistiken:', stats);
        // Rekursive Funktion um Hierarchie zu parsen
        function parseUserCard($card, level = 0, parentId = null) {
            const userId = $card.attr('data-user-id');
            if (!userId)
                return null;
            // Name extrahieren
            const nameLink = $card.find('.card-title a');
            const name = nameLink.text().trim();
            const editUrl = nameLink.attr('href') || '';
            // VC24-Nummer extrahieren
            const vcNumber = $card.find('.card-subtitle').text().trim();
            // GPNR/GF extrahieren
            const gfText = $card.find('.mb-1').last().text().trim();
            const gpnrMatch = gfText.match(/GF:\s*(\d+)/);
            const gpnr = gpnrMatch ? gpnrMatch[1] : '';
            // Vergütung/Provision extrahieren
            const badge = $card.find('.verguetung-badge');
            const verguetung = badge.text().trim();
            // Initialen oder Foto
            const initials = $card.find('.user-initials').text().trim();
            const photoSrc = $card.find('.user-photo').attr('src') || '';
            const userData = {
                user_id: userId,
                name: name,
                vc24_number: vcNumber,
                gpnr: gpnr,
                verguetung: verguetung,
                initials: initials,
                photo_url: photoSrc,
                edit_url: editUrl,
                level: level,
                parent_id: parentId,
                children: []
            };
            return userData;
        }
        // Rekursive Funktion um Hierarchie zu durchlaufen
        function traverseHierarchy($li, level = 0, parentId = null) {
            const results = [];
            const $card = $li.find('> .user-card').first();
            if ($card.length > 0) {
                const userData = parseUserCard($card, level, parentId);
                if (userData) {
                    results.push(userData);
                    // Finde unterstellte Mitarbeiter
                    const $childrenList = $li.find('> .children-list').first();
                    if ($childrenList.length > 0) {
                        $childrenList.find('> li').each((_i, childLi) => {
                            const children = traverseHierarchy($(childLi), level + 1, userData.user_id);
                            results.push(...children);
                            // NUR die DIREKTEN Kinder (erste Ebene) hinzufügen, nicht alle rekursiven!
                            const directChild = children[0]; // Das erste Element ist immer das direkte Kind
                            if (directChild) {
                                userData.children.push(directChild.user_id);
                            }
                        });
                    }
                }
            }
            return results;
        }
        // Starte Parsing vom Root
        $('.org-chart > ul > li').each((i, li) => {
            const hierarchy = traverseHierarchy($(li), 0, null);
            organigrammData.push(...hierarchy);
        });
        console.log('[ORGANIGRAMM] Gesamt gefundene Mitarbeiter:', organigrammData.length);
        // Log erste 5 Mitarbeiter
        organigrammData.slice(0, 5).forEach((user, index) => {
            console.log(`[ORGANIGRAMM] Mitarbeiter ${index + 1}:`, JSON.stringify(user, null, 2));
        });
        // Datengröße berechnen
        const dataSizeBytes = JSON.stringify(organigrammData).length;
        const dataSizeMB = (dataSizeBytes / (1024 * 1024)).toFixed(2);
        return {
            success: true,
            count: organigrammData.length,
            size: dataSizeMB,
            data: organigrammData,
            stats: stats
        };
    }
    catch (error) {
        console.error('[ORGANIGRAMM] Fehler:', error.message);
        console.error('[ORGANIGRAMM] Stack:', error.stack);
        return { success: false, error: error.message };
    }
}
// ===== SUPABASE SPEICHERN =====
async function saveToSupabase(organigrammData, socketId) {
    try {
        if (socketId && io) {
            io.to(socketId).emit('organigramm-status', { message: 'Speichere in Datenbank...' });
        }
        console.log('[SUPABASE] Starte Speicherung von', organigrammData.length, 'Mitarbeitern');
        // Formatiere Daten für Supabase
        const formattedData = organigrammData.map(user => ({
            user_id: user.user_id,
            vc24_number: user.vc24_number,
            gpnr: user.gpnr,
            full_name: user.name,
            initials: user.initials,
            photo_url: user.photo_url,
            edit_url: user.edit_url,
            level: user.level,
            parent_user_id: user.parent_id,
            children_user_ids: user.children,
            verguetung: user.verguetung,
            anzahl_unterstellte: user.children.length,
            is_active: true,
            last_scraped_at: new Date().toISOString()
        }));
        // Upsert in Supabase (Insert oder Update wenn user_id existiert)
        const supabaseClient = getSupabaseClient();
        const { data, error } = await supabaseClient
            .from('mitarbeiter_organigramm')
            .upsert(formattedData, {
            onConflict: 'user_id',
            ignoreDuplicates: false
        })
            .select();
        if (error) {
            console.error('[SUPABASE] Fehler beim Speichern:', error);
            return { success: false, error: error.message };
        }
        console.log('[SUPABASE] Erfolgreich gespeichert:', data?.length || 0, 'Mitarbeiter');
        return { success: true, savedCount: data?.length || 0 };
    }
    catch (error) {
        console.error('[SUPABASE] Fehler:', error.message);
        return { success: false, error: error.message };
    }
}
// ===== FESTE LOGIN-DATEN =====
const GPNR = '60235';
const PASSWORD = 'r87cucd';
// ===== API ROUTE =====
router.post('/', async (req, res) => {
    const { socketId } = req.body;
    const gpnr = GPNR;
    const password = PASSWORD;
    // Cookie-Jar und Axios-Client erstellen
    const jar = new tough_cookie_1.CookieJar();
    const client = (0, axios_cookiejar_support_1.wrapper)(axios_1.default.create({
        jar,
        withCredentials: true,
        validateStatus: () => true,
        maxRedirects: 5,
        timeout: 30000
    }));
    try {
        // Socket-Status Update
        if (socketId && io) {
            io.to(socketId).emit('organigramm-status', { message: 'Login wird durchgeführt...' });
        }
        // Login durchführen
        const loginResult = await performLogin(client, jar, gpnr, password);
        if (!loginResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('organigramm-error', { error: loginResult.error });
            }
            return res.status(401).json({
                success: false,
                error: loginResult.error
            });
        }
        // Organigramm scrapen
        const scrapeResult = await scrapeOrganigramm(client, loginResult.cookieString, socketId);
        if (!scrapeResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('organigramm-error', { error: scrapeResult.error });
            }
            return res.status(500).json({
                success: false,
                error: scrapeResult.error
            });
        }
        // In Supabase speichern
        const saveResult = await saveToSupabase(scrapeResult.data, socketId);
        if (!saveResult.success) {
            console.error('[API] Warnung: Supabase-Speicherung fehlgeschlagen:', saveResult.error);
            // Weitermachen trotz Fehler - Daten sind trotzdem verfügbar
        }
        // Socket.io Update
        if (socketId && io) {
            io.to(socketId).emit('organigramm-complete', {
                count: scrapeResult.count,
                size: scrapeResult.size,
                data: scrapeResult.data,
                stats: scrapeResult.stats,
                savedToDb: saveResult.success,
                savedCount: saveResult.savedCount || 0
            });
        }
        // Logout durchführen
        await performLogout(client, jar);
        // Erfolgreiche Antwort
        res.json({
            success: true,
            count: scrapeResult.count,
            size: scrapeResult.size,
            data: scrapeResult.data,
            stats: scrapeResult.stats,
            savedToDb: saveResult.success,
            savedCount: saveResult.savedCount || 0,
            message: saveResult.success
                ? `Organigramm erfolgreich geladen und ${saveResult.savedCount} Mitarbeiter in DB gespeichert`
                : 'Organigramm erfolgreich geladen (DB-Speicherung fehlgeschlagen)'
        });
    }
    catch (error) {
        console.error('[ORGANIGRAMM API] Fehler:', error.message);
        if (req.body.socketId && io) {
            io.to(req.body.socketId).emit('organigramm-error', { error: error.message });
        }
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
exports.default = router;

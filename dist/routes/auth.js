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
const router = (0, express_1.Router)();
let io;
// TLS-Zertifikatsprüfung deaktivieren (für Entwicklung)
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
// ===== KONFIGURATION =====
const PROJECT_BASE_URL = 'https://project-5.at';
const SCRAPE_URL = 'https://project-5.at/formularuebersicht_mitarbeiter.php';
const MITARBEITER_URL = 'https://project-5.at/mitarbeiter.php';
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
        console.log('[AUTH] Login-Versuch für:', username);
        // Login-Seite laden
        const loginPageResp = await client.get(`${PROJECT_BASE_URL}/index.php`, {
            headers: getBrowserHeaders()
        });
        if (loginPageResp.status !== 200) {
            return { success: false, error: `Login-Seite: HTTP ${loginPageResp.status}` };
        }
        console.log('[AUTH] Login-Seite geladen');
        // Formular-Daten extrahieren
        const { data: formData, action: formAction } = extractFormData(loginPageResp.data, username, password);
        const loginUrl = !formAction || formAction === ''
            ? `${PROJECT_BASE_URL}/login.php`
            : formAction.startsWith('http')
                ? formAction
                : PROJECT_BASE_URL + formAction;
        console.log('[AUTH] Login-Request an:', loginUrl);
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
            console.log('[AUTH] Login erfolgreich!');
            console.log('[AUTH] Cookie:', cookieStr);
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
        console.log('[AUTH] Logout durchführen...');
        // Logout-Anfrage
        await client.get(`${PROJECT_BASE_URL}/logout.php`, {
            headers: getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`)
        });
        // Cookie-Jar leeren
        jar.removeAllCookiesSync();
        console.log('[AUTH] Logout erfolgreich - Cookies gelöscht');
    }
    catch (error) {
        console.log('[AUTH] Logout-Fehler:', error.message);
    }
}
// ===== SCRAPING-FUNKTION FÜR FORMULARE =====
async function scrapeData(client, cookieString, socketId) {
    try {
        if (socketId && io) {
            io.to(socketId).emit('scrape-status', { message: 'Lade Formulare...' });
        }
        console.log('[SCRAPE] Starte Scraping von:', SCRAPE_URL);
        const response = await client.get(SCRAPE_URL, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': cookieString
            }
        });
        if (response.status !== 200) {
            return { success: false, error: `HTTP ${response.status}` };
        }
        // Berechne Datengröße
        const dataSizeBytes = Buffer.byteLength(response.data, 'utf8');
        const dataSizeMB = (dataSizeBytes / (1024 * 1024)).toFixed(2);
        // Parse HTML
        const $ = cheerio.load(response.data);
        const tableData = [];
        // Finde alle Tabellenzeilen
        $('table tr').each((index, element) => {
            if (index === 0)
                return; // Überspringe Header
            const row = {};
            $(element).find('td').each((cellIndex, cell) => {
                const text = $(cell).text().trim();
                // Dynamische Spaltennamen basierend auf dem Header
                switch (cellIndex) {
                    case 0:
                        row.id = text;
                        break;
                    case 1:
                        row.datum = text;
                        break;
                    case 2:
                        row.kategorie = text;
                        break;
                    case 3:
                        row.mitarbeiter = text;
                        break;
                    case 4:
                        row.name = text;
                        break;
                    case 5:
                        row.plz = text;
                        break;
                    case 6:
                        row.anbieter = text;
                        break;
                    case 7:
                        row.firma = text;
                        break;
                    case 8:
                        row.kdnr = text;
                        break;
                    case 9:
                        row.wechsel = text;
                        break;
                    case 10:
                        row.strom = text;
                        break;
                    case 11:
                        row.gas = text;
                        break;
                    case 12:
                        row.frist = text;
                        break;
                    case 13:
                        row.aktion = text;
                        break;
                    default: break;
                }
            });
            if (Object.keys(row).length > 0) {
                tableData.push(row);
            }
        });
        console.log('[SCRAPE] Gefunden:', tableData.length, 'Datensätze');
        console.log('[SCRAPE] Datengröße:', dataSizeMB, 'MB');
        return {
            success: true,
            count: tableData.length,
            size: dataSizeMB,
            data: tableData
        };
    }
    catch (error) {
        console.error('[SCRAPE] Fehler:', error.message);
        return { success: false, error: error.message };
    }
}
// ===== SCRAPING-FUNKTION FÜR MITARBEITER =====
async function scrapeMitarbeiter(client, cookieString) {
    try {
        console.log('[SCRAPE] Starte Scraping von Mitarbeitern:', MITARBEITER_URL);
        const response = await client.get(MITARBEITER_URL, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': cookieString
            }
        });
        if (response.status !== 200) {
            console.log('[SCRAPE] Mitarbeiter HTTP Status:', response.status);
            return { success: false, error: `HTTP ${response.status}` };
        }
        // Parse HTML
        const $ = cheerio.load(response.data);
        const mitarbeiterData = [];
        // Finde Header-Spalten zuerst - suche speziell nach thead oder erster th-Zeile
        let fuehrungskraftIndex = -1;
        let registratorIndex = -1;
        let nameIndex = -1;
        let vornameIndex = -1;
        let aktionIndex = -1;
        let gpnrIndex = -1;
        let vcnrIndex = -1;
        // Versuche zuerst thead
        let headerFound = false;
        $('table thead tr th').each((index, element) => {
            const headerText = $(element).text().trim().toLowerCase();
            if (headerText) {
                headerFound = true;
                console.log(`[SCRAPE] Header ${index}:`, headerText);
                if (headerText.includes('führung'))
                    fuehrungskraftIndex = index;
                if (headerText.includes('registrator'))
                    registratorIndex = index;
                if (headerText.includes('nachname') || (headerText.includes('name') && !headerText.includes('vorname')))
                    nameIndex = index;
                if (headerText.includes('vorname'))
                    vornameIndex = index;
                if (headerText.includes('aktion'))
                    aktionIndex = index;
                if (headerText === 'gpnr')
                    gpnrIndex = index;
                if (headerText === 'vcnr')
                    vcnrIndex = index;
            }
        });
        // Fallback: Wenn kein thead, suche erste Zeile mit th
        if (!headerFound) {
            $('table tr').first().find('th, td').each((index, element) => {
                const headerText = $(element).text().trim().toLowerCase();
                console.log(`[SCRAPE] Header ${index}:`, headerText);
                if (headerText.includes('führung'))
                    fuehrungskraftIndex = index;
                if (headerText.includes('registrator'))
                    registratorIndex = index;
                if (headerText.includes('nachname') || (headerText.includes('name') && !headerText.includes('vorname')))
                    nameIndex = index;
                if (headerText.includes('vorname'))
                    vornameIndex = index;
                if (headerText.includes('aktion'))
                    aktionIndex = index;
                if (headerText === 'gpnr')
                    gpnrIndex = index;
                if (headerText === 'vcnr')
                    vcnrIndex = index;
            });
        }
        console.log('[SCRAPE] Führungskraft-Index:', fuehrungskraftIndex);
        console.log('[SCRAPE] Registrator-Index:', registratorIndex);
        console.log('[SCRAPE] Name-Index:', nameIndex);
        console.log('[SCRAPE] Vorname-Index:', vornameIndex);
        console.log('[SCRAPE] Aktion-Index:', aktionIndex);
        console.log('[SCRAPE] GPNR-Index:', gpnrIndex);
        console.log('[SCRAPE] VCNR-Index:', vcnrIndex);
        // Finde alle Datenzeilen
        let rowCount = 0;
        let skippedHeaderRows = 0;
        $('table tbody tr, table tr').each((index, element) => {
            // Überspringe Header-Zeilen (mit th)
            const hasHeaderCells = $(element).find('th').length > 0;
            if (hasHeaderCells) {
                skippedHeaderRows++;
                return;
            }
            // Überspringe erste Zeile wenn kein tbody vorhanden
            if (index === 0 && !headerFound) {
                skippedHeaderRows++;
                return;
            }
            const cells = $(element).find('td');
            if (cells.length === 0)
                return;
            const row = {};
            cells.each((cellIndex, cell) => {
                const text = $(cell).text().trim();
                const cellHtml = $(cell).html() || '';
                if (nameIndex >= 0 && cellIndex === nameIndex) {
                    row.name = text;
                }
                if (vornameIndex >= 0 && cellIndex === vornameIndex) {
                    row.vorname = text;
                }
                if (fuehrungskraftIndex >= 0 && cellIndex === fuehrungskraftIndex) {
                    row.fuehrungskraft = text;
                }
                if (registratorIndex >= 0 && cellIndex === registratorIndex) {
                    row.registrator = text;
                }
                if (aktionIndex >= 0 && cellIndex === aktionIndex) {
                    // Prüfe auf Aktionen (Leads/Formulare) - schaue nach Buttons/Links
                    row.hasLeads = cellHtml.toLowerCase().includes('leads');
                    row.hasFormulare = cellHtml.toLowerCase().includes('formulare');
                    row.hasAnyAction = row.hasLeads || row.hasFormulare;
                    // Extrahiere die Mitarbeiter-ID aus dem Formulare-Link
                    if (row.hasFormulare) {
                        const match = cellHtml.match(/mitarbeiter=(\d+)/);
                        if (match) {
                            row.mitarbeiterId = match[1];
                        }
                    }
                }
                if (gpnrIndex >= 0 && cellIndex === gpnrIndex) {
                    row.gpnr = text;
                }
                if (vcnrIndex >= 0 && cellIndex === vcnrIndex) {
                    row.vcnr = text;
                }
                // Fallback: Speichere alle Spalten mit Index
                row[`col_${cellIndex}`] = text;
            });
            if (Object.keys(row).length > 0) {
                mitarbeiterData.push(row);
                rowCount++;
                // Log erste paar Zeilen zur Diagnose
                if (rowCount <= 3) {
                    console.log('[SCRAPE] Zeile', rowCount, ':', JSON.stringify(row));
                }
            }
        });
        console.log('[SCRAPE] Header-Zeilen übersprungen:', skippedHeaderRows);
        console.log('[SCRAPE] Mitarbeiter gesamt gefunden:', mitarbeiterData.length);
        // Filtere nur Mitarbeiter mit Aktionen
        const mitarbeiterMitAktionen = mitarbeiterData.filter(ma => ma.hasAnyAction === true);
        console.log('[SCRAPE] Mitarbeiter mit Aktionen:', mitarbeiterMitAktionen.length);
        // Debug: Wie viele haben welche Aktionen
        const mitFormulare = mitarbeiterMitAktionen.filter(ma => ma.hasFormulare).length;
        const mitLeads = mitarbeiterMitAktionen.filter(ma => ma.hasLeads).length;
        console.log('[SCRAPE] - davon mit Formulare-Aktion:', mitFormulare);
        console.log('[SCRAPE] - davon mit Leads-Aktion:', mitLeads);
        // Speichere HTML für Debugging (erste 2000 Zeichen)
        console.log('[SCRAPE] HTML Preview:', response.data.substring(0, 2000));
        // Keine Verifizierung beim initialen Scraping mehr
        // Verifizierung erfolgt on-demand beim Filtern über /api/verify
        return {
            success: true,
            data: mitarbeiterMitAktionen,
            totalCount: mitarbeiterData.length,
            activeCount: mitarbeiterMitAktionen.length
        };
    }
    catch (error) {
        console.error('[SCRAPE] Mitarbeiter-Fehler:', error.message);
        console.error('[SCRAPE] Stack:', error.stack);
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
            io.to(socketId).emit('scrape-status', { message: 'Login wird durchgeführt...' });
        }
        // Login durchführen
        const loginResult = await performLogin(client, jar, gpnr, password);
        if (!loginResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('scrape-error', { error: loginResult.error });
            }
            return res.status(401).json({
                success: false,
                error: loginResult.error
            });
        }
        // Formulare scrapen
        const scrapeResult = await scrapeData(client, loginResult.cookieString, socketId);
        if (!scrapeResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('scrape-error', { error: scrapeResult.error });
            }
            return res.status(500).json({
                success: false,
                error: scrapeResult.error
            });
        }
        // Mitarbeiter scrapen
        if (socketId && io) {
            io.to(socketId).emit('scrape-status', { message: 'Lade Mitarbeiter...' });
        }
        const mitarbeiterResult = await scrapeMitarbeiter(client, loginResult.cookieString);
        if (!mitarbeiterResult.success) {
            console.log('[SCRAPE] Warnung: Mitarbeiter konnten nicht geladen werden:', mitarbeiterResult.error);
        }
        else {
            console.log('[SCRAPE] Mitarbeiter erfolgreich geladen:', mitarbeiterResult.data.length);
        }
        const mitarbeiterData = mitarbeiterResult.success ? mitarbeiterResult.data : [];
        // Socket.io Update mit Mitarbeiter-Daten
        if (socketId && io) {
            io.to(socketId).emit('scrape-complete', {
                count: scrapeResult.count,
                size: scrapeResult.size,
                data: scrapeResult.data,
                mitarbeiterData: mitarbeiterData
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
            mitarbeiterData: mitarbeiterData,
            message: 'Scraping erfolgreich abgeschlossen'
        });
    }
    catch (error) {
        console.error('[API] Fehler:', error.message);
        if (req.body.socketId && io) {
            io.to(req.body.socketId).emit('scrape-error', { error: error.message });
        }
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
exports.default = router;

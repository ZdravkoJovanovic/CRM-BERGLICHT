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
// TLS-Zertifikatsprüfung deaktivieren
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
const PROJECT_BASE_URL = 'https://project-5.at';
const setSocketIO = (socketIO) => {
    io = socketIO;
};
exports.setSocketIO = setSocketIO;
// Supabase Client
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
async function scrapeTelefonnummer(client, cookieString, userId) {
    try {
        const url = `${PROJECT_BASE_URL}/mitarbeiter_bearbeiten.php?id=${userId}`;
        const response = await client.get(url, {
            headers: {
                ...getBrowserHeaders(PROJECT_BASE_URL),
                'Cookie': cookieString
            }
        });
        if (response.status !== 200) {
            return '';
        }
        const $ = cheerio.load(response.data);
        let telefon = '';
        // Suche nach Telefon-Feld
        $('label, th, td, div, span').each((i, elem) => {
            const text = $(elem).text().toLowerCase().trim();
            if (text === 'telefon' || text === 'tel' || text === 'phone' || text.includes('telefonnummer')) {
                const parent = $(elem).parent();
                const input = parent.find('input').first();
                if (input.length > 0) {
                    telefon = input.val() || '';
                    if (telefon)
                        return false; // Stop loop
                }
                const nextTd = $(elem).next('td');
                if (nextTd.length > 0) {
                    const val = nextTd.text().trim();
                    if (val && val.length > 3) {
                        telefon = val;
                        return false;
                    }
                }
            }
        });
        // Fallback: Suche input mit name="telefon"
        if (!telefon) {
            const telInput = $('input[name="telefon"], input[name="phone"], input[name="tel"]').first();
            if (telInput.length > 0) {
                telefon = telInput.val() || '';
            }
        }
        return telefon.trim();
    }
    catch (error) {
        console.error('[TELEFON] Fehler:', error.message);
        return '';
    }
}
const GPNR = '60235';
const PASSWORD = 'r87cucd';
// API Route: Hole Telefonnummern für Mitarbeiter
router.post('/fetch-numbers', async (req, res) => {
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
            io.to(socketId).emit('telefon-status', { message: 'Login wird durchgefuehrt...' });
        }
        const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
        if (!loginResult.success) {
            if (socketId && io) {
                io.to(socketId).emit('telefon-error', { error: loginResult.error });
            }
            return res.status(401).json({ success: false, error: loginResult.error });
        }
        // Hole Telefonnummern
        const results = [];
        let processed = 0;
        for (const userId of userIds) {
            if (socketId && io) {
                processed++;
                io.to(socketId).emit('telefon-progress', {
                    current: processed,
                    total: userIds.length,
                    message: `Lade Telefonnummer ${processed}/${userIds.length}...`
                });
            }
            const telefon = await scrapeTelefonnummer(client, loginResult.cookieString, userId);
            if (telefon) {
                results.push({ user_id: userId, telefon: telefon });
                console.log('[TELEFON] user_id', userId, ':', telefon);
                // Update in Supabase
                try {
                    const supabaseClient = getSupabaseClient();
                    await supabaseClient
                        .from('mitarbeiter_organigramm')
                        .update({ telefon: telefon })
                        .eq('user_id', userId);
                }
                catch (error) {
                    console.error('[TELEFON] Supabase Update Fehler:', error.message);
                }
            }
            // Kleine Pause um Server nicht zu überlasten
            await new Promise(resolve => setTimeout(resolve, 300));
        }
        if (socketId && io) {
            io.to(socketId).emit('telefon-complete', {
                total: results.length,
                results: results
            });
        }
        res.json({
            success: true,
            count: results.length,
            results: results
        });
    }
    catch (error) {
        console.error('[TELEFON API] Fehler:', error.message);
        if (socketId && io) {
            io.to(socketId).emit('telefon-error', { error: error.message });
        }
        res.status(500).json({ success: false, error: error.message });
    }
});
exports.default = router;

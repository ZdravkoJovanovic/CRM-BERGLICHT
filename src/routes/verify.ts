import { Router, Request, Response } from 'express';
import axios from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import * as cheerio from 'cheerio';
import { Server } from 'socket.io';

const router = Router();
let io: Server;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const PROJECT_BASE_URL = 'https://project-5.at';
const GPNR = '60235';
const PASSWORD = 'r87cucd';

export const setSocketIO = (socketIO: Server) => {
  io = socketIO;
};

const getBrowserHeaders = (referer?: string) => {
  const headers: any = {
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

const getCookieString = (jar: CookieJar, url: string): string | null => {
  try {
    const cookies = jar.getCookiesSync(url);
    if (cookies.length === 0) return null;
    return cookies.map((c: any) => `${c.key}=${c.value}`).join('; ');
  } catch {
    return null;
  }
};

const extractFormData = (html: string, username: string, password: string) => {
  const $ = cheerio.load(html);
  const form = $('form').first();
  const loginData: Record<string, string> = {};
  
  form.find('input[type="hidden"]').each((i, elem) => {
    const name = $(elem).attr('name');
    const value = $(elem).attr('value') || '';
    if (name) loginData[name] = value;
  });
  
  loginData['gpnr'] = username;
  loginData['passwort'] = password;
  
  return { data: loginData, action: form.attr('action') || null };
};

async function performLogin(client: any, jar: CookieJar, username: string, password: string): Promise<any> {
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
      : formAction.startsWith('http') ? formAction : PROJECT_BASE_URL + formAction;
    
    await client.post(loginUrl, new URLSearchParams(formData).toString(), {
      headers: {
        ...getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`),
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': `${PROJECT_BASE_URL}/index.php`,
        'Origin': PROJECT_BASE_URL
      }
    });
    
    const cookieStr = getCookieString(jar, PROJECT_BASE_URL);
    
    if (cookieStr) {
      return { success: true, cookieString: cookieStr };
    } else {
      return { success: false, error: 'Kein Cookie erhalten' };
    }
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}

async function performLogout(client: any, jar: CookieJar): Promise<void> {
  try {
    await client.get(`${PROJECT_BASE_URL}/logout.php`, {
      headers: getBrowserHeaders(`${PROJECT_BASE_URL}/index.php`)
    });
    jar.removeAllCookiesSync();
  } catch (error: any) {
    console.log('[AUTH] Logout-Fehler:', error.message);
  }
}

router.post('/', async (req: Request, res: Response) => {
  const { mitarbeiterIds, socketId, hauptMitarbeiterId } = req.body;
  
  if (!mitarbeiterIds || !Array.isArray(mitarbeiterIds)) {
    return res.status(400).json({
      success: false,
      error: 'Mitarbeiter-IDs fehlen'
    });
  }
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    validateStatus: () => true,
    maxRedirects: 5,
    timeout: 30000
  }));
  
  try {
    console.log('\n\x1b[35m' + '█'.repeat(80) + '\x1b[0m');
    console.log('\x1b[35m█ VERIFIZIERUNG GESTARTET - ' + mitarbeiterIds.length + ' Mitarbeiter ████████████████████████████████████\x1b[0m');
    console.log('\x1b[35m' + '█'.repeat(80) + '\x1b[0m\n');
    
    // Login
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    
    if (!loginResult.success) {
      return res.status(401).json({
        success: false,
        error: loginResult.error
      });
    }
    
    let verifiedFormulareCount = 0;
    let verifiedMitarbeiterCount = 0;
    const verifiedDetails: any[] = [];
    const leadsPhoneMap: Map<string, string> = new Map();
    
    // Lade auch Leads des Hauptmitarbeiters (der gefilterte Mitarbeiter selbst)
    if (hauptMitarbeiterId) {
      console.log('\n\x1b[33m=== LADE LEADS VOM HAUPTMITARBEITER ===\x1b[0m');
      console.log(`[HAUPT] ID: ${hauptMitarbeiterId.id}, Name: ${hauptMitarbeiterId.name}`);
      
      try {
        const leadsUrl = `${PROJECT_BASE_URL}/leads_mitarbeiter.php?mitarbeiter=${hauptMitarbeiterId.id}`;
        const leadsResponse = await client.get(leadsUrl, {
          headers: {
            ...getBrowserHeaders(PROJECT_BASE_URL),
            'Cookie': loginResult.cookieString
          }
        });
        
        if (leadsResponse.status === 200) {
          const $leads = cheerio.load(leadsResponse.data);
          
          let hauptLeadsFound = 0;
          $leads('table tr').each((i, el) => {
            if (i === 0 || i === 1) return;
            
            const cells = $leads(el).find('td');
            if (cells.length === 0) return;
            
            const nameField = $leads(cells.eq(3)).text();
            if (!nameField) return;
            
            const cleanField = nameField.replace(/\s+/g, ' ').trim();
            const phoneMatch = cleanField.match(/[\+\d][\d\s\-()\/]+/g);
            
            if (phoneMatch && phoneMatch.length > 0) {
              const phone = phoneMatch.sort((a, b) => b.length - a.length)[0].trim().replace(/\s+/g, '');
              const kundenName = cleanField.split(/[\+]|(?=\d{5,})/)[0].trim();
              
              if (kundenName && phone.length >= 7) {
                leadsPhoneMap.set(kundenName.toLowerCase(), phone);
                hauptLeadsFound++;
              }
            }
          });
          
          console.log(`\x1b[33m[HAUPT] ${hauptLeadsFound} Telefonnummern vom Hauptmitarbeiter\x1b[0m`);
        }
      } catch (err) {
        console.log('[HAUPT] Fehler beim Laden der Leads');
      }
    }
    
    for (const mitarbeiterInfo of mitarbeiterIds) {
      const mitarbeiterId = mitarbeiterInfo.id;
      
      if (!mitarbeiterId) continue;
      
      try {
        // 1. Lade Leads des Mitarbeiters
        const leadsUrl = `${PROJECT_BASE_URL}/leads_mitarbeiter.php?mitarbeiter=${mitarbeiterId}`;
        
        console.log('\n\x1b[36m--- REQUEST START (LEADS) ---\x1b[0m');
        console.log(`[REQUEST] URL: ${leadsUrl}`);
        console.log(`[REQUEST] Mitarbeiter: ${mitarbeiterInfo.name}`);
        
        const leadsResponse = await client.get(leadsUrl, {
          headers: {
            ...getBrowserHeaders(PROJECT_BASE_URL),
            'Cookie': loginResult.cookieString
          }
        });
        
        console.log(`[RESPONSE] Status: ${leadsResponse.status}`);
        
        if (leadsResponse.status === 200) {
          const $leads = cheerio.load(leadsResponse.data);
          
          // Finde Name-Spalten-Index
          let leadsNameIndex = -1;
          $leads('table thead tr th, table tr:first-child th, table tr:first-child td').each((idx, el) => {
            const headerText = $leads(el).text().trim().toLowerCase();
            if (headerText.includes('name')) {
              leadsNameIndex = idx;
            }
          });
          
          console.log(`[LEADS] Name-Spalten-Index: ${leadsNameIndex}`);
          
          // Debug: Zeige erste Zeilen komplett
          console.log('[LEADS] Erste 3 Zeilen der Tabelle:');
          $leads('table tr').slice(0, 3).each((rowIdx, row) => {
            console.log(`Zeile ${rowIdx}:`);
            $leads(row).find('th, td').each((colIdx, cell) => {
              const text = $leads(cell).text().trim();
              if (text) console.log(`  [${colIdx}]: "${text}"`);
            });
          });
          
          // Extrahiere Leads mit Name und Telefonnummer
          let leadsFound = 0;
          let leadsProcessed = 0;
          $leads('table tr').each((i, el) => {
            // Überspringe erste 2 Zeilen (Summe + Header)
            if (i === 0 || i === 1) return;
            
            leadsProcessed++;
            
            const cells = $leads(el).find('td');
            if (cells.length === 0) return;
            
            // Name ist immer in Spalte 3 - verwende .text() um auch Newlines zu bekommen
            const nameField = $leads(cells.eq(3)).text();
            
            if (!nameField) return;
            
            // Log erste 5 Einträge zur Diagnose
            if (leadsProcessed <= 5) {
              console.log(`[LEADS] Zeile ${leadsProcessed}: "${nameField.replace(/\n/g, '\\n')}"`);
            }
            
            // Entferne Whitespace aber behalte den Text zusammen
            const cleanField = nameField.replace(/\s+/g, ' ').trim();
            
            // Extrahiere Telefonnummer (beginnend mit + oder Ziffer)
            const phoneMatch = cleanField.match(/[\+\d][\d\s\-()\/]+/g);
            
            if (phoneMatch && phoneMatch.length > 0) {
              // Nimm längste Nummer und bereinige sie
              const phone = phoneMatch.sort((a, b) => b.length - a.length)[0].trim().replace(/\s+/g, '');
              
              // Extrahiere Kundenname (alles vor dem ersten + oder der ersten langen Zahlenfolge)
              const kundenName = cleanField.split(/[\+]|(?=\d{5,})/)[0].trim();
              
              if (kundenName && phone.length >= 7) {
                leadsPhoneMap.set(kundenName.toLowerCase(), phone);
                leadsFound++;
                console.log(`[LEADS] ✓ "${kundenName}" -> ${phone}`);
              }
            } else {
              // Kein Telefon gefunden
              if (leadsProcessed <= 5) {
                console.log(`[LEADS] ✗ Kein Telefon in: "${cleanField}"`);
              }
            }
          });
          
          console.log(`[LEADS] Verarbeitet: ${leadsProcessed} Zeilen`);
          console.log(`[LEADS] ${leadsFound} Leads mit Telefonnummern gefunden`);
        }
        
        console.log('\x1b[36m--- REQUEST END (LEADS) ---\x1b[0m');
        
        // 2. Lade Formulare des Mitarbeiters
        const formularUrl = `${PROJECT_BASE_URL}/formularuebersicht.php?mitarbeiter=${mitarbeiterId}`;
        
        console.log('\n\x1b[36m--- REQUEST START (FORMULARE) ---\x1b[0m');
        console.log(`[REQUEST] URL: ${formularUrl}`);
        
        const formularResponse = await client.get(formularUrl, {
          headers: {
            ...getBrowserHeaders(PROJECT_BASE_URL),
            'Cookie': loginResult.cookieString
          }
        });
        
        console.log(`[RESPONSE] Status: ${formularResponse.status}`);
        console.log(`[RESPONSE] Content-Length: ${formularResponse.data?.length || 0} Zeichen`);
        
        if (formularResponse.status === 200) {
          const $form = cheerio.load(formularResponse.data);
          
          let formularCount = 0;
          $form('table tr').each((i, el) => {
            const cells = $form(el).find('td');
            if (cells.length > 5) {
              formularCount++;
            }
          });
          
          console.log(`[RESPONSE] Formulare gefunden: ${formularCount}`);
          console.log('\x1b[36m--- REQUEST END (FORMULARE) ---\x1b[0m');
          
          if (formularCount > 0) {
            verifiedFormulareCount += formularCount;
            verifiedMitarbeiterCount++;
            verifiedDetails.push({
              name: mitarbeiterInfo.name,
              id: mitarbeiterId,
              count: formularCount
            });
            console.log(`\x1b[32m[SUCCESS] ${mitarbeiterInfo.name}: ${formularCount} Formulare\x1b[0m`);
            
            if (socketId && io) {
              io.to(socketId).emit('verify-progress', {
                current: verifiedMitarbeiterCount,
                total: mitarbeiterIds.length,
                totalFormulare: verifiedFormulareCount
              });
            }
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 150));
        
      } catch (err: any) {
        console.log(`\x1b[31m[ERROR] ${mitarbeiterInfo.name}: ${err.message}\x1b[0m`);
      }
    }
    
    await performLogout(client, jar);
    
    console.log('\n\x1b[35m' + '█'.repeat(80) + '\x1b[0m');
    console.log(`\x1b[35m█ VERIFIZIERUNG ABGESCHLOSSEN: ${verifiedMitarbeiterCount} MA | ${verifiedFormulareCount} Formulare ██████████████████████\x1b[0m`);
    console.log('\x1b[35m' + '█'.repeat(80) + '\x1b[0m\n');
    
    // Konvertiere Map zu Array für JSON
    const phoneData = Array.from(leadsPhoneMap.entries()).map(([name, phone]) => ({ name, phone }));
    
    console.log('[VERIFY] Telefonnummern gefunden:', phoneData.length);
    
    res.json({
      success: true,
      verifiedFormulareCount,
      verifiedMitarbeiterCount,
      details: verifiedDetails,
      phoneData
    });
    
  } catch (error: any) {
    console.error('[VERIFY] Fehler:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

export default router;


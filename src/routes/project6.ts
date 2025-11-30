import { Router, Request, Response } from 'express';
import axios, { AxiosInstance } from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import * as cheerio from 'cheerio';
import { Server } from 'socket.io';
import * as fs from 'fs';
import * as path from 'path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const router = Router();
let io: Server;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const PROJECT_BASE_URL = 'https://project-5.at';

// AWS S3 Configuration
const s3Client = new S3Client({
  region: 'eu-north-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
  },
  maxAttempts: 2
});

const S3_BUCKET_BK_MIT_PDFS = 'crm-berglicht-bk-formulare-mit-pdfs';
const S3_BUCKET_E_FORMULARE = 'crm-berglicht-e-formulare-mit-fotos';
const S3_BUCKET_MITARBEITER = 'crm-berglicht-mitarbeiter-liste';
const S3_BUCKET_LEADS = 'crm-berglicht-leads-formulare';

export const setSocketIO = (socketIO: Server) => {
  io = socketIO;
};

const getBrowserHeaders = (referer?: string) => {
  const headers: any = {
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

async function performLogin(client: AxiosInstance, jar: CookieJar, username: string, password: string): Promise<any> {
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
    
    const cookieString = getCookieString(jar, PROJECT_BASE_URL);
    if (!cookieString) {
      return { success: false, error: 'Keine Cookies nach Login' };
    }
    
    return { success: true, cookieString };
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}

// Route: Scrape Mitarbeiter-Tabelle
router.post('/scrape-mitarbeiter', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  const GPNR = '60235';
  const PASSWORD = 'r87cucd';
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    validateStatus: () => true,
    maxRedirects: 5,
    timeout: 60000
  }));
  
  try {
    if (socketId && io) {
      io.to(socketId).emit('project6-scrape-status', { message: 'Login...', progress: 0 });
    }
    
    console.log('[PROJECT-6] Starte Mitarbeiter-Scraping...');
    
    // Login
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) {
        io.to(socketId).emit('project6-scrape-error', { error: loginResult.error });
      }
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-scrape-status', { message: 'Lade Mitarbeiter...', progress: 20 });
    }
    
    // Scrape mitarbeiter.php
    const url = 'https://project-5.at/mitarbeiter.php';
    const response = await client.get(url, {
      headers: {
        ...getBrowserHeaders(PROJECT_BASE_URL),
        'Cookie': loginResult.cookieString
      }
    });
    
    if (response.status !== 200) {
      const error = `HTTP ${response.status}`;
      if (socketId && io) {
        io.to(socketId).emit('project6-scrape-error', { error });
      }
      return res.status(500).json({ success: false, error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-scrape-status', { message: 'Parse Tabelle...', progress: 50 });
    }
    
    const $ = cheerio.load(response.data);
    const mitarbeiter: any[] = [];
    
    console.log('[PROJECT-6] Parse Tabelle...');
    
    // Parse Tabelle - finde die richtige Tabelle
    $('table').each((tableIndex, table) => {
      const headers: string[] = [];
      
      // Prüfe ob es die richtige Tabelle ist (mit GPNr Header)
      $(table).find('tr').first().find('th, td').each((i, th) => {
        headers.push($(th).text().trim());
      });
      
      if (!headers.includes('GPNr')) return; // Skip diese Tabelle
      
      console.log('[PROJECT-6] Gefundene Header:', headers);
      
      // Parse alle Zeilen (skip header)
      $(table).find('tr').each((index, row) => {
        if (index === 0) return; // Skip header
        
        const cells = $(row).find('td');
        if (cells.length === 0) return;
        
        // Extrahiere ALLE Links aus der gesamten Zeile
        let leadsLink = '';
        let formulareLink = '';
        
        // Durchsuche ALLE <a> Tags in der Zeile
        $(row).find('a').each((i, link) => {
          const href = $(link).attr('href') || '';
          
          // Prüfe ob es ein Leads-Link ist
          if (href.includes('leads_mitarbeiter.php')) {
            leadsLink = href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`;
          }
          
          // Prüfe ob es ein Formulare-Link ist
          if (href.includes('formularuebersicht')) {
            formulareLink = href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`;
          }
        });
        
        const mitarbeiterData = {
          gpnr: cells.eq(0).text().trim(),
          vcnr: cells.eq(1).text().trim(),
          cisnr: cells.eq(2).text().trim(),
          nachname: cells.eq(3).text().trim(),
          vorname: cells.eq(4).text().trim(),
          email: cells.eq(5).text().trim(),
          fuehrungskraft: cells.eq(6).text().trim(),
          registrator: cells.eq(7).text().trim(),
          leads: cells.eq(8).text().trim(),
          leads_link: leadsLink,
          formulare_link: formulareLink
        };
        
        mitarbeiter.push(mitarbeiterData);
      });
    });
    
    console.log('[PROJECT-6] Mitarbeiter gefunden:', mitarbeiter.length);
    
    // Debug: Zeige erste 3 Mitarbeiter mit Links
    if (mitarbeiter.length > 0) {
      console.log('[PROJECT-6] Beispiel-Daten:');
      mitarbeiter.slice(0, 3).forEach(m => {
        console.log(`  - ${m.vorname} ${m.nachname} (${m.gpnr}): Leads=${m.leads}, Link=${m.leads_link}`);
      });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-scrape-status', { 
        message: 'Erstelle CSV...', 
        progress: 80,
        count: mitarbeiter.length
      });
    }
    
    // Konvertiere zu CSV
    if (mitarbeiter.length > 0) {
      const headers = [
        'GPNr', 'VCNr', 'CisNr', 'Nachname', 'Vorname', 
        'E-Mail', 'Führungskraft', 'Registrator', 'Leads',
        'Leads Link', 'Formulare Link'
      ];
      
      const csvRows = [
        headers.join(','), // Header-Zeile
        ...mitarbeiter.map(row => {
          return [
            row.gpnr,
            row.vcnr,
            row.cisnr,
            row.nachname,
            row.vorname,
            row.email,
            row.fuehrungskraft,
            row.registrator,
            row.leads,
            row.leads_link,
            row.formulare_link
          ].map(value => {
            // Escape Kommas und Anführungszeichen
            return `"${String(value || '').replace(/"/g, '""')}"`;
          }).join(',');
        })
      ];
      
      const csvContent = csvRows.join('\n');
      
      if (socketId && io) {
        io.to(socketId).emit('project6-scrape-complete', { 
          count: mitarbeiter.length,
          progress: 100
        });
      }
      
      // Setze Content-Type für Download
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="mitarbeiter-${new Date().toISOString().split('T')[0]}.csv"`);
      res.send('\uFEFF' + csvContent); // UTF-8 BOM für Excel
    } else {
      const error = 'Keine Mitarbeiter gefunden';
      if (socketId && io) {
        io.to(socketId).emit('project6-scrape-error', { error });
      }
      res.json({ success: false, error });
    }
    
  } catch (error: any) {
    console.error('[PROJECT-6] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('project6-scrape-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

// Route: Scrape Leads für ALLE Mitarbeiter mit Leads
router.post('/scrape-leads', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  const GPNR = '60235';
  const PASSWORD = 'r87cucd';
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    validateStatus: () => true,
    maxRedirects: 5,
    timeout: 60000
  }));
  
  try {
    if (socketId && io) {
      io.to(socketId).emit('project6-leads-status', { message: 'Login...', progress: 0 });
    }
    
    console.log('[PROJECT-6 LEADS] Starte für ALLE Mitarbeiter mit Leads...');
    
    // Login
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) {
        io.to(socketId).emit('project6-leads-error', { error: loginResult.error });
      }
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-leads-status', { message: 'Lade Mitarbeiter...', progress: 10 });
    }
    
    // SCHRITT 1: Scrape mitarbeiter.php um ALLE Mitarbeiter mit Leads zu finden
    const mitarbeiterUrl = 'https://project-5.at/mitarbeiter.php';
    const mitarbeiterResponse = await client.get(mitarbeiterUrl, {
      headers: {
        ...getBrowserHeaders(PROJECT_BASE_URL),
        'Cookie': loginResult.cookieString
      }
    });
    
    if (mitarbeiterResponse.status !== 200) {
      const error = `HTTP ${mitarbeiterResponse.status} bei mitarbeiter.php`;
      if (socketId && io) {
        io.to(socketId).emit('project6-leads-error', { error });
      }
      return res.status(500).json({ success: false, error });
    }
    
    // Parse Mitarbeiter-Tabelle um ALLE Mitarbeiter mit Leads zu finden
    const $mitarbeiter = cheerio.load(mitarbeiterResponse.data);
    const mitarbeiterMitLeads: any[] = [];
    
    console.log('[PROJECT-6 LEADS] Parse Mitarbeiter-Tabelle...');
    
    $mitarbeiter('table tr').each((index, row) => {
      if (index === 0) return; // Skip header
      
      const cells = $mitarbeiter(row).find('td');
      if (cells.length === 0) return;
      
      // Extrahiere Mitarbeiter-Daten
      const gpnr = cells.eq(0).text().trim();
      const vcnr = cells.eq(1).text().trim();
      const cisnr = cells.eq(2).text().trim();
      const nachname = cells.eq(3).text().trim();
      const vorname = cells.eq(4).text().trim();
      const leadsCount = parseInt(cells.eq(8).text().trim()) || 0;
      
      // Nur Mitarbeiter mit Leads > 0
      if (leadsCount > 0) {
        let leadsUrl = '';
        let formulareUrl = '';
        
        // Suche Links in dieser Zeile
        $mitarbeiter(row).find('a').each((i, link) => {
          const href = $mitarbeiter(link).attr('href') || '';
          
          if (href.includes('leads_mitarbeiter.php')) {
            leadsUrl = href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`;
          }
          
          if (href.includes('formularuebersicht')) {
            formulareUrl = href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`;
          }
        });
        
        mitarbeiterMitLeads.push({
          gpnr: gpnr || '',
          vcnr: vcnr || '',
          cisnr: cisnr || '',
          vorname,
          nachname,
          leadsCount,
          leadsUrl,
          formulareUrl
        });
      }
    });
    
    console.log('[PROJECT-6 LEADS] Mitarbeiter mit Leads gefunden:', mitarbeiterMitLeads.length);
    
    if (mitarbeiterMitLeads.length === 0) {
      const error = 'Keine Mitarbeiter mit Leads gefunden';
      if (socketId && io) {
        io.to(socketId).emit('project6-leads-error', { error });
      }
      return res.status(404).json({ success: false, error });
    }
    
    // SCHRITT 2: Iteriere durch ALLE Mitarbeiter mit Leads
    let totalLeadsCount = 0;
    let totalFormulareCount = 0;
    let totalMitarbeiterProcessed = 0;
    const baseDir = path.join(__dirname, '../../Mitarbeiter_Leads_Und_Formulare');
    
    // Erstelle Basis-Ordner falls nicht vorhanden
    if (!fs.existsSync(baseDir)) {
      fs.mkdirSync(baseDir, { recursive: true });
    }
    
    for (let i = 0; i < mitarbeiterMitLeads.length; i++) {
      const mitarbeiter = mitarbeiterMitLeads[i];
      
      // Erstelle Ordnernamen: GPNr_VCNr_CisNr_Vorname_Nachname (nur vorhandene Werte)
      const nameParts = [];
      if (mitarbeiter.gpnr) nameParts.push(mitarbeiter.gpnr);
      if (mitarbeiter.vcnr) nameParts.push(mitarbeiter.vcnr);
      if (mitarbeiter.cisnr) nameParts.push(mitarbeiter.cisnr);
      nameParts.push(mitarbeiter.vorname);
      nameParts.push(mitarbeiter.nachname);
      
      const folderName = nameParts.join('_').replace(/\s/g, '_');
      const mitarbeiterDir = path.join(baseDir, folderName);
      
      if (!fs.existsSync(mitarbeiterDir)) {
        fs.mkdirSync(mitarbeiterDir, { recursive: true });
      }
      
      const progress = Math.round(10 + (i / mitarbeiterMitLeads.length) * 85);
      if (socketId && io) {
        io.to(socketId).emit('project6-leads-status', { 
          message: `${mitarbeiter.vorname} ${mitarbeiter.nachname} (${i + 1}/${mitarbeiterMitLeads.length})`, 
          progress: progress
        });
      }
      
      console.log(`[PROJECT-6 LEADS] ${i + 1}/${mitarbeiterMitLeads.length} - ${mitarbeiter.vorname} ${mitarbeiter.nachname}`);
      
      // SCHRITT 2A: Scrape LEADS (falls vorhanden)
      let leadsCSV = '';
      let leadsCount = 0;
      
      if (mitarbeiter.leadsUrl) {
        const leadsResponse = await client.get(mitarbeiter.leadsUrl, {
        headers: {
          ...getBrowserHeaders(mitarbeiterUrl),
          'Cookie': loginResult.cookieString
        }
      });
      
        if (leadsResponse.status !== 200) {
          console.error('[PROJECT-6 LEADS] HTTP', leadsResponse.status, 'bei', mitarbeiter.leadsUrl);
        } else {
        
        const $leads = cheerio.load(leadsResponse.data);
        const leads: any[] = [];
        
        $leads('table tr').each((index, row) => {
          if (index === 0) return; // Skip header
          
          const cells = $leads(row).find('td');
          if (cells.length === 0) return;
          
          // Extrahiere Name + Telefon aus Link
          const nameCell = cells.eq(3);
          const nameLink = nameCell.find('a');
          const nameText = nameLink.text().trim();
          const nameHref = nameLink.attr('href') || '';
          const telefonMatch = nameHref.match(/tel:(\+?\d+)/);
          const telefon = telefonMatch ? telefonMatch[1] : '';
          
          const lead = {
            aktion: cells.eq(0).text().trim(),
            datum: cells.eq(1).text().trim(),
            kampagne: cells.eq(2).text().trim(),
            name: nameText,
            telefon: telefon,
            bl: cells.eq(4).text().trim(),
            plz: cells.eq(5).text().trim(),
            erledigt: cells.eq(6).text().trim(),
            termin_1_av: cells.eq(7).text().trim(),
            termin_2_av: cells.eq(8).text().trim(),
            termin_3_av: cells.eq(9).text().trim(),
            lead_erreicht: cells.eq(10).text().trim(),
            termin_vereinbart: cells.eq(11).text().trim(),
            termin_durchgefuehrt: cells.eq(12).text().trim(),
            formular_unterschrieben: cells.eq(13).text().trim(),
            geld_formular: cells.eq(14).text().trim(),
            qc_durchgefuehrt: cells.eq(15).text().trim(),
            bg_vereinbart: cells.eq(16).text().trim(),
            bg_durchgefuehrt: cells.eq(17).text().trim()
          };
          
          leads.push(lead);
        });
          
          leadsCount = leads.length;
          totalLeadsCount += leadsCount;
          console.log(`[PROJECT-6 LEADS] ${mitarbeiter.vorname} ${mitarbeiter.nachname}: ${leadsCount} Leads`);
        
          // Generiere Leads-CSV
          if (leads.length > 0) {
            const headers = [
              'Aktion', 'Datum', 'Kampagne', 'Name', 'Telefon', 'BL', 'PLZ', 'Erledigt',
              '1. AV', '2. AV', '3. AV', 'Lead erreicht', 'Termin vereinbart', 
              'Termin durchgef.', 'Formular unterschr.', 'Geld Formular',
              'QC durchgef.', 'BG vereinbart', 'BG durchgef.'
            ];
            
            const csvRows = [
              headers.join(','),
              ...leads.map(row => {
                return [
                  row.aktion, row.datum, row.kampagne, row.name, row.telefon,
                  row.bl, row.plz, row.erledigt, row.termin_1_av, row.termin_2_av,
                  row.termin_3_av, row.lead_erreicht, row.termin_vereinbart,
                  row.termin_durchgefuehrt, row.formular_unterschrieben, row.geld_formular,
                  row.qc_durchgefuehrt, row.bg_vereinbart, row.bg_durchgefuehrt
                ].map(value => {
                  return `"${String(value || '').replace(/"/g, '""')}"`;
                }).join(',');
              })
            ];
            
            leadsCSV = '\uFEFF' + csvRows.join('\n');
          }
        }
      }
      
      // SCHRITT 2B: Scrape FORMULARE (falls vorhanden)
      let formulareCSV = '';
      let formulareCount = 0;
      
      if (mitarbeiter.formulareUrl) {
        const formulareResponse = await client.get(mitarbeiter.formulareUrl, {
        headers: {
          ...getBrowserHeaders(mitarbeiterUrl),
          'Cookie': loginResult.cookieString
        }
      });
      
        if (formulareResponse.status !== 200) {
          console.error('[PROJECT-6 LEADS] HTTP', formulareResponse.status, 'bei', mitarbeiter.formulareUrl);
        } else {
          
          const $formulare = cheerio.load(formulareResponse.data);
          const formulare: any[] = [];
          
          $formulare('table tr').each((index, row) => {
            if (index === 0) return; // Skip header
            
            const cells = $formulare(row).find('td');
            if (cells.length === 0) return;
            
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
          
          formulareCount = formulare.length;
          totalFormulareCount += formulareCount;
          console.log(`[PROJECT-6 LEADS] ${mitarbeiter.vorname} ${mitarbeiter.nachname}: ${formulareCount} Formulare`);
          
          // Generiere Formulare-CSV
          if (formulare.length > 0) {
            const headers = [
              'ID', 'Datum', 'Kategorie', 'Mitarbeiter', 'Name', 'PLZ',
              'Anbieter', 'Firma', 'KdNr', 'Wechsel', 'Strom', 'Gas', 'Frist'
            ];
            
            const csvRows = [
              headers.join(','),
              ...formulare.map(row => {
                return [
                  row.id, row.datum, row.kategorie, row.mitarbeiter, row.name, row.plz,
                  row.anbieter, row.firma, row.kdnr, row.wechsel, row.strom, row.gas, row.frist
                ].map(value => {
                  return `"${String(value || '').replace(/"/g, '""')}"`;
                }).join(',');
              })
            ];
            
            formulareCSV = '\uFEFF' + csvRows.join('\n');
          }
        }
      }
      
      // Speichere BEIDE CSVs für diesen Mitarbeiter
      const dateStr = new Date().toISOString().split('T')[0];
      
      if (leadsCSV) {
        const leadsPath = path.join(mitarbeiterDir, `leads-${dateStr}.csv`);
        fs.writeFileSync(leadsPath, leadsCSV);
        console.log(`[PROJECT-6 LEADS] Leads CSV gespeichert: ${leadsPath}`);
      }
      
      if (formulareCSV) {
        const formularePath = path.join(mitarbeiterDir, `formulare-${dateStr}.csv`);
        fs.writeFileSync(formularePath, formulareCSV);
        console.log(`[PROJECT-6 LEADS] Formulare CSV gespeichert: ${formularePath}`);
      }
      
      totalMitarbeiterProcessed++;
      
      // Kleine Pause zwischen Mitarbeitern
      await new Promise(resolve => setTimeout(resolve, 300));
    }
    
    // FERTIG: Alle Mitarbeiter verarbeitet
    console.log('[PROJECT-6 LEADS] Fertig!');
    console.log(`[PROJECT-6 LEADS] Mitarbeiter: ${totalMitarbeiterProcessed}/${mitarbeiterMitLeads.length}`);
    console.log(`[PROJECT-6 LEADS] Gesamt Leads: ${totalLeadsCount}`);
    console.log(`[PROJECT-6 LEADS] Gesamt Formulare: ${totalFormulareCount}`);
    
    if (socketId && io) {
      io.to(socketId).emit('project6-leads-complete', { 
        totalMitarbeiter: totalMitarbeiterProcessed,
        totalLeads: totalLeadsCount,
        totalFormulare: totalFormulareCount,
        progress: 100
      });
    }
    
    res.json({ 
      success: true, 
      totalMitarbeiter: totalMitarbeiterProcessed,
      totalLeads: totalLeadsCount,
      totalFormulare: totalFormulareCount,
      message: `${totalMitarbeiterProcessed} Mitarbeiter verarbeitet (${totalLeadsCount} Leads, ${totalFormulareCount} Formulare)`
    });
    
  } catch (error: any) {
    console.error('[PROJECT-6 LEADS] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('project6-leads-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

// Route: Scrape BK-Formulare für ALLE Mitarbeiter
router.post('/scrape-bk-formulare', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  const GPNR = '60235';
  const PASSWORD = 'r87cucd';
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    validateStatus: () => true,
    maxRedirects: 5,
    timeout: 60000
  }));
  
  try {
    if (socketId && io) {
      io.to(socketId).emit('project6-bk-status', { message: 'Login...', progress: 0 });
    }
    
    console.log('[PROJECT-6 BK] Starte BK-Formulare für ALLE Mitarbeiter...');
    
    // Login
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) {
        io.to(socketId).emit('project6-bk-error', { error: loginResult.error });
      }
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-bk-status', { message: 'Lade BK-Formulare...', progress: 30 });
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
      const error = `HTTP ${response.status}`;
      if (socketId && io) {
        io.to(socketId).emit('project6-bk-error', { error });
      }
      return res.status(500).json({ success: false, error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('project6-bk-status', { message: 'Parse BK-Formulare...', progress: 60 });
    }
    
    const $ = cheerio.load(response.data);
    const bkFormulare: any[] = [];
    
    console.log('[PROJECT-6 BK] Parse Tabelle - hole ALLE Formulare...');
    
    // Parse Tabelle - hole ALLE Formulare (kein Filter!)
    $('table tr').each((index, row) => {
      if (index === 0) return; // Skip header
      
      const cells = $(row).find('td');
      if (cells.length === 0) return;
      
      // Extrahiere Bearbeiten-Link aus Aktion-Spalte
      const aktionCell = cells.eq(0);
      const bearbeitenLink = aktionCell.find('a').attr('href') || '';
      const bearbeitenUrl = bearbeitenLink.startsWith('http') ? bearbeitenLink : `${PROJECT_BASE_URL}/${bearbeitenLink}`;
      
      const formular = {
        aktion: cells.eq(0).text().trim(),
        bk_id: cells.eq(1).text().trim(),
        lead_id: cells.eq(2).text().trim(),
        datum: cells.eq(3).text().trim(),
        kategorie: cells.eq(4).text().trim(),
        mitarbeiter: cells.eq(5).text().trim(),
        name: cells.eq(6).text().trim(),
        email: cells.eq(7).text().trim(),
        telefon: cells.eq(8).text().trim(),
        plz: cells.eq(9).text().trim(),
        vermieter: cells.eq(10).text().trim() || '',
        mv_vorhanden: cells.eq(11).text().trim() || '',
        mv_vollstaendig: cells.eq(12).text().trim() || '',
        schaden_summe: cells.eq(13).text().trim() || '',
        pfv_an_kunde_geschickt: cells.eq(14).text().trim() || '',
        formalfehler: cells.eq(15).text().trim() || '',
        kunde_hat_pfv_unterschrieben: cells.eq(16).text().trim() || '',
        vc_hat_pfv_angenommen: cells.eq(17).text().trim() || '',
        bearbeiten_url: bearbeitenUrl,
        // Zusätzliche Felder von Detail-Seite (werden später gefüllt)
        strasse: '',
        geburtsdatum: '',
        pdf_datei: ''
      };
      
      bkFormulare.push(formular);
    });
    
    console.log('[PROJECT-6 BK] BK-Formulare gefunden:', bkFormulare.length);
    
    // SCHRITT 2: Für jedes Formular die Detail-Seite scrapen + speichern
    const baseDir = path.join(__dirname, '../../BK_Formulare');
    if (!fs.existsSync(baseDir)) {
      fs.mkdirSync(baseDir, { recursive: true });
    }
    
    let totalFormulareProcessed = 0;
    let totalMitarbeiter = new Set<string>();
    
    for (let i = 0; i < bkFormulare.length; i++) {
      const formular = bkFormulare[i];
      totalMitarbeiter.add(formular.mitarbeiter);
      
      if (socketId && io) {
        const progress = Math.round(60 + (i / bkFormulare.length) * 25);
        io.to(socketId).emit('project6-bk-status', { 
          message: `Detail ${i + 1}/${bkFormulare.length}...`, 
          progress: progress
        });
      }
      
      console.log(`[PROJECT-6 BK] Scrape Detail ${i + 1}/${bkFormulare.length}: ${formular.bearbeiten_url}`);
      
      try {
        const detailResponse = await client.get(formular.bearbeiten_url, {
          headers: {
            ...getBrowserHeaders(url),
            'Cookie': loginResult.cookieString
          }
        });
        
        if (detailResponse.status === 200) {
          const $detail = cheerio.load(detailResponse.data);
          
          // Extrahiere Straße und Geburtsdatum über Input-Namen oder Labels
          // Versuche verschiedene Methoden:
          
          // Methode 1: Über input name/id Attribute
          $detail('input').each((idx, input) => {
            const name = $detail(input).attr('name') || '';
            const id = $detail(input).attr('id') || '';
            const value = $detail(input).val() as string || '';
            
            // Straße
            if (name.toLowerCase().includes('strasse') || name.toLowerCase().includes('straße') || 
                id.toLowerCase().includes('strasse') || id.toLowerCase().includes('straße')) {
              formular.strasse = value;
            }
            
            // Geburtsdatum
            if (name.toLowerCase().includes('geburtsdatum') || id.toLowerCase().includes('geburtsdatum') ||
                name.toLowerCase().includes('birthday') || id.toLowerCase().includes('birthday')) {
              formular.geburtsdatum = value;
            }
          });
          
          // Methode 2: Falls immer noch leer, versuche über Labels
          if (!formular.strasse || !formular.geburtsdatum) {
            $detail('label').each((idx, label) => {
              const labelText = $detail(label).text().trim();
              
              // Straße
              if (labelText.includes('Straße') || labelText.includes('Strasse')) {
                // Versuche verschiedene Wege das Input zu finden
                let input = $detail(label).next('input');
                if (input.length === 0) {
                  input = $detail(label).parent().find('input');
                }
                if (input.length > 0 && !formular.strasse) {
                  formular.strasse = input.val() as string || '';
                }
              }
              
              // Geburtsdatum
              if (labelText.includes('Geburtsdatum')) {
                let input = $detail(label).next('input');
                if (input.length === 0) {
                  input = $detail(label).parent().find('input');
                }
                if (input.length > 0 && !formular.geburtsdatum) {
                  formular.geburtsdatum = input.val() as string || '';
                }
              }
            });
          }
          
          // Extrahiere PDF-Link aus "Angehängte Dateien" und speichere im Kunden-Ordner
          let pdfSaved = false;
          $detail('a').each((idx, link) => {
            const href = $detail(link).attr('href') || '';
            if (href.endsWith('.pdf') && !pdfSaved) {
              const pdfUrl = href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`;
              formular.pdf_datei = $detail(link).text().trim();
              pdfSaved = true;
              
              // Download PDF direkt in Kunden-Ordner
              console.log(`[PROJECT-6 BK] Download PDF: ${formular.pdf_datei}`);
              client.get(pdfUrl, {
                headers: {
                  ...getBrowserHeaders(formular.bearbeiten_url),
                  'Cookie': loginResult.cookieString
                },
                responseType: 'arraybuffer'
              }).then(pdfResponse => {
                if (pdfResponse.status === 200) {
                  const mitarbeiterDir = path.join(baseDir, formular.mitarbeiter.replace(/\s/g, '_'));
                  const kundeDir = path.join(mitarbeiterDir, formular.name.replace(/\s/g, '_'));
                  const pdfPath = path.join(kundeDir, formular.pdf_datei);
                  fs.writeFileSync(pdfPath, pdfResponse.data);
                  console.log(`[PROJECT-6 BK] PDF gespeichert: ${pdfPath}`);
                }
              }).catch(err => {
                console.error(`[PROJECT-6 BK] PDF Download Fehler:`, err.message);
              });
            }
          });
          
          // Debug: Wenn Straße/Geburtsdatum leer, zeige alle Input-Felder
          if (!formular.strasse || !formular.geburtsdatum) {
            console.log(`[PROJECT-6 BK] WARNUNG: Fehlende Daten für BK ${formular.bk_id}`);
            console.log(`[PROJECT-6 BK] Debug - Alle Input-Felder:`);
            $detail('input[type="text"], input:not([type])').each((idx, input) => {
              const name = $detail(input).attr('name') || '';
              const id = $detail(input).attr('id') || '';
              const value = $detail(input).val() as string || '';
              const placeholder = $detail(input).attr('placeholder') || '';
              if (value) {
                console.log(`  - name="${name}", id="${id}", value="${value}", placeholder="${placeholder}"`);
              }
            });
          }
          
          console.log(`[PROJECT-6 BK] Detail: Straße="${formular.strasse}", Geburtsdatum="${formular.geburtsdatum}", PDF="${formular.pdf_datei}"`);
          
          // SCHRITT 3: Speichere jedes Formular in eigenem Kunden-Ordner
          // Struktur: BK_Formulare/Mitarbeiter/Kunde/
          const mitarbeiterDir = path.join(baseDir, formular.mitarbeiter.replace(/\s/g, '_'));
          const kundeDir = path.join(mitarbeiterDir, formular.name.replace(/\s/g, '_'));
          
          if (!fs.existsSync(mitarbeiterDir)) {
            fs.mkdirSync(mitarbeiterDir, { recursive: true });
          }
          if (!fs.existsSync(kundeDir)) {
            fs.mkdirSync(kundeDir, { recursive: true });
          }
          
          // Speichere Formular als CSV
          const headers = [
            'Aktion', 'BK ID', 'Lead-ID', 'Datum', 'Kategorie', 'Mitarbeiter', 'Name', 
            'E-Mail', 'Telefon', 'PLZ', 'Straße', 'Geburtsdatum', 'Vermieter', 
            'MV vorhanden', 'MV vollständig', 'Schaden-Summe', 'PFV an Kunde geschickt', 
            'Formalfehler', 'Kunde hat PFV unterschrieben', 'VC hat PFV angenommen',
            'PDF-Datei', 'Bearbeiten-URL'
          ];
          
          const csvRow = [
            formular.aktion, formular.bk_id, formular.lead_id, formular.datum, formular.kategorie, 
            formular.mitarbeiter, formular.name, formular.email, formular.telefon, formular.plz, 
            formular.strasse, formular.geburtsdatum, formular.vermieter, formular.mv_vorhanden, 
            formular.mv_vollstaendig, formular.schaden_summe, formular.pfv_an_kunde_geschickt, 
            formular.formalfehler, formular.kunde_hat_pfv_unterschrieben, formular.vc_hat_pfv_angenommen, 
            formular.pdf_datei, formular.bearbeiten_url
          ].map(value => `"${String(value || '').replace(/"/g, '""')}"`).join(',');
          
          const csvContent = '\uFEFF' + headers.join(',') + '\n' + csvRow;
          const csvPath = path.join(kundeDir, 'formular.csv');
          fs.writeFileSync(csvPath, csvContent);
          console.log(`[PROJECT-6 BK] CSV gespeichert: ${csvPath}`);
          
          totalFormulareProcessed++;
        }
      } catch (err: any) {
        console.error(`[PROJECT-6 BK] Detail-Scraping Fehler:`, err.message);
      }
      
      // Kleine Pause zwischen Requests
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    
    // FERTIG: Alle Formulare verarbeitet
    console.log('[PROJECT-6 BK] Fertig!');
    console.log(`[PROJECT-6 BK] Formulare: ${totalFormulareProcessed}/${bkFormulare.length}`);
    console.log(`[PROJECT-6 BK] Mitarbeiter: ${totalMitarbeiter.size}`);
    
    if (socketId && io) {
      io.to(socketId).emit('project6-bk-complete', { 
        totalFormulare: totalFormulareProcessed,
        totalMitarbeiter: totalMitarbeiter.size,
        progress: 100
      });
    }
    
    res.json({ 
      success: true, 
      totalFormulare: totalFormulareProcessed,
      totalMitarbeiter: totalMitarbeiter.size,
      message: `${totalFormulareProcessed} BK-Formulare von ${totalMitarbeiter.size} Mitarbeitern gespeichert`
    });
    
  } catch (error: any) {
    console.error('[PROJECT-6 BK] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('project6-bk-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

// BK-Formulare MIT PDFs Route (100 Workers!)
router.post('/scrape-bk-mit-pdfs', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  const GPNR = '60235';
  const PASSWORD = 'r87cucd';
  const PARALLEL_WORKERS = 100;
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({ jar, withCredentials: true, validateStatus: () => true, maxRedirects: 5, timeout: 60000 }));
  
  try {
    const startTime = Date.now();
    if (socketId && io) io.to(socketId).emit('bk-pdfs-status', { message: '🔐 Login...', progress: 0 });
    
    console.log('[BK-PDFS] 🚀 START - 100 Workers');
    
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) io.to(socketId).emit('bk-pdfs-error', { error: loginResult.error });
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    const cookieString = loginResult.cookieString;
    const url = 'https://project-5.at/formularuebersicht_bk_mitarbeiter.php';
    const response = await client.get(url, { headers: { ...getBrowserHeaders(PROJECT_BASE_URL), 'Cookie': cookieString } });
    
    if (response.status !== 200) return res.status(500).json({ success: false, error: `HTTP ${response.status}` });
    
    const $ = cheerio.load(response.data);
    let bkFormulare: any[] = [];
    
    $('table tr').each((index, row) => {
      if (index === 0) return;
      const cells = $(row).find('td');
      if (cells.length === 0) return;
      
      const bearbeitenLink = cells.eq(0).find('a').attr('href') || '';
      const bearbeitenUrl = bearbeitenLink.startsWith('http') ? bearbeitenLink : `${PROJECT_BASE_URL}/${bearbeitenLink}`;
      
      bkFormulare.push({
        bearbeiten_url: bearbeitenUrl,
        bk_id: cells.eq(1).text().trim(),
        mitarbeiter: cells.eq(5).text().trim(),
        name: cells.eq(6).text().trim()
      });
    });
    
    console.log('[BK-PDFS] 📊 Gefunden:', bkFormulare.length);
    const formulareToProcess = bkFormulare;
    console.log(`[BK-PDFS] ⚙️  Verarbeite ALLE ${formulareToProcess.length} Formulare mit 100 Workers!`);
    
    let processedFormulare = 0, totalPdfs = 0, totalUploadedBytes = 0, failedUploads = 0;
    
    const processFormular = async (formular: any) => {
      const wClient = axios.create({
        withCredentials: true, validateStatus: () => true, timeout: 90000,
        maxContentLength: 100*1024*1024, maxBodyLength: 100*1024*1024,
        httpAgent: new (require('http').Agent)({ keepAlive: true, maxSockets: 5 }),
        httpsAgent: new (require('https').Agent)({ keepAlive: true, maxSockets: 5 })
      });
      
      try {
        const dResp = await wClient.get(formular.bearbeiten_url, { headers: {...getBrowserHeaders(url), 'Cookie': cookieString}});
        if (dResp.status !== 200) return { success: false, pdfs: 0, bytes: 0 };
        
        const $d = cheerio.load(dResp.data);
        const mName = formular.mitarbeiter.replace(/\s/g, '_').replace(/[^a-zA-Z0-9_-äöüÄÖÜß]/g, '_');
        const kName = (formular.name || 'Unbekannt').replace(/\s/g, '_').replace(/[^a-zA-Z0-9_-äöüÄÖÜß]/g, '_');
        
        let bytes = 0, pdfs = 0;
        
        // CSV
        const csv = '\uFEFF' + 'BK ID,Mitarbeiter,Kunde\n' + `"${formular.bk_id}","${formular.mitarbeiter}","${formular.name}"`;
        await s3Client.send(new PutObjectCommand({ Bucket: S3_BUCKET_BK_MIT_PDFS, Key: `${mName}/${kName}/formular.csv`, Body: csv, ContentType: 'text/csv; charset=utf-8' }));
        bytes += Buffer.byteLength(csv);
        
        // PDFs
        const pdfUrls: string[] = [];
        $d('a').each((i, link) => {
          const href = $d(link).attr('href') || '';
          if (href.endsWith('.pdf')) pdfUrls.push(href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`);
        });
        
        for (const pUrl of pdfUrls) {
          try {
            const pResp = await wClient.get(pUrl, { headers: {...getBrowserHeaders(formular.bearbeiten_url), 'Cookie': cookieString}, responseType: 'arraybuffer' });
            if (pResp.status === 200) {
              const pName = pUrl.split('/').pop()?.replace(/[^a-zA-Z0-9._-]/g, '_') || 'pdf.pdf';
              await s3Client.send(new PutObjectCommand({ Bucket: S3_BUCKET_BK_MIT_PDFS, Key: `${mName}/${kName}/${pName}`, Body: Buffer.from(pResp.data), ContentType: 'application/pdf' }));
              bytes += pResp.data.byteLength;
              pdfs++;
            }
          } catch (e) {}
        }
        
        return { success: true, pdfs, bytes };
      } catch (e: any) {
        return { success: false, pdfs: 0, bytes: 0 };
      }
    };
    
    for (let i = 0; i < formulareToProcess.length; i += PARALLEL_WORKERS) {
      const batch = formulareToProcess.slice(i, i + PARALLEL_WORKERS);
      const results = await Promise.allSettled(batch.map(f => processFormular(f)));
      
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value.success) {
          processedFormulare++;
          totalPdfs += r.value.pdfs;
          totalUploadedBytes += r.value.bytes;
        } else failedUploads++;
      }
      
      const mb = (totalUploadedBytes / (1024*1024)).toFixed(2);
      console.log(`[BK-PDFS] ⚡ ${processedFormulare}/${formulareToProcess.length} | ${mb} MB | ${totalPdfs} PDFs`);
    }
    
    const min = Math.floor((Date.now() - startTime) / 60000);
    const sec = Math.floor(((Date.now() - startTime) % 60000) / 1000);
    
    console.log(`[BK-PDFS] ✅ ${min}m ${sec}s | ${processedFormulare} Formulare | ${totalPdfs} PDFs | ${(totalUploadedBytes/(1024*1024)).toFixed(2)} MB`);
    
    if (socketId && io) io.to(socketId).emit('bk-pdfs-complete', { formulare: processedFormulare, pdfs: totalPdfs, sizeMB: (totalUploadedBytes/(1024*1024)).toFixed(2) });
    
    res.json({ success: true, formulare: processedFormulare, pdfs: totalPdfs, sizeMB: (totalUploadedBytes/(1024*1024)).toFixed(2) });
    
  } catch (error: any) {
    console.error('[BK-PDFS] ❌', error.message);
    if (socketId && io) io.to(socketId).emit('bk-pdfs-error', { error: error.message });
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;


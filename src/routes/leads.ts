import { Router, Request, Response } from 'express';
import axios, { AxiosInstance } from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import * as cheerio from 'cheerio';
import { Server } from 'socket.io';
import { createClient, SupabaseClient } from '@supabase/supabase-js';

const router = Router();
let io: Server;
let supabase: SupabaseClient | null = null;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

// Supabase Client initialisieren (lazy)
const getSupabaseClient = () => {
  if (!supabase) {
    const supabaseUrl = process.env.SUPABASE_URL || '';
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
    
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Supabase nicht konfiguriert');
    }
    
    supabase = createClient(supabaseUrl, supabaseKey);
  }
  return supabase;
};

const PROJECT_BASE_URL = 'https://project-5.at';
const MITARBEITER_URL = 'https://project-5.at/mitarbeiter.php';

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
    
    const cookieStr = getCookieString(jar, PROJECT_BASE_URL);
    
    if (cookieStr) {
      console.log('[LEADS] Login erfolgreich');
      return { success: true, cookieString: cookieStr };
    } else {
      return { success: false, error: 'Kein Cookie erhalten' };
    }
      
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}

// Scrape mitarbeiter.php und finde alle mit Leads
async function scrapeMitarbeiterWithLeads(client: AxiosInstance, cookieString: string): Promise<any[]> {
  try {
    const response = await client.get(MITARBEITER_URL, {
      headers: {
        ...getBrowserHeaders(PROJECT_BASE_URL),
        'Cookie': cookieString
      }
    });
    
    if (response.status !== 200) {
      console.error('[LEADS] HTTP Status:', response.status);
      return [];
    }
    
    console.log('[LEADS] mitarbeiter.php geladen, HTML Länge:', response.data.length);
    
    const $ = cheerio.load(response.data);
    const mitarbeiterMitLeads: any[] = [];
    
    // Parse Tabelle
    $('table tr').each((index, row) => {
      if (index === 0) return; // Skip header
      
      const rowHtml = $(row).html() || '';
      const cells = $(row).find('td');
      
      // Prüfe ob "Leads" Button existiert
      if (rowHtml.toLowerCase().includes('leads')) {
        // Extrahiere Mitarbeiter-ID aus dem Leads-Link
        const leadsMatch = rowHtml.match(/leads_mitarbeiter\.php\?mitarbeiter=(\d+)/);
        
        if (leadsMatch) {
          const mitarbeiterId = leadsMatch[1];
          
          // Spalte 2 = Nachname, Spalte 3 = Vorname
          const nachname = cells.eq(2).text().trim();
          const vorname = cells.eq(3).text().trim();
          const name = vorname + ' ' + nachname;
          
          // Spalte 1 = VCNr (z.B. "VC24999" oder "2411644")
          let vcrNumber = cells.eq(1).text().trim();
          
          // Stelle sicher, dass es mit "VC24-" beginnt
          if (vcrNumber && !vcrNumber.startsWith('VC24-')) {
            // Entferne "VC" falls vorhanden, dann füge "VC24-" hinzu
            vcrNumber = vcrNumber.replace(/^VC/, '');
            vcrNumber = 'VC24-' + vcrNumber;
          }
          
          mitarbeiterMitLeads.push({
            id: mitarbeiterId,
            name: name,
            vcrNumber: vcrNumber
          });
        }
      }
    });
    
    console.log('[LEADS] Mitarbeiter mit Leads gefunden:', mitarbeiterMitLeads.length);
    
    // Log erste 5
    mitarbeiterMitLeads.slice(0, 5).forEach(m => {
      console.log(`[LEADS] - ${m.name} (VCR: ${m.vcrNumber}, ID: ${m.id})`);
    });
    
    return mitarbeiterMitLeads;
    
  } catch (error: any) {
    console.error('[LEADS] Fehler:', error.message);
    return [];
  }
}

// Zähle Leads eines Mitarbeiters
async function countLeadsForMitarbeiter(client: AxiosInstance, cookieString: string, mitarbeiterId: string): Promise<number> {
  try {
    const url = `${PROJECT_BASE_URL}/leads_mitarbeiter.php?mitarbeiter=${mitarbeiterId}`;
    
    const response = await client.get(url, {
      headers: {
        ...getBrowserHeaders(PROJECT_BASE_URL),
        'Cookie': cookieString
      }
    });
    
    if (response.status !== 200) return 0;
    
    const $ = cheerio.load(response.data);
    
    // Zähle Tabellenzeilen (ohne Header)
    let leadCount = 0;
    $('table tr').each((index, row) => {
      if (index === 0) return; // Skip header
      
      const cells = $(row).find('td');
      if (cells.length > 0) {
        leadCount++;
      }
    });
    
    return leadCount;
    
  } catch (error: any) {
    console.error('[LEADS] Fehler bei Mitarbeiter', mitarbeiterId, ':', error.message);
    return 0;
  }
}

// Speichere Leads-Anzahl in Supabase
async function saveLeadsCountToSupabase(vcrNumber: string, mitarbeiterId: string, leadsCount: number): Promise<boolean> {
  try {
    const supabase = getSupabaseClient();
    
    // Versuche UPDATE via vc24_number
    const { data, error } = await supabase
      .from('mitarbeiter_organigramm')
      .update({ 
        leads_count: leadsCount,
        updated_at: new Date().toISOString()
      })
      .eq('vc24_number', vcrNumber)
      .select();
    
    if (error) {
      console.error('[LEADS] Supabase Update Fehler für VCR', vcrNumber, ':', error.message);
      return false;
    }
    
    // Prüfe ob ein Update durchgeführt wurde
    if (!data || data.length === 0) {
      // Kein Match gefunden - versuche mit user_id
      const { data: userData, error: userError } = await supabase
        .from('mitarbeiter_organigramm')
        .update({ 
          leads_count: leadsCount,
          updated_at: new Date().toISOString()
        })
        .eq('user_id', mitarbeiterId)
        .select();
      
      if (userError || !userData || userData.length === 0) {
        return false;
      }
    }
    
    return true;
    
  } catch (error: any) {
    console.error('[LEADS] Fehler beim Speichern für VCR', vcrNumber, ':', error.message);
    return false;
  }
}

const GPNR = '60235';
const PASSWORD = 'r87cucd';

// API Route - NUR aus Supabase lesen (schnell!)
router.post('/count-total', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  
  try {
    if (socketId && io) {
      io.to(socketId).emit('total-leads-status', { message: 'Lade aus DB...' });
    }
    
    const supabase = getSupabaseClient();
    
    // Verwende SQL-Aggregation für ALLE Zeilen (kein Limit!)
    const { data, error } = await supabase
      .rpc('get_total_leads_count');
    
    if (error) {
      console.error('[LEADS] Supabase RPC Fehler:', error.message);
      
      // Fallback: Hole ALLE Zeilen mit größerem Limit
      console.log('[LEADS] Verwende Fallback-Methode...');
      const { data: allData, error: allError } = await supabase
        .from('mitarbeiter_organigramm')
        .select('leads_count')
        .limit(10000); // Erhöhe Limit auf 10.000
      
      if (allError || !allData) {
        if (socketId && io) {
          io.to(socketId).emit('total-leads-result', { success: false, error: allError?.message || 'Keine Daten' });
        }
        return res.status(500).json({ success: false, error: allError?.message || 'Keine Daten' });
      }
      
      const totalLeads = allData.reduce((sum, row) => sum + (row.leads_count || 0), 0);
      const mitarbeiterMitLeads = allData.filter(row => row.leads_count > 0).length;
      
      console.log('[LEADS] Fallback - Gesamt Leads:', totalLeads);
      console.log('[LEADS] Fallback - Mitarbeiter mit Leads:', mitarbeiterMitLeads);
      
      if (socketId && io) {
        io.to(socketId).emit('total-leads-result', {
          success: true,
          totalLeads: totalLeads,
          mitarbeiterCount: mitarbeiterMitLeads
        });
      }
      
      return res.json({
        success: true,
        totalLeads: totalLeads,
        mitarbeiterCount: mitarbeiterMitLeads
      });
    }
    
    // RPC gibt ein Array zurück - erstes Element holen
    const result = Array.isArray(data) ? data[0] : data;
    const totalLeads = result?.total_leads || 0;
    const mitarbeiterMitLeads = result?.mitarbeiter_count || 0;
    
    console.log('[LEADS] RPC Response:', data);
    console.log('[LEADS] Gesamt Leads aus DB (via RPC):', totalLeads);
    console.log('[LEADS] Mitarbeiter mit Leads:', mitarbeiterMitLeads);
    
    if (socketId && io) {
      io.to(socketId).emit('total-leads-result', {
        success: true,
        totalLeads: totalLeads,
        mitarbeiterCount: mitarbeiterMitLeads
      });
    }
    
    
    res.json({
      success: true,
      totalLeads: totalLeads,
      mitarbeiterCount: mitarbeiterMitLeads
    });
    
  } catch (error: any) {
    console.error('[LEADS API] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('total-leads-result', { success: false, error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

// Route: Scrape ALLE Mitarbeiter-Leads
router.post('/scrape-all', async (req: Request, res: Response) => {
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
      io.to(socketId).emit('leads-scrape-status', { message: 'Login...', progress: 0 });
    }
    
    console.log('[LEADS-SCRAPE] Starte...');
    
    // Login
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) {
        io.to(socketId).emit('leads-scrape-error', { error: loginResult.error });
      }
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    if (socketId && io) {
      io.to(socketId).emit('leads-scrape-status', { message: 'Lade Mitarbeiter...', progress: 10 });
    }
    
    // Schritt 1: Hole ALLE Mitarbeiter aus mitarbeiter_organigramm mit Leads
    const supabase = getSupabaseClient();
    const { data: mitarbeiterData, error: mitarbeiterError } = await supabase
      .from('mitarbeiter_organigramm')
      .select('user_id, vc24_number, gpnr, full_name, leads_count')
      .gt('leads_count', 0)
      .order('leads_count', { ascending: false });
    
    if (mitarbeiterError || !mitarbeiterData || mitarbeiterData.length === 0) {
      console.error('[LEADS-SCRAPE] Keine Mitarbeiter mit Leads gefunden');
      if (socketId && io) {
        io.to(socketId).emit('leads-scrape-error', { error: 'Keine Mitarbeiter mit Leads gefunden' });
      }
      return res.status(500).json({ success: false, error: 'Keine Mitarbeiter mit Leads gefunden' });
    }
    
    console.log('[LEADS-SCRAPE] Mitarbeiter mit Leads:', mitarbeiterData.length);
    
    // Schritt 2: Scrape Leads für jeden Mitarbeiter
    let totalLeadsScraped = 0;
    let totalMitarbeiterProcessed = 0;
    
    for (let i = 0; i < mitarbeiterData.length; i++) {
      const mitarbeiter = mitarbeiterData[i];
      const mitarbeiterId = mitarbeiter.vc24_number || mitarbeiter.gpnr || mitarbeiter.user_id;
      
      console.log(`[LEADS-SCRAPE] ${i + 1}/${mitarbeiterData.length} - ${mitarbeiter.full_name} (${mitarbeiterId})`);
      
      const progress = Math.round(10 + (i / mitarbeiterData.length) * 80);
      if (socketId && io) {
        io.to(socketId).emit('leads-scrape-status', { 
          message: `Scrape ${mitarbeiter.full_name}...`, 
          progress: progress,
          current: i + 1,
          total: mitarbeiterData.length
        });
      }
      
      try {
        // Scrape leads_mitarbeiter.php?mitarbeiter=X
        const leadsUrl = `https://project-5.at/leads_mitarbeiter.php?mitarbeiter=${mitarbeiterId}`;
        const response = await client.get(leadsUrl, {
          headers: {
            ...getBrowserHeaders(MITARBEITER_URL),
            'Cookie': loginResult.cookieString
          }
        });
        
        if (response.status !== 200) {
          console.error(`[LEADS-SCRAPE] HTTP ${response.status} für ${mitarbeiter.full_name}`);
          continue;
        }
        
        const $ = cheerio.load(response.data);
        const leads: any[] = [];
        
        // Parse Tabelle
        $('table tr').each((index, row) => {
          if (index === 0) return; // Skip header
          
          const cells = $(row).find('td');
          if (cells.length === 0) return;
          
          // Extrahiere Telefonnummer aus Link
          const nameLink = cells.eq(3).find('a').attr('href') || '';
          const telefonMatch = nameLink.match(/tel:(\+?\d+)/);
          const telefon = telefonMatch ? telefonMatch[1] : '';
          
          const lead = {
            lead_id: null, // Keine eindeutige ID sichtbar
            mitarbeiter_id: mitarbeiterId,
            mitarbeiter_name: mitarbeiter.full_name,
            datum: cells.eq(1).text().trim() || null,
            kampagne: cells.eq(2).text().trim() || null,
            kunde_name: cells.eq(3).text().trim() || null,
            kunde_telefon: telefon || null,
            bundesland: cells.eq(4).text().trim() || null,
            plz: cells.eq(5).text().trim() || null,
            erledigt: cells.eq(6).find('input[type="checkbox"]').prop('checked') || false,
            termin_1_av: cells.eq(7).text().trim() || null,
            termin_2_av: cells.eq(8).text().trim() || null,
            termin_3_av: cells.eq(9).text().trim() || null,
            lead_erreicht: cells.eq(10).find('input[type="checkbox"]').prop('checked') || false,
            termin_vereinbart: cells.eq(11).text().trim() || null,
            termin_durchgefuehrt: cells.eq(12).text().trim() || null,
            formular_unterschrieben: cells.eq(13).find('input[type="checkbox"]').prop('checked') || false,
            geld_formular: parseInt(cells.eq(14).text().trim()) || 0,
            qc_durchgefuehrt: cells.eq(15).text().trim() || null,
            bg_vereinbart: cells.eq(16).text().trim() || null,
            bg_durchgefuehrt: parseInt(cells.eq(17).text().trim()) || 0,
            ist_kunde: cells.eq(18).find('input[type="checkbox"]').prop('checked') || false,
            einzahlung: cells.eq(19).text().trim() || null,
            bws: cells.eq(20).text().trim() || null
          };
          
          leads.push(lead);
        });
        
        console.log(`[LEADS-SCRAPE] ${mitarbeiter.full_name}: ${leads.length} Leads gefunden`);
        
        // Speichere in Supabase
        if (leads.length > 0) {
          for (const lead of leads) {
            try {
              // Da wir keine eindeutige lead_id haben, verwenden wir eine Kombination als Unique-Key
              // Wir erstellen eine temporäre ID basierend auf mitarbeiter_id + kunde_name + telefon + datum
              const tempLeadId = `${mitarbeiterId}_${lead.kunde_name}_${lead.kunde_telefon}_${lead.datum}`.replace(/\s/g, '_');
              
              const { error } = await supabase
                .from('mitarbeiter_leads')
                .upsert({
                  lead_id: tempLeadId.hashCode(), // Simple Hash für eindeutige ID
                  mitarbeiter_id: lead.mitarbeiter_id,
                  mitarbeiter_name: lead.mitarbeiter_name,
                  datum: lead.datum,
                  kampagne: lead.kampagne,
                  kunde_name: lead.kunde_name,
                  kunde_telefon: lead.kunde_telefon,
                  bundesland: lead.bundesland,
                  plz: lead.plz,
                  erledigt: lead.erledigt,
                  termin_1_av: lead.termin_1_av,
                  termin_2_av: lead.termin_2_av,
                  termin_3_av: lead.termin_3_av,
                  lead_erreicht: lead.lead_erreicht,
                  termin_vereinbart: lead.termin_vereinbart,
                  termin_durchgefuehrt: lead.termin_durchgefuehrt,
                  formular_unterschrieben: lead.formular_unterschrieben,
                  geld_formular: lead.geld_formular,
                  qc_durchgefuehrt: lead.qc_durchgefuehrt,
                  bg_vereinbart: lead.bg_vereinbart,
                  bg_durchgefuehrt: lead.bg_durchgefuehrt,
                  ist_kunde: lead.ist_kunde,
                  einzahlung: lead.einzahlung,
                  bws: lead.bws,
                  last_scraped_at: new Date().toISOString()
                }, {
                  onConflict: 'lead_id,mitarbeiter_id'
                });
              
              if (!error) {
                totalLeadsScraped++;
              }
            } catch (err) {
              console.error('[LEADS-SCRAPE] Fehler beim Speichern:', err);
            }
          }
        }
        
        totalMitarbeiterProcessed++;
        
        // Pause zwischen Requests (500ms)
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error: any) {
        console.error(`[LEADS-SCRAPE] Fehler bei ${mitarbeiter.full_name}:`, error.message);
      }
    }
    
    console.log('[LEADS-SCRAPE] Fertig!');
    console.log(`[LEADS-SCRAPE] Mitarbeiter: ${totalMitarbeiterProcessed}/${mitarbeiterData.length}`);
    console.log(`[LEADS-SCRAPE] Leads: ${totalLeadsScraped}`);
    
    if (socketId && io) {
      io.to(socketId).emit('leads-scrape-complete', { 
        mitarbeiter: totalMitarbeiterProcessed,
        leads: totalLeadsScraped,
        progress: 100
      });
    }
    
    res.json({
      success: true,
      mitarbeiter: totalMitarbeiterProcessed,
      leads: totalLeadsScraped,
      message: `${totalLeadsScraped} Leads von ${totalMitarbeiterProcessed} Mitarbeitern gescraped`
    });
    
  } catch (error: any) {
    console.error('[LEADS-SCRAPE] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('leads-scrape-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

// Helper: Simple Hash-Funktion für String
declare global {
  interface String {
    hashCode(): number;
  }
}

String.prototype.hashCode = function(): number {
  let hash = 0;
  for (let i = 0; i < this.length; i++) {
    const char = this.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return Math.abs(hash);
};

export default router;


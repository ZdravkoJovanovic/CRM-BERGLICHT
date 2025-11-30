import { Router, Request, Response } from 'express';
import axios, { AxiosInstance } from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import * as cheerio from 'cheerio';
import { Server } from 'socket.io';
import { createClient } from '@supabase/supabase-js';

const router = Router();
let io: Server;

// TLS-Zertifikatsprüfung deaktivieren
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const PROJECT_BASE_URL = 'https://project-5.at';

export const setSocketIO = (socketIO: Server) => {
  io = socketIO;
};

// Supabase Client
let supabase: any = null;

function getSupabaseClient() {
  if (!supabase) {
    const supabaseUrl = process.env.SUPABASE_URL || '';
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
    
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt!');
    }
    
    supabase = createClient(supabaseUrl, supabaseKey);
  }
  return supabase;
}

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
      return { success: true, cookieString: cookieStr };
    } else {
      return { success: false, error: 'Kein Cookie erhalten' };
    }
      
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}

async function scrapeTelefonnummer(client: AxiosInstance, cookieString: string, userId: string): Promise<string> {
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
          telefon = input.val() as string || '';
          if (telefon) return false; // Stop loop
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
        telefon = telInput.val() as string || '';
      }
    }
    
    return telefon.trim();
    
  } catch (error: any) {
    console.error('[TELEFON] Fehler:', error.message);
    return '';
  }
}

const GPNR = '60235';
const PASSWORD = 'r87cucd';

// API Route: Hole Telefonnummern für Mitarbeiter
router.post('/fetch-numbers', async (req: Request, res: Response) => {
  const { userIds, socketId } = req.body;
  
  if (!userIds || !Array.isArray(userIds)) {
    return res.status(400).json({ success: false, error: 'userIds array erforderlich' });
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
    const results: any[] = [];
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
        } catch (error: any) {
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
    
  } catch (error: any) {
    console.error('[TELEFON API] Fehler:', error.message);
    if (socketId && io) {
      io.to(socketId).emit('telefon-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;




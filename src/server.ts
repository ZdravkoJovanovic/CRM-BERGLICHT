import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import path from 'path';
import { promises as fs } from 'fs';
import { Readable } from 'stream';
import archiver from 'archiver';
import { S3Client, ListObjectsV2Command, GetObjectCommand, _Object as S3Object } from '@aws-sdk/client-s3';
import authRouter, { setSocketIO as setAuthSocketIO } from './routes/auth';
import pdfRouter from './routes/pdf';
import verifyRouter, { setSocketIO as setVerifySocketIO } from './routes/verify';
import organigrammRouter, { setSocketIO as setOrganigrammSocketIO } from './routes/organigramm';
import telefonRouter, { setSocketIO as setTelefonSocketIO } from './routes/telefon';
import formulareRouter, { setSocketIO as setFormulareSocketIO } from './routes/formulare';
import leadsRouter, { setSocketIO as setLeadsSocketIO } from './routes/leads';
import project6Router, { setSocketIO as setProject6SocketIO } from './routes/project6';

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || '';
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || '';
const STAMMDATEN_BUCKET = 'crm-berglicht-leads-formulare';

const s3Client = new S3Client({
  region: 'eu-north-1',
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY
  },
  maxAttempts: 3
});

const ROOT_DIR = path.join(__dirname, '..');
const DAVID_STRUCTURE_FILE = path.join(ROOT_DIR, 'david_structure.json');

const normalizePrefix = (input: string) => input.trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');

const ensureTrailingSlash = (value: string) => (value.endsWith('/') ? value : `${value}/`);

const sanitizeZipSegment = (value: string) => {
  if (!value) return 'ordner';
  return value.replace(/[^a-zA-Z0-9._-]/g, '_');
};

type BulkDownloadEntry = {
  prefix: string;
  name: string;
};

const FOLDER_CACHE_TTL_MS = 5 * 60 * 1000;
let cachedFolderPrefixes: string[] = [];
let folderCacheExpires = 0;
let folderCachePromise: Promise<string[]> | null = null;

const fetchFolderPrefixes = async () => {
  const prefixes: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: STAMMDATEN_BUCKET,
      Delimiter: '/',
      ContinuationToken: continuationToken
    }));

    (response.CommonPrefixes || []).forEach((entry) => {
      if (entry.Prefix) {
        prefixes.push(entry.Prefix);
      }
    });

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  return prefixes;
};

const refreshFolderCache = async () => {
  const prefixes = await fetchFolderPrefixes();
  cachedFolderPrefixes = prefixes;
  folderCacheExpires = Date.now() + FOLDER_CACHE_TTL_MS;
  return prefixes;
};

const getFolderPrefixes = async () => {
  if (Date.now() < folderCacheExpires && cachedFolderPrefixes.length > 0) {
    return cachedFolderPrefixes;
  }

  if (!folderCachePromise) {
    folderCachePromise = refreshFolderCache().finally(() => {
      folderCachePromise = null;
    });
  }

  return folderCachePromise;
};

const normalizeSearchValue = (value: string) => value
  .toLowerCase()
  .replace(/[_\s]+/g, ' ')
  .trim();

const stripLeadingNumeric = (value: string) => value.replace(/^\d+[_-]?/, '');

const normalizeFolderName = (value: string) => {
  const withoutSlash = value.replace(/\/$/, '');
  return normalizeSearchValue(stripLeadingNumeric(withoutSlash));
};

const tryMatchFolder = (prefixes: string[], normalizedQuery: string) => {
  for (const prefix of prefixes) {
    const folder = prefix.replace(/\/$/, '');
    const normalizedFolder = normalizeFolderName(folder);
    if (normalizedFolder.includes(normalizedQuery)) {
      return folder;
    }
  }
  return null;
};

const findFolderMatch = async (query: string) => {
  const normalizedQuery = normalizeSearchValue(query);
  if (!normalizedQuery) return null;

  const prefixes = await getFolderPrefixes();
  const initialMatch = tryMatchFolder(prefixes, normalizedQuery);
  if (initialMatch) {
    return initialMatch;
  }

  const refreshed = await refreshFolderCache();
  if (refreshed === prefixes) {
    return null;
  }

  return tryMatchFolder(refreshed, normalizedQuery);
};

const listFolderObjects = async (rawPrefix: string) => {
  const trimmed = normalizePrefix(rawPrefix);
  const effectivePrefix = trimmed ? ensureTrailingSlash(trimmed) : '';
  const objects: S3Object[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: STAMMDATEN_BUCKET,
      Prefix: effectivePrefix,
      ContinuationToken: continuationToken
    }));

    (response.Contents || []).forEach((item) => {
      if (!item.Key) return;
      if (item.Key.endsWith('/') && (!item.Size || item.Size === 0)) return;
      objects.push(item);
    });

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  return { objects, prefix: effectivePrefix };
};

// Socket.io an Router übergeben
setAuthSocketIO(io);
setVerifySocketIO(io);
setOrganigrammSocketIO(io);
setTelefonSocketIO(io);
setFormulareSocketIO(io);
setLeadsSocketIO(io);
setProject6SocketIO(io);

// EJS View Engine Setup
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '../views'));

// Middleware mit erhöhtem Limit für große Datensätze
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Statische Dateien aus dem public-Ordner servieren
app.use(express.static(path.join(__dirname, '../public')));

// API-Routes
app.use('/api/auth', authRouter);
app.use('/api/pdf', pdfRouter);
app.use('/api/verify', verifyRouter);
app.use('/api/organigramm', organigrammRouter);
app.use('/api/telefon', telefonRouter);
app.use('/api/formulare', formulareRouter);
app.use('/api/leads', leadsRouter);
app.use('/api/project6', project6Router);

app.post('/api/stammdaten/search', async (req, res) => {
  const { query } = req.body;
  const rawPrefix = typeof query === 'string' ? query.trim() : '';

  if (!rawPrefix) {
    return res.status(400).json({ success: false, error: 'Suchbegriff fehlt' });
  }

  try {
    let resolvedPrefix = normalizePrefix(rawPrefix).replace(/\/$/, '');
    let autoMatched = false;
    let { objects, prefix } = await listFolderObjects(rawPrefix);

    if (objects.length === 0) {
      const match = await findFolderMatch(rawPrefix);
      if (match) {
        autoMatched = true;
        resolvedPrefix = match;
        const result = await listFolderObjects(match);
        objects = result.objects;
        prefix = result.prefix;
      }
    }

    if (objects.length === 0) {
      return res.status(404).json({ success: false, error: 'Keine Treffer', prefix: resolvedPrefix });
    }

    const totalSize = objects.reduce((sum, obj) => sum + (obj.Size || 0), 0);
    const canonicalPrefix = prefix.replace(/\/$/, '');
    if (!resolvedPrefix) {
      resolvedPrefix = canonicalPrefix;
    }

    res.json({
      success: true,
      prefix,
      resolvedPrefix,
      totalFiles: objects.length,
      totalSize,
      autoMatched,
      items: objects.map((obj) => ({
        key: obj.Key,
        size: obj.Size || 0,
        lastModified: obj.LastModified
      }))
    });
  } catch (error: any) {
    console.error('[STAMMDATEN] Suche Fehler:', error.message || error);
    res.status(500).json({ success: false, error: 'S3 Suche fehlgeschlagen' });
  }
});

app.get('/api/stammdaten/download', async (req, res) => {
  const rawPrefix = typeof req.query.prefix === 'string' ? req.query.prefix : '';

  if (!rawPrefix) {
    return res.status(400).json({ success: false, error: 'Prefix fehlt' });
  }

  try {
    const { objects, prefix } = await listFolderObjects(rawPrefix);

    if (objects.length === 0) {
      return res.status(404).json({ success: false, error: 'Keine Dateien gefunden' });
    }

    const zipName = `${normalizePrefix(rawPrefix).replace(/\//g, '_') || 'stammdaten'}.zip`;

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.on('error', (archiveError) => {
      console.error('[STAMMDATEN] Archiv Fehler:', archiveError);
      if (!res.headersSent) {
        res.status(500).json({ success: false, error: 'Archivierung fehlgeschlagen' });
      } else {
        res.end();
      }
    });

    archive.pipe(res);

    for (const obj of objects) {
      if (!obj.Key) continue;
      const relativePath = obj.Key.substring(prefix.length);
      if (!relativePath) continue;

      const file = await s3Client.send(new GetObjectCommand({
        Bucket: STAMMDATEN_BUCKET,
        Key: obj.Key
      }));

      const bodyStream = file.Body as Readable | undefined;
      if (!bodyStream) continue;

      archive.append(bodyStream, { name: relativePath });
    }

    archive.finalize();
  } catch (error: any) {
    console.error('[STAMMDATEN] Download Fehler:', error.message || error);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: 'Download fehlgeschlagen' });
    } else {
      res.end();
    }
  }
});

app.get('/api/stammdaten/list', async (_req, res) => {
  try {
    const rawList = await fs.readFile(DAVID_STRUCTURE_FILE, 'utf-8');
    const parsed = JSON.parse(rawList);

    if (!Array.isArray(parsed)) {
      return res.status(500).json({ success: false, error: 'Liste ist ungültig' });
    }

    const items = parsed
      .map((item) => (typeof item === 'string' ? item.trim() : ''))
      .filter((item) => item.length > 0);

    res.json({ success: true, items });
  } catch (error: any) {
    console.error('[STAMMDATEN] Liste Fehler:', error.message || error);
    res.status(500).json({ success: false, error: 'Liste konnte nicht geladen werden' });
  }
});

app.post('/api/stammdaten/download-bulk', async (req, res) => {
  const entriesInput: any[] = Array.isArray(req.body?.entries) ? req.body.entries : [];

  const mappedEntries: BulkDownloadEntry[] = entriesInput.map((entry: any, index: number) => {
    const prefix = typeof entry?.prefix === 'string' ? entry.prefix.trim() : '';
    const name = typeof entry?.name === 'string' ? entry.name.trim() : `Ordner_${index + 1}`;
    return { prefix, name };
  });

  const validEntries = mappedEntries.filter((entry) => entry.prefix.length > 0);

  if (validEntries.length === 0) {
    return res.status(400).json({ success: false, error: 'Keine gültigen Einträge' });
  }

  try {
    const collected: Array<{ entry: { prefix: string; name: string }; objects: S3Object[]; prefix: string }> = [];
    for (const entry of validEntries) {
      const { objects, prefix } = await listFolderObjects(entry.prefix);
      if (objects.length === 0) continue;
      collected.push({ entry, objects, prefix });
    }

    if (collected.length === 0) {
      return res.status(404).json({ success: false, error: 'Keine Objekte gefunden' });
    }

    const zipName = `stammdaten_bundle_${new Date().toISOString().replace(/[:.]/g, '-')}.zip`;
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.on('error', (archiveError) => {
      console.error('[STAMMDATEN] Bulk-Archiv Fehler:', archiveError);
      if (!res.headersSent) {
        res.status(500).json({ success: false, error: 'Archivierung fehlgeschlagen' });
      } else {
        res.end();
      }
    });

    archive.pipe(res);

    for (const { entry, objects, prefix } of collected) {
      const folderName = sanitizeZipSegment(entry.name || entry.prefix);
      for (const obj of objects) {
        if (!obj.Key) continue;

        const relativePath = obj.Key.substring(prefix.length) || path.posix.basename(obj.Key);
        const targetPath = path.posix.join(folderName, relativePath);

        const file = await s3Client.send(new GetObjectCommand({
          Bucket: STAMMDATEN_BUCKET,
          Key: obj.Key
        }));

        const bodyStream = file.Body as Readable | undefined;
        if (!bodyStream) continue;

        archive.append(bodyStream, { name: targetPath });
      }
    }

    archive.finalize();
  } catch (error: any) {
    console.error('[STAMMDATEN] Bulk Download Fehler:', error.message || error);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: 'Bulk-Download fehlgeschlagen' });
    } else {
      res.end();
    }
  }
});

const ALL_BUCKETS = [
  'crm-berglicht-bk-formulare-mit-pdfs',
  'crm-berglicht-data',
  'crm-berglicht-e-formulare-mit-fotos',
  'crm-berglicht-leads-formulare',
  'crm-berglicht-mitarbeiter-liste'
];

const calculateBucketSize = async (bucketName: string): Promise<{ size: number; objectCount: number }> => {
  let totalSize = 0;
  let objectCount = 0;
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
      ContinuationToken: continuationToken,
      MaxKeys: 1000
    }));

    if (response.Contents) {
      for (const obj of response.Contents) {
        if (obj.Size && obj.Size > 0) {
          totalSize += obj.Size;
          objectCount++;
        }
      }
    }

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  return { size: totalSize, objectCount };
};

app.get('/api/stammdaten/bucket-stats', async (_req, res) => {
  try {
    const results = await Promise.allSettled(
      ALL_BUCKETS.map(async (bucketName) => {
        try {
          const { size, objectCount } = await calculateBucketSize(bucketName);
          return {
            bucket: bucketName,
            size,
            objectCount,
            sizeGB: size / (1024 * 1024 * 1024)
          };
        } catch (error: any) {
          console.error(`[STAMMDATEN] Fehler bei Bucket ${bucketName}:`, error.message || error);
          return {
            bucket: bucketName,
            size: 0,
            objectCount: 0,
            sizeGB: 0,
            error: error.message || 'Unbekannter Fehler'
          };
        }
      })
    );

    const buckets = results.map((result) => {
      if (result.status === 'fulfilled') {
        return result.value;
      } else {
        return {
          bucket: 'unbekannt',
          size: 0,
          objectCount: 0,
          sizeGB: 0,
          error: result.reason?.message || 'Fehler beim Abrufen'
        };
      }
    });

    const totalSize = buckets.reduce((sum, b) => sum + b.size, 0);
    const totalObjects = buckets.reduce((sum, b) => sum + b.objectCount, 0);
    const totalSizeGB = totalSize / (1024 * 1024 * 1024);

    res.json({
      success: true,
      buckets,
      totals: {
        size: totalSize,
        sizeGB: totalSizeGB,
        objectCount: totalObjects
      }
    });
  } catch (error: any) {
    console.error('[STAMMDATEN] Bucket-Statistik Fehler:', error.message || error);
    res.status(500).json({ success: false, error: 'Statistik-Abruf fehlgeschlagen' });
  }
});

const listAllBucketObjects = async (bucketName: string): Promise<S3Object[]> => {
  const objects: S3Object[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
      ContinuationToken: continuationToken,
      MaxKeys: 1000
    }));

    if (response.Contents) {
      for (const obj of response.Contents) {
        if (obj.Size && obj.Size > 0) {
          objects.push(obj);
        }
      }
    }

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  return objects;
};

app.get('/api/stammdaten/download-all-buckets', async (_req, res) => {
  try {
    console.log('[STAMMDATEN] Starte Download aller Buckets...');

    const zipName = `alle_buckets_${new Date().toISOString().replace(/[:.]/g, '-')}.zip`;
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.on('error', (archiveError) => {
      console.error('[STAMMDATEN] Archiv Fehler:', archiveError);
      if (!res.headersSent) {
        res.status(500).json({ success: false, error: 'Archivierung fehlgeschlagen' });
      } else {
        res.end();
      }
    });

    archive.pipe(res);

    for (const bucketName of ALL_BUCKETS) {
      console.log(`[STAMMDATEN] Verarbeite Bucket: ${bucketName}`);
      try {
        const objects = await listAllBucketObjects(bucketName);
        console.log(`[STAMMDATEN] Bucket ${bucketName}: ${objects.length} Objekte gefunden`);

        for (const obj of objects) {
          if (!obj.Key) continue;

          try {
            const file = await s3Client.send(new GetObjectCommand({
              Bucket: bucketName,
              Key: obj.Key
            }));

            const bodyStream = file.Body as Readable | undefined;
            if (!bodyStream) continue;

            const bucketFolderName = bucketName.replace(/[^a-zA-Z0-9._-]/g, '_');
            const targetPath = `${bucketFolderName}/${obj.Key}`;

            archive.append(bodyStream, { name: targetPath });
          } catch (fileError: any) {
            console.error(`[STAMMDATEN] Fehler beim Laden von ${obj.Key} aus ${bucketName}:`, fileError.message || fileError);
            continue;
          }
        }
      } catch (bucketError: any) {
        console.error(`[STAMMDATEN] Fehler beim Verarbeiten von Bucket ${bucketName}:`, bucketError.message || bucketError);
        continue;
      }
    }

    archive.finalize();
    console.log('[STAMMDATEN] Download aller Buckets abgeschlossen');
  } catch (error: any) {
    console.error('[STAMMDATEN] Download aller Buckets Fehler:', error.message || error);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: 'Download fehlgeschlagen' });
    } else {
      res.end();
    }
  }
});

// Helper Route: Hole alle direkten oder alle Mitarbeiter-IDs
app.post('/api/get-subordinates', async (req, res) => {
  const { userId, includeManager, getAll } = req.body;
  
  try {
    const { createClient } = await import('@supabase/supabase-js');
    const supabaseUrl = process.env.SUPABASE_URL || '';
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
    
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({ success: false, error: 'Supabase nicht konfiguriert' });
    }
    
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    let userIds: string[] = [];
    
    if (getAll) {
      // Hole ALLE Mitarbeiter rekursiv
      const { data: allSubs, error } = await supabase
        .rpc('get_all_subordinates', { p_user_id: userId });
      
      if (error) {
        return res.status(500).json({ success: false, error: error.message });
      }
      
      userIds = allSubs?.map((s: any) => s.user_id) || [];
      console.log('[API] Hole ALLE Mitarbeiter:', userIds.length);
    } else {
      // Hole nur direkte Mitarbeiter
      const { data: subordinates, error } = await supabase
        .from('mitarbeiter_organigramm')
        .select('user_id')
        .eq('parent_user_id', userId);
      
      if (error) {
        return res.status(500).json({ success: false, error: error.message });
      }
      
      userIds = subordinates?.map(s => s.user_id) || [];
      console.log('[API] Hole direkte Mitarbeiter:', userIds.length);
    }
    
    // Füge Manager hinzu falls gewünscht
    if (includeManager) {
      userIds.unshift(userId);
    }
    
    res.json({ success: true, userIds: userIds });
    
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Page Routes
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.get('/glm', (req, res) => {
  res.render('glm', { 
    title: 'GEO LEAD MAPPING',
    googleMapsApiKey: process.env.GOOGLE_MAPS_API_KEY || '' 
  });
});

app.get('/team-leads', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/team-leads.html'));
});

app.get('/project-6', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/project-6.html'));
});

app.get('/database', (req, res) => {
  res.render('database', { 
    title: 'Database'
  });
});

app.get('/stammdaten', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/stammdaten.html'));
});

// Socket.io-Verbindungen verwalten
io.on('connection', (socket) => {
  console.log('[SOCKET] Client verbunden:', socket.id);

  socket.on('filter-start', (data) => {
    console.log('\n\x1b[32m' + '='.repeat(80) + '\x1b[0m');
    console.log('\x1b[32m' + '=== FILTER START: ' + data.name + ' ==='.padEnd(80, '=') + '\x1b[0m');
    console.log('\x1b[32m' + '='.repeat(80) + '\x1b[0m\n');
  });

  socket.on('filter-complete', (data) => {
    console.log('\n\x1b[31m' + '='.repeat(80) + '\x1b[0m');
    console.log('\x1b[31m' + '=== FILTER ABGESCHLOSSEN ==='.padEnd(80, '=') + '\x1b[0m');
    console.log('\x1b[31m' + `Name: ${data.name} | Formulare: ${data.formulare} | MA-Formulare: ${data.mitarbeiterFormulare} | MA-Leads: ${data.mitarbeiterLeads}`.padEnd(80) + '\x1b[0m');
    console.log('\x1b[31m' + '='.repeat(80) + '\x1b[0m\n');
  });

  // Mitarbeiter-Suche in Supabase
  socket.on('search-mitarbeiter', async (data) => {
    const searchName = data.name;
    const showAll = data.showAll || false;
    console.log('[SOCKET] Suche nach Mitarbeiter:', searchName, '| Modus:', showAll ? 'Alle' : 'Direkte');
    
    try {
      // Supabase Import (lazy)
      const { createClient } = await import('@supabase/supabase-js');
      const supabaseUrl = process.env.SUPABASE_URL || '';
      const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
      
      if (!supabaseUrl || !supabaseKey) {
        socket.emit('search-result', { found: false, error: 'Supabase nicht konfiguriert' });
        return;
      }
      
      const supabase = createClient(supabaseUrl, supabaseKey);
      
      // Suche nach Mitarbeiter (alle Treffer!)
      console.log('[SOCKET] Suche in Supabase nach:', searchName);
      const { data: allMatches, error: searchError } = await supabase
        .from('mitarbeiter_organigramm')
        .select('*')
        .ilike('full_name', `%${searchName}%`)
        .order('anzahl_unterstellte', { ascending: false });
      
      if (searchError) {
        console.error('[SOCKET] Supabase Fehler:', searchError);
        socket.emit('search-result', { found: false, error: searchError.message });
        return;
      }
      
      if (!allMatches || allMatches.length === 0) {
        console.log('[SOCKET] Mitarbeiter nicht gefunden:', searchName);
        socket.emit('search-result', { found: false });
        return;
      }
      
      // Nimm den mit den MEISTEN unterstellten Mitarbeitern (wichtigster)
      const manager = allMatches[0];
      
      console.log('[SOCKET] Mitarbeiter gefunden:', manager.full_name, '(VC24:', manager.vc24_number, ')');
      console.log('[SOCKET] Manager E-Form:', manager.erledigte_formulare);
      console.log('[SOCKET] Manager E-Form-Gesamt:', manager.erledigte_formulare_gesamt);
      
      if (allMatches.length > 1) {
        console.log('[SOCKET] Warnung: Mehrere Treffer gefunden!', allMatches.map(m => `${m.full_name} (${m.vc24_number})`));
      }
      
      let subordinates = [];
      
      if (showAll) {
        // Hole ALLE Mitarbeiter rekursiv - aber mit allen Feldern!
        // Die RPC-Funktion gibt nur wenige Felder zurück, daher hole ich sie separat
        const { data: allSubIds } = await supabase
          .rpc('get_all_subordinates', { p_user_id: manager.user_id });
        
        if (allSubIds && allSubIds.length > 0) {
          const userIds = allSubIds.map((s: any) => s.user_id);
          
          // Hole ALLE Daten für diese user_ids
          const { data: fullData } = await supabase
            .from('mitarbeiter_organigramm')
            .select('*')
            .in('user_id', userIds)
            .order('level', { ascending: true });
          
          subordinates = fullData || [];
        }
        
        console.log('[SOCKET] ALLE unterstellten Mitarbeiter:', subordinates.length);
      } else {
        // Hole nur direkte Mitarbeiter mit ALLEN Feldern
        const { data: directSubs, error: subError } = await supabase
          .from('mitarbeiter_organigramm')
          .select('*')
          .eq('parent_user_id', manager.user_id)
          .order('full_name');
        
        subordinates = directSubs || [];
        console.log('[SOCKET] Direkte Mitarbeiter:', subordinates.length);
      }
      
      socket.emit('search-result', {
        found: true,
        manager: manager,
        subordinates: subordinates,
        showAll: showAll
      });
      
    } catch (error: any) {
      console.error('[SOCKET] Suchfehler:', error.message);
      socket.emit('search-result', { found: false, error: error.message });
    }
  });

  // Reload by user_id (nach Telefonnummern-Laden)
  socket.on('reload-by-userid', async (data) => {
    const userId = data.userId;
    const showAll = data.showAll || false;
    console.log('[SOCKET] Reload für user_id:', userId, '| Modus:', showAll ? 'Alle' : 'Direkte');
    
    try {
      const { createClient } = await import('@supabase/supabase-js');
      const supabaseUrl = process.env.SUPABASE_URL || '';
      const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
      
      if (!supabaseUrl || !supabaseKey) {
        socket.emit('search-result', { found: false, error: 'Supabase nicht konfiguriert' });
        return;
      }
      
      const supabase = createClient(supabaseUrl, supabaseKey);
      
      // Hole Mitarbeiter per user_id
      const { data: manager, error: managerError } = await supabase
        .from('mitarbeiter_organigramm')
        .select('*')
        .eq('user_id', userId)
        .single();
      
      if (managerError || !manager) {
        console.error('[SOCKET] Reload Fehler:', managerError);
        socket.emit('search-result', { found: false });
        return;
      }
      
      console.log('[SOCKET] Reload erfolgreich:', manager.full_name);
      console.log('[SOCKET] Manager E-Form:', manager.erledigte_formulare);
      console.log('[SOCKET] Manager E-Form-Gesamt:', manager.erledigte_formulare_gesamt);
      
      let subordinates = [];
      
      if (showAll) {
        // Hole ALLE Mitarbeiter rekursiv mit allen Feldern
        const { data: allSubIds } = await supabase
          .rpc('get_all_subordinates', { p_user_id: manager.user_id });
        
        if (allSubIds && allSubIds.length > 0) {
          const userIds = allSubIds.map((s: any) => s.user_id);
          
          // Hole ALLE Daten für diese user_ids
          const { data: fullData } = await supabase
            .from('mitarbeiter_organigramm')
            .select('*')
            .in('user_id', userIds)
            .order('level', { ascending: true });
          
          subordinates = fullData || [];
        }
      } else {
        // Hole nur direkte Mitarbeiter mit ALLEN Feldern
        const { data: directSubs } = await supabase
          .from('mitarbeiter_organigramm')
          .select('*')
          .eq('parent_user_id', manager.user_id)
          .order('full_name');
        subordinates = directSubs || [];
      }
      
      console.log('[SOCKET] Reload - Mitarbeiter:', subordinates.length);
      
      // Debug: Log erste 3 Mitarbeiter mit erledigte_formulare
      if (subordinates.length > 0) {
        subordinates.slice(0, 3).forEach((s: any) => {
          console.log('[SOCKET] Debug:', s.full_name, '- E-Form:', s.erledigte_formulare);
        });
      }
      
      socket.emit('search-result', {
        found: true,
        manager: manager,
        subordinates: subordinates,
        showAll: showAll
      });
      
    } catch (error: any) {
      console.error('[SOCKET] Reload Fehler:', error.message);
      socket.emit('search-result', { found: false, error: error.message });
    }
  });

  // Zähle Gesamt-Leads
  socket.on('count-total-leads', async () => {
    console.log('[LEADS] Starte Gesamt-Leads-Zählung...');
    
    try {
      // Triggere API-Request
      const response = await fetch(`http://localhost:${process.env.PORT || 3000}/api/leads/count-total`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ socketId: socket.id })
      });
      
      // Response wird über Socket.io Events gehandhabt
      
    } catch (error: any) {
      console.error('[LEADS] Fehler:', error.message);
      socket.emit('total-leads-result', { success: false, error: error.message });
    }
  });
  
  socket.on('stammdaten-search', (payload) => {
    const query = typeof payload?.query === 'string' ? payload.query : '';
    const meta = {
      socketId: socket.id,
      length: query.length,
      timestamp: payload?.timestamp || new Date().toISOString()
    };
    console.log('[STAMMDATEN] Eingabe:', { ...meta, query });
  });

  socket.on('disconnect', () => {
    console.log('[SOCKET] Client getrennt:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, () => {
  console.log(`Server läuft auf http://localhost:${PORT}`);
});


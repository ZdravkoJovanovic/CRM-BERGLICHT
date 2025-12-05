import dotenv from 'dotenv';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import path from 'path';
import { promises as fs } from 'fs';
import { Readable } from 'stream';
import archiver from 'archiver';
import { S3Client, ListObjectsV2Command, GetObjectCommand, _Object as S3Object } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import authRouter, { setSocketIO as setAuthSocketIO } from './routes/auth';
import pdfRouter from './routes/pdf';
import verifyRouter, { setSocketIO as setVerifySocketIO } from './routes/verify';
import organigrammRouter, { setSocketIO as setOrganigrammSocketIO } from './routes/organigramm';
import telefonRouter, { setSocketIO as setTelefonSocketIO } from './routes/telefon';
import formulareRouter, { setSocketIO as setFormulareSocketIO } from './routes/formulare';
import leadsRouter, { setSocketIO as setLeadsSocketIO } from './routes/leads';
import project6Router, { setSocketIO as setProject6SocketIO } from './routes/project6';

// .env-Datei explizit laden (aus dem Root-Verzeichnis)
// In CommonJS ist __dirname verfügbar, aber TypeScript erkennt es nicht immer
// Wir verwenden einen Workaround für TypeScript
const rootDir = path.resolve(__dirname, '..');
dotenv.config({ path: path.join(rootDir, '.env') });

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

// AWS Credentials aus .env lesen
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID?.trim() || '';
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY?.trim() || '';
const STAMMDATEN_BUCKET = 'crm-berglicht-leads-formulare';

// Verfügbare Buckets
const AVAILABLE_BUCKETS = [
  'crm-berglicht-bk-formulare-mit-pdfs',
  'crm-berglicht-data',
  'crm-berglicht-e-formulare-mit-fotos',
  'crm-berglicht-leads-formulare',
  'crm-berglicht-mitarbeiter-liste'
];

// Debug-Ausgabe für Credentials (ohne die tatsächlichen Werte zu zeigen)
if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
  console.warn('[WARN] AWS-Credentials nicht gesetzt. S3-Funktionen werden nicht funktionieren.');
  console.warn('[WARN] Bitte prüfe deine .env-Datei im Projekt-Root-Verzeichnis.');
  console.warn('[WARN] Erwartete Variablen: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY');
} else {
  console.log('[INFO] AWS-Credentials erfolgreich aus .env geladen.');
  console.log('[INFO] Access Key ID:', AWS_ACCESS_KEY_ID.substring(0, 8) + '...');
}

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'eu-north-1',
  credentials: AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY ? {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY
  } : undefined,
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

const checkAWSCredentials = (): { valid: boolean; error?: string } => {
  // Credentials dynamisch aus process.env lesen (falls .env nachträglich geladen wurde)
  const accessKey = process.env.AWS_ACCESS_KEY_ID?.trim() || '';
  const secretKey = process.env.AWS_SECRET_ACCESS_KEY?.trim() || '';
  
  if (!accessKey || !secretKey) {
    return {
      valid: false,
      error: 'AWS-Credentials nicht konfiguriert. Bitte AWS_ACCESS_KEY_ID und AWS_SECRET_ACCESS_KEY in der .env-Datei setzen.'
    };
  }
  if (accessKey === '' || secretKey === '') {
    return {
      valid: false,
      error: 'AWS-Credentials sind leer. Bitte AWS_ACCESS_KEY_ID und AWS_SECRET_ACCESS_KEY in der .env-Datei setzen.'
    };
  }
  return { valid: true };
};

const fetchFolderPrefixes = async (bucketName: string = STAMMDATEN_BUCKET) => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

  const prefixes: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
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

const findFolderMatch = async (query: string, bucketName: string = STAMMDATEN_BUCKET) => {
  const normalizedQuery = normalizeSearchValue(query);
  if (!normalizedQuery) return null;

  const prefixes = await fetchFolderPrefixes(bucketName);
  return tryMatchFolder(prefixes, normalizedQuery);
};

// Prüft, ob ein Ordner nur CSV-Dateien enthält (optimiert: bricht früh ab)
const hasOnlyCsvFiles = async (folderPrefix: string, bucketName: string): Promise<boolean> => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

  const effectivePrefix = ensureTrailingSlash(folderPrefix);
  let hasAnyFile = false;
  let continuationToken: string | undefined;
  const maxFilesToCheck = 100; // Maximal 100 Dateien prüfen für Performance
  let filesChecked = 0;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: effectivePrefix,
      MaxKeys: 100, // Weniger Dateien pro Request
      ContinuationToken: continuationToken
    }));

    for (const item of response.Contents || []) {
      if (!item.Key) continue;
      // Überspringe Ordner-Marker
      if (item.Key.endsWith('/') && (!item.Size || item.Size === 0)) continue;
      
      hasAnyFile = true;
      filesChecked++;
      
      const fileName = item.Key.substring(effectivePrefix.length);
      const extension = fileName.toLowerCase().split('.').pop() || '';
      
      // Wenn es eine Datei ist (nicht ein Unterordner) und nicht CSV
      if (extension && extension !== 'csv' && !fileName.endsWith('/')) {
        // Früher Abbruch: Nicht-CSV-Datei gefunden
        return false; // Hat andere Dateien, nicht nur CSV
      }
      
      // Performance: Stoppe nach maxFilesToCheck Dateien
      if (filesChecked >= maxFilesToCheck) {
        // Wenn wir schon viele Dateien geprüft haben und alle CSV waren,
        // nehmen wir an, dass der Ordner nur CSV hat
        return true;
      }
    }

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  // Wenn keine Dateien gefunden oder alle geprüften Dateien waren CSV, return true (nur CSV)
  return hasAnyFile;
};

// Listet Unterordner eines Mitarbeiter-Ordners auf
const listSubfolders = async (parentPrefix: string, bucketName: string): Promise<string[]> => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

  const effectivePrefix = ensureTrailingSlash(parentPrefix);
  const subfolders: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: effectivePrefix,
      Delimiter: '/',
      ContinuationToken: continuationToken
    }));

    (response.CommonPrefixes || []).forEach((entry) => {
      if (entry.Prefix) {
        subfolders.push(entry.Prefix);
      }
    });

    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);

  return subfolders;
};

const listFolderObjects = async (rawPrefix: string, bucketName: string = STAMMDATEN_BUCKET) => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

  const trimmed = normalizePrefix(rawPrefix);
  const effectivePrefix = trimmed ? ensureTrailingSlash(trimmed) : '';
  const objects: S3Object[] = [];
  let continuationToken: string | undefined;

  // Spezialfall: Wenn es ein Mitarbeiter-Ordner ist (z.B. "Abdullah_Ali_Farag/"),
  // liste die Unterordner (Kunden-Ordner) auf und filtere die, die nur CSV haben
  // Ein Mitarbeiter-Ordner ist ein Ordner auf der ersten Ebene (nur ein "/" am Ende)
  const prefixParts = effectivePrefix.split('/').filter(p => p.length > 0);
  const isMitarbeiterFolder = prefixParts.length === 1 && effectivePrefix.endsWith('/');
  
  if (isMitarbeiterFolder && bucketName === 'crm-berglicht-e-formulare-mit-fotos') {
    // Liste alle Unterordner (Kunden-Ordner) auf
    const subfolders = await listSubfolders(effectivePrefix, bucketName);
    console.log(`[STAMMDATEN] Mitarbeiter ${effectivePrefix}: ${subfolders.length} Kunden-Ordner gefunden`);
    
    // Filtere: Nur Ordner anzeigen, die mehr als nur CSV enthalten
    let filteredCount = 0;
    for (const subfolder of subfolders) {
      try {
        const hasOnlyCsv = await hasOnlyCsvFiles(subfolder, bucketName);
        if (!hasOnlyCsv) {
          // Erstelle ein "virtuelles" Objekt für den Ordner
          objects.push({
            Key: subfolder,
            Size: 0,
            LastModified: new Date()
          } as S3Object);
          filteredCount++;
        }
      } catch (error: any) {
        console.error(`[STAMMDATEN] Fehler beim Prüfen von ${subfolder}:`, error.message);
        // Bei Fehler: Ordner trotzdem anzeigen (sicherer)
        objects.push({
          Key: subfolder,
          Size: 0,
          LastModified: new Date()
        } as S3Object);
        filteredCount++;
      }
    }
    
    console.log(`[STAMMDATEN] ${filteredCount} von ${subfolders.length} Kunden-Ordnern haben andere Dateien als CSV`);
    return { objects, prefix: effectivePrefix };
  }

  // Normale Datei-Listung
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucketName,
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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

  const { query, bucket } = req.body;
  const rawPrefix = typeof query === 'string' ? query.trim() : '';
  const bucketName = typeof bucket === 'string' && AVAILABLE_BUCKETS.includes(bucket) 
    ? bucket 
    : STAMMDATEN_BUCKET;

  if (!rawPrefix) {
    return res.status(400).json({ success: false, error: 'Suchbegriff fehlt' });
  }

  try {
    let resolvedPrefix = normalizePrefix(rawPrefix).replace(/\/$/, '');
    let autoMatched = false;
    let { objects, prefix } = await listFolderObjects(rawPrefix, bucketName);

    if (objects.length === 0) {
      const match = await findFolderMatch(rawPrefix, bucketName);
      if (match) {
        autoMatched = true;
        resolvedPrefix = match;
        const result = await listFolderObjects(match, bucketName);
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
      bucket: bucketName,
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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

  const rawPrefix = typeof req.query.prefix === 'string' ? req.query.prefix : '';
  const bucketName = typeof req.query.bucket === 'string' && AVAILABLE_BUCKETS.includes(req.query.bucket)
    ? req.query.bucket
    : STAMMDATEN_BUCKET;

  if (!rawPrefix) {
    return res.status(400).json({ success: false, error: 'Prefix fehlt' });
  }

  try {
    // REKURSIV alle Dateien aus dem Ordner und allen Unterordnern holen
    const effectivePrefix = ensureTrailingSlash(rawPrefix);
    const allObjects: S3Object[] = [];
    let continuationToken: string | undefined;

    do {
      const response = await s3Client.send(new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: effectivePrefix,
        ContinuationToken: continuationToken
      }));

      (response.Contents || []).forEach((item) => {
        if (!item.Key) return;
        // Überspringe Ordner-Marker
        if (item.Key.endsWith('/') && (!item.Size || item.Size === 0)) return;
        allObjects.push(item);
      });

      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);

    if (allObjects.length === 0) {
      return res.status(404).json({ success: false, error: 'Keine Dateien gefunden' });
    }

    // Filtere: Nur Dateien die NICHT CSV sind
    const nonCsvObjects = allObjects.filter((obj) => {
      if (!obj.Key) return false;
      if (obj.Key.endsWith('/')) return false; // Überspringe Ordner
      
      const extension = (obj.Key.toLowerCase().split('.').pop() || '').toLowerCase();
      return extension !== 'csv';
    });

    if (nonCsvObjects.length === 0) {
      return res.status(404).json({ success: false, error: 'Keine nicht-CSV Dateien gefunden' });
    }

    // Erstelle Proxy-Links (über unseren Server, umgeht CORS)
    const downloadLinks: Array<{ url: string; filename: string; key: string }> = [];

    for (const obj of nonCsvObjects) {
      if (!obj.Key) continue;
      
      // relativePath z.B. "kunde@email.com/foto.jpg"
      const relativePath = obj.Key.substring(effectivePrefix.length);
      
      // Dateiname: Kundenordner + Dateiname (z.B. "kunde@email.com_foto.jpg")
      // Ersetze "/" durch "_" damit der Kundenordner im Dateinamen sichtbar ist
      const filename = relativePath.replace(/\//g, '_') || obj.Key.split('/').pop() || 'datei';
      
      downloadLinks.push({ 
        url: `/api/stammdaten/file?bucket=${encodeURIComponent(bucketName)}&key=${encodeURIComponent(obj.Key)}&filename=${encodeURIComponent(filename)}`,
        filename,
        key: obj.Key
      });
    }

    res.json({ 
      success: true, 
      count: downloadLinks.length,
      links: downloadLinks 
    });
  } catch (error: any) {
    console.error('[STAMMDATEN] Download Fehler:', error.message || error);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: 'Download fehlgeschlagen' });
    } else {
      res.end();
    }
  }
});

// Proxy-Route: Datei von S3 direkt streamen (umgeht CORS)
app.get('/api/stammdaten/file', async (req, res) => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ success: false, error: credCheck.error });
  }

  const key = typeof req.query.key === 'string' ? req.query.key : '';
  const bucketName = typeof req.query.bucket === 'string' && AVAILABLE_BUCKETS.includes(req.query.bucket)
    ? req.query.bucket
    : STAMMDATEN_BUCKET;
  
  // Dateiname aus Query oder aus Key extrahieren
  const customFilename = typeof req.query.filename === 'string' ? req.query.filename : '';

  if (!key) {
    return res.status(400).json({ success: false, error: 'Key fehlt' });
  }

  try {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key
    });

    const response = await s3Client.send(command);
    
    // Dateiname: Benutze den übergebenen Namen (enthält Kundenordner) oder fallback
    const filename = customFilename || key.split('/').pop() || 'datei';
    
    // Content-Type setzen
    res.setHeader('Content-Type', response.ContentType || 'application/octet-stream');
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`);
    
    if (response.ContentLength) {
      res.setHeader('Content-Length', response.ContentLength);
    }

    // Stream die Datei direkt zum Client
    if (response.Body) {
      const stream = response.Body as any;
      stream.pipe(res);
    } else {
      res.status(404).json({ success: false, error: 'Datei nicht gefunden' });
    }
  } catch (error: any) {
    console.error('[STAMMDATEN] File-Proxy Fehler:', error.message);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: 'Datei konnte nicht geladen werden' });
    }
  }
});

app.get('/api/stammdaten/buckets', async (_req, res) => {
  res.json({ success: true, buckets: AVAILABLE_BUCKETS });
});

// Schnelle Statistiken für einen Bucket (nur nicht-CSV Dateien)
app.post('/api/stammdaten/bucket-details', async (req, res) => {
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

  const { bucket, socketId } = req.body;
  const bucketName = typeof bucket === 'string' && AVAILABLE_BUCKETS.includes(bucket)
    ? bucket
    : 'crm-berglicht-e-formulare-mit-fotos';

  // Sofortige Antwort senden, dann im Hintergrund verarbeiten
  res.json({ success: true, message: 'Statistiken werden geladen...' });

  try {
    if (socketId && io) {
      io.to(socketId).emit('bucket-stats-progress', { message: 'Starte Analyse...', progress: 0 });
    }

    console.log(`[STAMMDATEN] Starte schnelle Statistiken für Bucket: ${bucketName}`);
    
    // Liste alle ersten-Level-Ordner (Mitarbeiter-Ordner)
    const mitarbeiterPrefixes = await fetchFolderPrefixes(bucketName);
    console.log(`[STAMMDATEN] Gefunden: ${mitarbeiterPrefixes.length} Mitarbeiter-Ordner`);
    
    if (socketId && io) {
      io.to(socketId).emit('bucket-stats-progress', { 
        message: `${mitarbeiterPrefixes.length} Mitarbeiter-Ordner gefunden, analysiere...`, 
        progress: 10 
      });
    }
    
    // Nur Dateitypen für NICHT-CSV Dateien
    const fileTypes = new Map<string, number>();
    let totalNonCsvFiles = 0;
    let processedMitarbeiter = 0;
    const mitarbeiterMitAnderenDateien = new Set<string>(); // Set für eindeutige Mitarbeiter-Namen
    
    // Optimierte Verarbeitung: Nur erste Dateien pro Kunden-Ordner prüfen
    const batchSize = 20; // Größere Batches für bessere Performance
    const maxFilesPerKunde = 50; // Nur erste 50 Dateien pro Kunden-Ordner prüfen (schneller)
    
    for (let i = 0; i < mitarbeiterPrefixes.length; i += batchSize) {
      const batch = mitarbeiterPrefixes.slice(i, i + batchSize);
      
      await Promise.all(batch.map(async (mitarbeiterPrefix) => {
        try {
          const mitarbeiterName = mitarbeiterPrefix.replace(/\/$/, '');
          const kundenPrefixes = await listSubfolders(mitarbeiterPrefix, bucketName);
          let hatAndereDateien = false;
          let filesChecked = 0;
          
          // Prüfe nur erste Dateien in jedem Kunden-Ordner (schneller)
          for (const kundenPrefix of kundenPrefixes) {
            if (hatAndereDateien && filesChecked > maxFilesPerKunde) break; // Früh abbrechen wenn gefunden
            
            const effectivePrefix = ensureTrailingSlash(kundenPrefix);
            
            try {
              const response = await s3Client.send(new ListObjectsV2Command({
                Bucket: bucketName,
                Prefix: effectivePrefix,
                MaxKeys: 100 // Weniger Dateien pro Request
              }));

              (response.Contents || []).forEach((item) => {
                if (!item.Key) return;
                if (item.Key.endsWith('/') && (!item.Size || item.Size === 0)) return;
                
                filesChecked++;
                const extension = (item.Key.toLowerCase().split('.').pop() || '').toLowerCase();
                
                // Nur NICHT-CSV Dateien zählen
                if (extension && extension !== 'csv') {
                  totalNonCsvFiles++;
                  fileTypes.set(extension, (fileTypes.get(extension) || 0) + 1);
                  hatAndereDateien = true;
                }
              });
            } catch (kundenError: any) {
              // Überspringe fehlerhafte Kunden-Ordner
              continue;
            }
          }
          
          // Wenn dieser Mitarbeiter andere Dateien hat, zur Liste hinzufügen
          if (hatAndereDateien) {
            mitarbeiterMitAnderenDateien.add(mitarbeiterName);
          }
        } catch (error: any) {
          console.error(`[STAMMDATEN] Fehler bei Mitarbeiter ${mitarbeiterPrefix}:`, error.message);
        }
      }));
      
      processedMitarbeiter += batch.length;
      const progress = Math.min(90, 10 + Math.floor((processedMitarbeiter / mitarbeiterPrefixes.length) * 80));
      
      if (socketId && io) {
        io.to(socketId).emit('bucket-stats-progress', { 
          message: `Verarbeitet ${processedMitarbeiter}/${mitarbeiterPrefixes.length} Mitarbeiter...`, 
          progress 
        });
      }
      
      // Log alle 100 Mitarbeiter
      if (processedMitarbeiter % 100 === 0) {
        console.log(`[STAMMDATEN] Fortschritt: ${processedMitarbeiter}/${mitarbeiterPrefixes.length} (${progress}%)`);
      }
    }
    
    // Sortiere Dateitypen nach Häufigkeit
    const sortedFileTypes = Array.from(fileTypes.entries())
      .sort((a, b) => b[1] - a[1]);
    
    // Konvertiere Set zu sortiertem Array
    const mitarbeiterListe = Array.from(mitarbeiterMitAnderenDateien).sort();
    
    const stats = {
      bucket: bucketName,
      totalNonCsvFiles,
      mitarbeiterCount: mitarbeiterListe.length,
      mitarbeiter: mitarbeiterListe,
      fileTypes: sortedFileTypes.map(([ext, count]) => ({ extension: ext, count }))
    };

    console.log(`[STAMMDATEN] Statistiken geladen: ${totalNonCsvFiles} nicht-CSV Dateien, ${sortedFileTypes.length} verschiedene Formate`);
    
    if (socketId && io) {
      io.to(socketId).emit('bucket-stats-complete', { success: true, stats });
    }
  } catch (error: any) {
    console.error('[STAMMDATEN] Bucket-Details Fehler:', error.message || error);
    if (socketId && io) {
      io.to(socketId).emit('bucket-stats-error', { error: error.message || 'Statistiken konnten nicht geladen werden' });
    }
  }
});

app.get('/api/stammdaten/list', async (req, res) => {
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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    throw new Error(credCheck.error);
  }

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
  const credCheck = checkAWSCredentials();
  if (!credCheck.valid) {
    return res.status(503).json({ 
      success: false, 
      error: credCheck.error,
      code: 'AWS_CREDENTIALS_MISSING'
    });
  }

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


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
const dotenv_1 = __importDefault(require("dotenv"));
const express_1 = __importDefault(require("express"));
const http_1 = require("http");
const socket_io_1 = require("socket.io");
const path_1 = __importDefault(require("path"));
const fs_1 = require("fs");
const archiver_1 = __importDefault(require("archiver"));
const client_s3_1 = require("@aws-sdk/client-s3");
const auth_1 = __importStar(require("./routes/auth"));
const pdf_1 = __importDefault(require("./routes/pdf"));
const verify_1 = __importStar(require("./routes/verify"));
const organigramm_1 = __importStar(require("./routes/organigramm"));
const telefon_1 = __importStar(require("./routes/telefon"));
const formulare_1 = __importStar(require("./routes/formulare"));
const leads_1 = __importStar(require("./routes/leads"));
const project6_1 = __importStar(require("./routes/project6"));
// .env-Datei explizit laden (aus dem Root-Verzeichnis)
// In CommonJS ist __dirname verfügbar, aber TypeScript erkennt es nicht immer
// Wir verwenden einen Workaround für TypeScript
const rootDir = path_1.default.resolve(__dirname, '..');
dotenv_1.default.config({ path: path_1.default.join(rootDir, '.env') });
const app = (0, express_1.default)();
const httpServer = (0, http_1.createServer)(app);
const io = new socket_io_1.Server(httpServer);
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
}
else {
    console.log('[INFO] AWS-Credentials erfolgreich aus .env geladen.');
    console.log('[INFO] Access Key ID:', AWS_ACCESS_KEY_ID.substring(0, 8) + '...');
}
const s3Client = new client_s3_1.S3Client({
    region: process.env.AWS_REGION || 'eu-north-1',
    credentials: AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY ? {
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY
    } : undefined,
    maxAttempts: 3
});
const ROOT_DIR = path_1.default.join(__dirname, '..');
const DAVID_STRUCTURE_FILE = path_1.default.join(ROOT_DIR, 'david_structure.json');
const normalizePrefix = (input) => input.trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
const ensureTrailingSlash = (value) => (value.endsWith('/') ? value : `${value}/`);
const sanitizeZipSegment = (value) => {
    if (!value)
        return 'ordner';
    return value.replace(/[^a-zA-Z0-9._-]/g, '_');
};
const FOLDER_CACHE_TTL_MS = 5 * 60 * 1000;
let cachedFolderPrefixes = [];
let folderCacheExpires = 0;
let folderCachePromise = null;
const checkAWSCredentials = () => {
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
const fetchFolderPrefixes = async (bucketName = STAMMDATEN_BUCKET) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    const prefixes = [];
    let continuationToken;
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
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
const normalizeSearchValue = (value) => value
    .toLowerCase()
    .replace(/[_\s]+/g, ' ')
    .trim();
const stripLeadingNumeric = (value) => value.replace(/^\d+[_-]?/, '');
const normalizeFolderName = (value) => {
    const withoutSlash = value.replace(/\/$/, '');
    return normalizeSearchValue(stripLeadingNumeric(withoutSlash));
};
const tryMatchFolder = (prefixes, normalizedQuery) => {
    for (const prefix of prefixes) {
        const folder = prefix.replace(/\/$/, '');
        const normalizedFolder = normalizeFolderName(folder);
        if (normalizedFolder.includes(normalizedQuery)) {
            return folder;
        }
    }
    return null;
};
const findFolderMatch = async (query, bucketName = STAMMDATEN_BUCKET) => {
    const normalizedQuery = normalizeSearchValue(query);
    if (!normalizedQuery)
        return null;
    const prefixes = await fetchFolderPrefixes(bucketName);
    return tryMatchFolder(prefixes, normalizedQuery);
};
// Prüft, ob ein Ordner nur CSV-Dateien enthält (optimiert: bricht früh ab)
const hasOnlyCsvFiles = async (folderPrefix, bucketName) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    const effectivePrefix = ensureTrailingSlash(folderPrefix);
    let hasAnyFile = false;
    let continuationToken;
    const maxFilesToCheck = 100; // Maximal 100 Dateien prüfen für Performance
    let filesChecked = 0;
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: effectivePrefix,
            MaxKeys: 100, // Weniger Dateien pro Request
            ContinuationToken: continuationToken
        }));
        for (const item of response.Contents || []) {
            if (!item.Key)
                continue;
            // Überspringe Ordner-Marker
            if (item.Key.endsWith('/') && (!item.Size || item.Size === 0))
                continue;
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
const listSubfolders = async (parentPrefix, bucketName) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    const effectivePrefix = ensureTrailingSlash(parentPrefix);
    const subfolders = [];
    let continuationToken;
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
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
const listFolderObjects = async (rawPrefix, bucketName = STAMMDATEN_BUCKET) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    const trimmed = normalizePrefix(rawPrefix);
    const effectivePrefix = trimmed ? ensureTrailingSlash(trimmed) : '';
    const objects = [];
    let continuationToken;
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
                    });
                    filteredCount++;
                }
            }
            catch (error) {
                console.error(`[STAMMDATEN] Fehler beim Prüfen von ${subfolder}:`, error.message);
                // Bei Fehler: Ordner trotzdem anzeigen (sicherer)
                objects.push({
                    Key: subfolder,
                    Size: 0,
                    LastModified: new Date()
                });
                filteredCount++;
            }
        }
        console.log(`[STAMMDATEN] ${filteredCount} von ${subfolders.length} Kunden-Ordnern haben andere Dateien als CSV`);
        return { objects, prefix: effectivePrefix };
    }
    // Normale Datei-Listung
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: effectivePrefix,
            ContinuationToken: continuationToken
        }));
        (response.Contents || []).forEach((item) => {
            if (!item.Key)
                return;
            if (item.Key.endsWith('/') && (!item.Size || item.Size === 0))
                return;
            objects.push(item);
        });
        continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
    return { objects, prefix: effectivePrefix };
};
// Socket.io an Router übergeben
(0, auth_1.setSocketIO)(io);
(0, verify_1.setSocketIO)(io);
(0, organigramm_1.setSocketIO)(io);
(0, telefon_1.setSocketIO)(io);
(0, formulare_1.setSocketIO)(io);
(0, leads_1.setSocketIO)(io);
(0, project6_1.setSocketIO)(io);
// EJS View Engine Setup
app.set('view engine', 'ejs');
app.set('views', path_1.default.join(__dirname, '../views'));
// Middleware mit erhöhtem Limit für große Datensätze
app.use(express_1.default.json({ limit: '50mb' }));
app.use(express_1.default.urlencoded({ extended: true, limit: '50mb' }));
// Statische Dateien aus dem public-Ordner servieren
app.use(express_1.default.static(path_1.default.join(__dirname, '../public')));
// API-Routes
app.use('/api/auth', auth_1.default);
app.use('/api/pdf', pdf_1.default);
app.use('/api/verify', verify_1.default);
app.use('/api/organigramm', organigramm_1.default);
app.use('/api/telefon', telefon_1.default);
app.use('/api/formulare', formulare_1.default);
app.use('/api/leads', leads_1.default);
app.use('/api/project6', project6_1.default);
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
    }
    catch (error) {
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
        const allObjects = [];
        let continuationToken;
        do {
            const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
                Bucket: bucketName,
                Prefix: effectivePrefix,
                ContinuationToken: continuationToken
            }));
            (response.Contents || []).forEach((item) => {
                if (!item.Key)
                    return;
                // Überspringe Ordner-Marker
                if (item.Key.endsWith('/') && (!item.Size || item.Size === 0))
                    return;
                allObjects.push(item);
            });
            continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
        } while (continuationToken);
        if (allObjects.length === 0) {
            return res.status(404).json({ success: false, error: 'Keine Dateien gefunden' });
        }
        // Filtere: Nur Dateien die NICHT CSV sind
        const nonCsvObjects = allObjects.filter((obj) => {
            if (!obj.Key)
                return false;
            if (obj.Key.endsWith('/'))
                return false; // Überspringe Ordner
            const extension = (obj.Key.toLowerCase().split('.').pop() || '').toLowerCase();
            return extension !== 'csv';
        });
        if (nonCsvObjects.length === 0) {
            return res.status(404).json({ success: false, error: 'Keine nicht-CSV Dateien gefunden' });
        }
        // Erstelle Proxy-Links (über unseren Server, umgeht CORS)
        const downloadLinks = [];
        for (const obj of nonCsvObjects) {
            if (!obj.Key)
                continue;
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
    }
    catch (error) {
        console.error('[STAMMDATEN] Download Fehler:', error.message || error);
        if (!res.headersSent) {
            res.status(500).json({ success: false, error: 'Download fehlgeschlagen' });
        }
        else {
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
        const command = new client_s3_1.GetObjectCommand({
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
            const stream = response.Body;
            stream.pipe(res);
        }
        else {
            res.status(404).json({ success: false, error: 'Datei nicht gefunden' });
        }
    }
    catch (error) {
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
        const fileTypes = new Map();
        let totalNonCsvFiles = 0;
        let processedMitarbeiter = 0;
        const mitarbeiterMitAnderenDateien = new Set(); // Set für eindeutige Mitarbeiter-Namen
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
                        if (hatAndereDateien && filesChecked > maxFilesPerKunde)
                            break; // Früh abbrechen wenn gefunden
                        const effectivePrefix = ensureTrailingSlash(kundenPrefix);
                        try {
                            const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
                                Bucket: bucketName,
                                Prefix: effectivePrefix,
                                MaxKeys: 100 // Weniger Dateien pro Request
                            }));
                            (response.Contents || []).forEach((item) => {
                                if (!item.Key)
                                    return;
                                if (item.Key.endsWith('/') && (!item.Size || item.Size === 0))
                                    return;
                                filesChecked++;
                                const extension = (item.Key.toLowerCase().split('.').pop() || '').toLowerCase();
                                // Nur NICHT-CSV Dateien zählen
                                if (extension && extension !== 'csv') {
                                    totalNonCsvFiles++;
                                    fileTypes.set(extension, (fileTypes.get(extension) || 0) + 1);
                                    hatAndereDateien = true;
                                }
                            });
                        }
                        catch (kundenError) {
                            // Überspringe fehlerhafte Kunden-Ordner
                            continue;
                        }
                    }
                    // Wenn dieser Mitarbeiter andere Dateien hat, zur Liste hinzufügen
                    if (hatAndereDateien) {
                        mitarbeiterMitAnderenDateien.add(mitarbeiterName);
                    }
                }
                catch (error) {
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
    }
    catch (error) {
        console.error('[STAMMDATEN] Bucket-Details Fehler:', error.message || error);
        if (socketId && io) {
            io.to(socketId).emit('bucket-stats-error', { error: error.message || 'Statistiken konnten nicht geladen werden' });
        }
    }
});
app.get('/api/stammdaten/list', async (req, res) => {
    try {
        const rawList = await fs_1.promises.readFile(DAVID_STRUCTURE_FILE, 'utf-8');
        const parsed = JSON.parse(rawList);
        if (!Array.isArray(parsed)) {
            return res.status(500).json({ success: false, error: 'Liste ist ungültig' });
        }
        const items = parsed
            .map((item) => (typeof item === 'string' ? item.trim() : ''))
            .filter((item) => item.length > 0);
        res.json({ success: true, items });
    }
    catch (error) {
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
    const entriesInput = Array.isArray(req.body?.entries) ? req.body.entries : [];
    const mappedEntries = entriesInput.map((entry, index) => {
        const prefix = typeof entry?.prefix === 'string' ? entry.prefix.trim() : '';
        const name = typeof entry?.name === 'string' ? entry.name.trim() : `Ordner_${index + 1}`;
        return { prefix, name };
    });
    const validEntries = mappedEntries.filter((entry) => entry.prefix.length > 0);
    if (validEntries.length === 0) {
        return res.status(400).json({ success: false, error: 'Keine gültigen Einträge' });
    }
    try {
        const collected = [];
        for (const entry of validEntries) {
            const { objects, prefix } = await listFolderObjects(entry.prefix);
            if (objects.length === 0)
                continue;
            collected.push({ entry, objects, prefix });
        }
        if (collected.length === 0) {
            return res.status(404).json({ success: false, error: 'Keine Objekte gefunden' });
        }
        const zipName = `stammdaten_bundle_${new Date().toISOString().replace(/[:.]/g, '-')}.zip`;
        res.setHeader('Content-Type', 'application/zip');
        res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);
        const archive = (0, archiver_1.default)('zip', { zlib: { level: 9 } });
        archive.on('error', (archiveError) => {
            console.error('[STAMMDATEN] Bulk-Archiv Fehler:', archiveError);
            if (!res.headersSent) {
                res.status(500).json({ success: false, error: 'Archivierung fehlgeschlagen' });
            }
            else {
                res.end();
            }
        });
        archive.pipe(res);
        for (const { entry, objects, prefix } of collected) {
            const folderName = sanitizeZipSegment(entry.name || entry.prefix);
            for (const obj of objects) {
                if (!obj.Key)
                    continue;
                const relativePath = obj.Key.substring(prefix.length) || path_1.default.posix.basename(obj.Key);
                const targetPath = path_1.default.posix.join(folderName, relativePath);
                const file = await s3Client.send(new client_s3_1.GetObjectCommand({
                    Bucket: STAMMDATEN_BUCKET,
                    Key: obj.Key
                }));
                const bodyStream = file.Body;
                if (!bodyStream)
                    continue;
                archive.append(bodyStream, { name: targetPath });
            }
        }
        archive.finalize();
    }
    catch (error) {
        console.error('[STAMMDATEN] Bulk Download Fehler:', error.message || error);
        if (!res.headersSent) {
            res.status(500).json({ success: false, error: 'Bulk-Download fehlgeschlagen' });
        }
        else {
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
const calculateBucketSize = async (bucketName) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    let totalSize = 0;
    let objectCount = 0;
    let continuationToken;
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
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
        const results = await Promise.allSettled(ALL_BUCKETS.map(async (bucketName) => {
            try {
                const { size, objectCount } = await calculateBucketSize(bucketName);
                return {
                    bucket: bucketName,
                    size,
                    objectCount,
                    sizeGB: size / (1024 * 1024 * 1024)
                };
            }
            catch (error) {
                console.error(`[STAMMDATEN] Fehler bei Bucket ${bucketName}:`, error.message || error);
                return {
                    bucket: bucketName,
                    size: 0,
                    objectCount: 0,
                    sizeGB: 0,
                    error: error.message || 'Unbekannter Fehler'
                };
            }
        }));
        const buckets = results.map((result) => {
            if (result.status === 'fulfilled') {
                return result.value;
            }
            else {
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
    }
    catch (error) {
        console.error('[STAMMDATEN] Bucket-Statistik Fehler:', error.message || error);
        res.status(500).json({ success: false, error: 'Statistik-Abruf fehlgeschlagen' });
    }
});
const listAllBucketObjects = async (bucketName) => {
    const credCheck = checkAWSCredentials();
    if (!credCheck.valid) {
        throw new Error(credCheck.error);
    }
    const objects = [];
    let continuationToken;
    do {
        const response = await s3Client.send(new client_s3_1.ListObjectsV2Command({
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
        const archive = (0, archiver_1.default)('zip', { zlib: { level: 9 } });
        archive.on('error', (archiveError) => {
            console.error('[STAMMDATEN] Archiv Fehler:', archiveError);
            if (!res.headersSent) {
                res.status(500).json({ success: false, error: 'Archivierung fehlgeschlagen' });
            }
            else {
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
                    if (!obj.Key)
                        continue;
                    try {
                        const file = await s3Client.send(new client_s3_1.GetObjectCommand({
                            Bucket: bucketName,
                            Key: obj.Key
                        }));
                        const bodyStream = file.Body;
                        if (!bodyStream)
                            continue;
                        const bucketFolderName = bucketName.replace(/[^a-zA-Z0-9._-]/g, '_');
                        const targetPath = `${bucketFolderName}/${obj.Key}`;
                        archive.append(bodyStream, { name: targetPath });
                    }
                    catch (fileError) {
                        console.error(`[STAMMDATEN] Fehler beim Laden von ${obj.Key} aus ${bucketName}:`, fileError.message || fileError);
                        continue;
                    }
                }
            }
            catch (bucketError) {
                console.error(`[STAMMDATEN] Fehler beim Verarbeiten von Bucket ${bucketName}:`, bucketError.message || bucketError);
                continue;
            }
        }
        archive.finalize();
        console.log('[STAMMDATEN] Download aller Buckets abgeschlossen');
    }
    catch (error) {
        console.error('[STAMMDATEN] Download aller Buckets Fehler:', error.message || error);
        if (!res.headersSent) {
            res.status(500).json({ success: false, error: 'Download fehlgeschlagen' });
        }
        else {
            res.end();
        }
    }
});
// Helper Route: Hole alle direkten oder alle Mitarbeiter-IDs
app.post('/api/get-subordinates', async (req, res) => {
    const { userId, includeManager, getAll } = req.body;
    try {
        const { createClient } = await Promise.resolve().then(() => __importStar(require('@supabase/supabase-js')));
        const supabaseUrl = process.env.SUPABASE_URL || '';
        const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
        if (!supabaseUrl || !supabaseKey) {
            return res.status(500).json({ success: false, error: 'Supabase nicht konfiguriert' });
        }
        const supabase = createClient(supabaseUrl, supabaseKey);
        let userIds = [];
        if (getAll) {
            // Hole ALLE Mitarbeiter rekursiv
            const { data: allSubs, error } = await supabase
                .rpc('get_all_subordinates', { p_user_id: userId });
            if (error) {
                return res.status(500).json({ success: false, error: error.message });
            }
            userIds = allSubs?.map((s) => s.user_id) || [];
            console.log('[API] Hole ALLE Mitarbeiter:', userIds.length);
        }
        else {
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
    }
    catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});
// Page Routes
app.get('/', (req, res) => {
    res.sendFile(path_1.default.join(__dirname, '../public/index.html'));
});
app.get('/glm', (req, res) => {
    res.render('glm', {
        title: 'GEO LEAD MAPPING',
        googleMapsApiKey: process.env.GOOGLE_MAPS_API_KEY || ''
    });
});
app.get('/team-leads', (req, res) => {
    res.sendFile(path_1.default.join(__dirname, '../public/team-leads.html'));
});
app.get('/project-6', (req, res) => {
    res.sendFile(path_1.default.join(__dirname, '../public/project-6.html'));
});
app.get('/database', (req, res) => {
    res.render('database', {
        title: 'Database'
    });
});
app.get('/stammdaten', (req, res) => {
    res.sendFile(path_1.default.join(__dirname, '../public/stammdaten.html'));
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
            const { createClient } = await Promise.resolve().then(() => __importStar(require('@supabase/supabase-js')));
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
                    const userIds = allSubIds.map((s) => s.user_id);
                    // Hole ALLE Daten für diese user_ids
                    const { data: fullData } = await supabase
                        .from('mitarbeiter_organigramm')
                        .select('*')
                        .in('user_id', userIds)
                        .order('level', { ascending: true });
                    subordinates = fullData || [];
                }
                console.log('[SOCKET] ALLE unterstellten Mitarbeiter:', subordinates.length);
            }
            else {
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
        }
        catch (error) {
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
            const { createClient } = await Promise.resolve().then(() => __importStar(require('@supabase/supabase-js')));
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
                    const userIds = allSubIds.map((s) => s.user_id);
                    // Hole ALLE Daten für diese user_ids
                    const { data: fullData } = await supabase
                        .from('mitarbeiter_organigramm')
                        .select('*')
                        .in('user_id', userIds)
                        .order('level', { ascending: true });
                    subordinates = fullData || [];
                }
            }
            else {
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
                subordinates.slice(0, 3).forEach((s) => {
                    console.log('[SOCKET] Debug:', s.full_name, '- E-Form:', s.erledigte_formulare);
                });
            }
            socket.emit('search-result', {
                found: true,
                manager: manager,
                subordinates: subordinates,
                showAll: showAll
            });
        }
        catch (error) {
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
        }
        catch (error) {
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

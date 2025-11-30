// ===== FÜGE DAS VOR "export default router;" EIN =====

// BK-Formulare MIT PDFs Route (25 Workers - DNS-stabil)
router.post('/scrape-bk-mit-pdfs', async (req: Request, res: Response) => {
  const { socketId } = req.body;
  const GPNR = '60235';
  const PASSWORD = 'r87cucd';
  const PARALLEL_WORKERS = 25;
  
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    validateStatus: () => true,
    maxRedirects: 5,
    timeout: 60000
  }));
  
  try {
    const startTime = Date.now();
    
    if (socketId && io) {
      io.to(socketId).emit('bk-pdfs-status', { message: '🔐 Login...', progress: 0 });
    }
    
    console.log('[BK-PDFS] 🚀 Starte MIT PDFs - 25 Workers');
    
    const loginResult = await performLogin(client, jar, GPNR, PASSWORD);
    if (!loginResult.success) {
      if (socketId && io) {
        io.to(socketId).emit('bk-pdfs-error', { error: loginResult.error });
      }
      return res.status(401).json({ success: false, error: loginResult.error });
    }
    
    const cookieString = loginResult.cookieString;
    
    const url = 'https://project-5.at/formularuebersicht_bk_mitarbeiter.php';
    const response = await client.get(url, {
      headers: {
        ...getBrowserHeaders(PROJECT_BASE_URL),
        'Cookie': cookieString
      }
    });
    
    if (response.status !== 200) {
      return res.status(500).json({ success: false, error: `HTTP ${response.status}` });
    }
    
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
        name: cells.eq(6).text().trim(),
        aktion: cells.eq(0).text().trim(),
        lead_id: cells.eq(2).text().trim(),
        datum: cells.eq(3).text().trim(),
        kategorie: cells.eq(4).text().trim(),
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
        strasse: '',
        geburtsdatum: '',
        pdf_datei: ''
      });
    });
    
    console.log('[BK-PDFS] 📊 BK-Formulare gefunden:', bkFormulare.length);
    
    // LIMIT 100 ZUM TESTEN
    const formulareToProcess = bkFormulare.slice(0, 100);
    console.log(`[BK-PDFS] ⚙️  TEST: ${formulareToProcess.length} Formulare`);
    
    let processedFormulare = 0;
    let totalPdfs = 0;
    let totalUploadedBytes = 0;
    let failedUploads = 0;
    
    const processFormular = async (formular: any, index: number, total: number) => {
      const workerClient = axios.create({
        withCredentials: true,
        validateStatus: () => true,
        timeout: 90000,
        maxContentLength: 100 * 1024 * 1024,
        maxBodyLength: 100 * 1024 * 1024,
        httpAgent: new (require('http').Agent)({ keepAlive: true, maxSockets: 5 }),
        httpsAgent: new (require('https').Agent)({ keepAlive: true, maxSockets: 5 })
      });
      
      try {
        const detailResponse = await workerClient.get(formular.bearbeiten_url, {
          headers: {
            ...getBrowserHeaders(url),
            'Cookie': cookieString
          }
        });
        
        if (detailResponse.status !== 200) {
          return { success: false, pdfs: 0, bytes: 0 };
        }
        
        const $detail = cheerio.load(detailResponse.data);
        
        // Extrahiere Straße + Geburtsdatum
        $detail('input').each((idx, input) => {
          const name = $detail(input).attr('name') || '';
          const value = $detail(input).val() as string || '';
          
          if (name.toLowerCase().includes('strasse') || name.toLowerCase().includes('straße')) {
            formular.strasse = value;
          }
          if (name.toLowerCase().includes('geburtsdatum') || name.toLowerCase().includes('birthday')) {
            formular.geburtsdatum = value;
          }
        });
        
        const mitarbeiterName = formular.mitarbeiter.replace(/\s/g, '_').replace(/[^a-zA-Z0-9_-äöüÄÖÜß]/g, '_');
        const kundeName = (formular.name || 'Unbekannt').replace(/\s/g, '_').replace(/[^a-zA-Z0-9_-äöüÄÖÜß]/g, '_');
        
        let uploadedBytes = 0;
        let pdfCount = 0;
        
        // CSV erstellen
        const headers = ['BK ID', 'Mitarbeiter', 'Kunde', 'Datum', 'PLZ', 'Straße', 'Geburtsdatum'];
        const values = [formular.bk_id, formular.mitarbeiter, formular.name, formular.datum, formular.plz, formular.strasse, formular.geburtsdatum]
          .map(v => `"${String(v || '').replace(/"/g, '""')}"`);
        const csvContent = '\uFEFF' + headers.join(',') + '\n' + values.join(',');
        
        const csvS3Key = `${mitarbeiterName}/${kundeName}/formular.csv`;
        
        // Upload CSV
        try {
          await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET_BK_MIT_PDFS,
            Key: csvS3Key,
            Body: csvContent,
            ContentType: 'text/csv; charset=utf-8'
          }));
          uploadedBytes += Buffer.byteLength(csvContent);
        } catch (csvErr: any) {
          console.error(`[BK-PDFS] ❌ CSV für ${kundeName}:`, csvErr.message);
          return { success: false, pdfs: 0, bytes: 0 };
        }
        
        // PDFs finden + downloaden
        const pdfs: string[] = [];
        $detail('a').each((idx, link) => {
          const href = $detail(link).attr('href') || '';
          if (href.endsWith('.pdf')) {
            pdfs.push(href.startsWith('http') ? href : `${PROJECT_BASE_URL}/${href}`);
          }
        });
        
        for (const pdfUrl of pdfs) {
          let pdfRetries = 2;
          let success = false;
          
          while (pdfRetries > 0 && !success) {
            try {
              const pdfResp = await workerClient.get(pdfUrl, {
                headers: { ...getBrowserHeaders(formular.bearbeiten_url), 'Cookie': cookieString },
                responseType: 'arraybuffer'
              });
              
              if (pdfResp.status === 200) {
                const pdfName = pdfUrl.split('/').pop() || `pdf.pdf`;
                const pdfS3Key = `${mitarbeiterName}/${kundeName}/${pdfName.replace(/[^a-zA-Z0-9._-]/g, '_')}`;
                
                await s3Client.send(new PutObjectCommand({
                  Bucket: S3_BUCKET_BK_MIT_PDFS,
                  Key: pdfS3Key,
                  Body: Buffer.from(pdfResp.data),
                  ContentType: 'application/pdf'
                }));
                
                uploadedBytes += pdfResp.data.byteLength;
                pdfCount++;
                success = true;
              }
            } catch (err: any) {
              pdfRetries--;
              if (pdfRetries > 0 && (err.code === 'ENOTFOUND' || err.code === 'ETIMEDOUT')) {
                await new Promise(r => setTimeout(r, 1000));
              }
            }
          }
        }
        
        return { success: true, pdfs: pdfCount, bytes: uploadedBytes };
      } catch (error: any) {
        return { success: false, pdfs: 0, bytes: 0 };
      }
    };
    
    // Worker-Pool
    for (let i = 0; i < formulareToProcess.length; i += PARALLEL_WORKERS) {
      const batch = formulareToProcess.slice(i, i + PARALLEL_WORKERS);
      const results = await Promise.allSettled(batch.map((f, idx) => processFormular(f, i + idx, formulareToProcess.length)));
      
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value.success) {
          processedFormulare++;
          totalPdfs += r.value.pdfs;
          totalUploadedBytes += r.value.bytes;
        } else {
          failedUploads++;
        }
      }
      
      const elapsed = (Date.now() - startTime) / 1000;
      const speed = elapsed > 0 ? processedFormulare / elapsed : 0;
      const mb = (totalUploadedBytes / (1024 * 1024)).toFixed(2);
      
      console.log(`[BK-PDFS] ⚡ ${processedFormulare}/${formulareToProcess.length} | ${speed.toFixed(2)}/s | ${mb} MB | ${totalPdfs} PDFs`);
      
      if (socketId && io) {
        io.to(socketId).emit('bk-pdfs-status', {
          message: `⚡ ${processedFormulare}/${formulareToProcess.length} | ${speed.toFixed(1)}/s`,
          progress: Math.round(10 + (processedFormulare / formulareToProcess.length) * 90)
        });
      }
    }
    
    const dur = (Date.now() - startTime) / 1000;
    const min = Math.floor(dur / 60);
    const sec = Math.floor(dur % 60);
    
    console.log('[BK-PDFS] ✅ Fertig!');
    console.log(`[BK-PDFS] ⏱️  ${min}m ${sec}s`);
    console.log(`[BK-PDFS] 📄 Erfolg: ${processedFormulare}/${formulareToProcess.length}`);
    console.log(`[BK-PDFS] ❌ Fehler: ${failedUploads}`);
    console.log(`[BK-PDFS] 📎 PDFs: ${totalPdfs}`);
    console.log(`[BK-PDFS] 💾 ${(totalUploadedBytes / (1024 * 1024)).toFixed(2)} MB`);
    
    if (socketId && io) {
      io.to(socketId).emit('bk-pdfs-complete', {
        formulare: processedFormulare,
        pdfs: totalPdfs,
        sizeMB: (totalUploadedBytes / (1024 * 1024)).toFixed(2)
      });
    }
    
    res.json({
      success: true,
      formulare: processedFormulare,
      pdfs: totalPdfs,
      sizeMB: (totalUploadedBytes / (1024 * 1024)).toFixed(2)
    });
    
  } catch (error: any) {
    console.error('[BK-PDFS] ❌', error.message);
    if (socketId && io) {
      io.to(socketId).emit('bk-pdfs-error', { error: error.message });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});













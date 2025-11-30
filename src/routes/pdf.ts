import { Router, Request, Response } from 'express';
import PDFDocument from 'pdfkit';

const router = Router();

router.post('/generate', async (req: Request, res: Response) => {
  try {
    const { name, directFormulare, unterstellteMitarbeiter, unterstellteFormulare, allFormulare, phoneData } = req.body;
    
    console.log('[PDF] Generiere PDF für:', name);
    console.log('[PDF] Direkte Formulare:', directFormulare?.length || 0);
    console.log('[PDF] Unterstellte Mitarbeiter:', unterstellteMitarbeiter?.length || 0);
    console.log('[PDF] Unterstellte Formulare:', unterstellteFormulare?.length || 0);
    console.log('[PDF] Alle Formulare:', allFormulare?.length || 0);
    console.log('[PDF] Telefonnummern:', phoneData?.length || 0);
    console.log('[PDF] Request Body Keys:', Object.keys(req.body));
    
    // Erstelle Map für schnellen Telefonnummern-Lookup
    const phoneMap = new Map();
    if (phoneData && Array.isArray(phoneData)) {
      phoneData.forEach((item: any) => {
        if (item.name && item.phone) {
          phoneMap.set(item.name.toLowerCase(), item.phone);
        }
      });
    }
    
    if (!name) {
      return res.status(400).json({
        success: false,
        error: 'Mitarbeiter-Name fehlt'
      });
    }
    
    // PDF-Dokument erstellen
    const doc = new PDFDocument({ margin: 50 });
    
    // Response-Header setzen
    const safeName = name.replace(/[^a-zA-Z0-9äöüÄÖÜß\s]/g, '_').replace(/\s/g, '_');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Mitarbeiter_${safeName}.pdf"`);
    
    // PDF in Response schreiben
    doc.pipe(res);
    
    // Titel
    doc.fontSize(20)
       .font('Helvetica-Bold')
       .text('Mitarbeiter-Bericht', { align: 'center' });
    
    doc.moveDown();
    
    // Mitarbeiter-Name
    doc.fontSize(16)
       .font('Helvetica-Bold')
       .text(`Mitarbeiter: ${name}`);
    
    doc.moveDown();
    
    // Zusammenfassung
    doc.fontSize(14)
       .font('Helvetica-Bold')
       .text('Zusammenfassung:');
    
    const directCount = directFormulare?.length || 0;
    const unterstellteCount = unterstellteMitarbeiter?.length || 0;
    const unterstellteFormsCount = unterstellteFormulare?.length || 0;
    const totalCount = allFormulare?.length || 0;
    
    doc.fontSize(12)
       .font('Helvetica')
       .text(`Direkte Formulare: ${directCount}`)
       .text(`Unterstellte Mitarbeiter: ${unterstellteCount}`)
       .text(`Formulare der unterstellten Mitarbeiter: ${unterstellteFormsCount}`)
       .text(`Gesamte Formulare: ${totalCount}`);
    
    doc.moveDown();
    
    // Unterstellte Mitarbeiter
    if (unterstellteMitarbeiter && unterstellteMitarbeiter.length > 0) {
      doc.fontSize(14)
         .font('Helvetica-Bold')
         .text('Unterstellte Mitarbeiter:');
      
      doc.fontSize(10)
         .font('Helvetica');
      
      unterstellteMitarbeiter.forEach((ma: any, index: number) => {
        const name = `${ma.vorname || ''} ${ma.name || ''}`.trim() || 'Unbekannt';
        const rolle = [];
        if (ma.isFuehrungskraft) rolle.push('Führungskraft');
        if (ma.isRegistrator) rolle.push('Registrator');
        
        doc.text(`${index + 1}. ${name} (${rolle.join(', ')})`);
      });
      
      doc.moveDown();
    }
    
    // Alle Formulare (direkt + von unterstellten Mitarbeitern)
    if (allFormulare && allFormulare.length > 0) {
      doc.addPage();
      doc.fontSize(14)
         .font('Helvetica-Bold')
         .text(`Alle Formulare (${allFormulare.length} Einträge):`);
      
      doc.moveDown(0.5);
      
      doc.fontSize(8)
         .font('Helvetica');
      
      console.log('[PDF] Schreibe', allFormulare.length, 'Formulare in PDF');
      
      let currentPage = 1;
      let linesOnPage = 0;
      const maxLinesPerPage = 40;
      
      for (let index = 0; index < allFormulare.length; index++) {
        const form = allFormulare[index];
        
        // Neue Seite wenn nötig
        if (linesOnPage >= maxLinesPerPage) {
          doc.addPage();
          linesOnPage = 0;
          currentPage++;
          
          // Header auf neuer Seite
          doc.fontSize(10)
             .font('Helvetica-Bold')
             .fillColor('black')
             .text(`Fortsetzung Seite ${currentPage}`, { align: 'center' });
          doc.fontSize(8)
             .font('Helvetica');
          doc.moveDown(0.5);
        }
        
        // Prüfe ob Kunde eine Telefonnummer in Leads hat
        const kundenName = (form.name || '').toLowerCase().trim();
        
        // Exaktes Match
        let telefonnummer = phoneMap.get(kundenName);
        
        // Fuzzy Match wenn kein exaktes Match
        if (!telefonnummer) {
          for (const [leadName, phone] of phoneMap.entries()) {
            if (kundenName.includes(leadName) || leadName.includes(kundenName)) {
              telefonnummer = phone;
              console.log(`[PDF] Fuzzy Match: "${kundenName}" matched mit "${leadName}"`);
              break;
            }
          }
        }
        
        const hasPhone = !!telefonnummer;
        
        // Normale Zeile
        doc.fillColor('black')
           .font('Helvetica');
        
        const baseLine = `${index + 1}. ID: ${form.id || '-'} | `;
        doc.text(baseLine, { continued: true });
        
        // Mitarbeiter (ROT wenn Telefonnummer vorhanden)
        if (hasPhone) {
          doc.fillColor('red').font('Helvetica-Bold');
        }
        doc.text(`MA: ${form.mitarbeiter || '-'}`, { continued: true });
        doc.fillColor('black').font('Helvetica');
        
        doc.text(` | Datum: ${form.datum || '-'} | Kunde: ${form.name || '-'} | PLZ: ${form.plz || '-'} | KdNr: ${form.kdnr || '-'} | Kat: ${form.kategorie || '-'} | Anbieter: ${form.anbieter || '-'}`, {
          lineBreak: true
        });
        
        // Telefonnummer in ROT FETT, falls vorhanden
        if (hasPhone) {
          doc.fillColor('red')
             .font('Helvetica-Bold')
             .text(`   TEL: ${telefonnummer}`, { indent: 20 });
          doc.fillColor('black')
             .font('Helvetica');
          linesOnPage++;
        }
        
        linesOnPage++;
        
        // Fortschritt loggen bei großen Mengen
        if ((index + 1) % 1000 === 0) {
          console.log(`[PDF] Fortschritt: ${index + 1}/${allFormulare.length} Formulare geschrieben`);
        }
      }
      
      console.log('[PDF] Alle', allFormulare.length, 'Formulare erfolgreich geschrieben');
    } else {
      console.log('[PDF] WARNUNG: Keine Formulare zum Schreiben vorhanden!');
      console.log('[PDF] allFormulare ist:', allFormulare ? `Array mit ${allFormulare.length} Elementen` : 'undefined/null');
      
      // Zeige Warnung in PDF
      doc.addPage();
      doc.fontSize(14)
         .font('Helvetica-Bold')
         .fillColor('red')
         .text('WARNUNG: Keine Formulare gefunden!');
    }
    
    // PDF finalisieren
    doc.end();
    
    console.log('[PDF] PDF erfolgreich generiert');
    
  } catch (error: any) {
    console.error('[PDF] Fehler:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

export default router;


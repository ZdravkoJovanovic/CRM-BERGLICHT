-- Tabelle für Mitarbeiter Leads (von leads_mitarbeiter.php)
CREATE TABLE public.mitarbeiter_leads (
  id BIGSERIAL NOT NULL,
  lead_id BIGINT NULL, -- Original Lead-ID von der Website
  mitarbeiter_id VARCHAR(50) NULL, -- Mitarbeiter VC24/GPNR
  mitarbeiter_name VARCHAR(200) NULL, -- Mitarbeiter Vor- und Nachname
  
  -- Lead Basis-Informationen
  datum DATE NULL,
  kampagne VARCHAR(200) NULL,
  kunde_name VARCHAR(200) NULL,
  kunde_telefon VARCHAR(50) NULL,
  bundesland VARCHAR(10) NULL,
  plz VARCHAR(10) NULL,
  erledigt BOOLEAN NULL DEFAULT FALSE,
  
  -- Termine (1. AV, 2. AV, 3. AV)
  termin_1_av DATE NULL,
  termin_2_av DATE NULL,
  termin_3_av DATE NULL,
  
  -- Lead Status & Fortschritt
  lead_erreicht BOOLEAN NULL DEFAULT FALSE,
  termin_vereinbart DATE NULL,
  termin_durchgefuehrt DATE NULL,
  formular_unterschrieben BOOLEAN NULL DEFAULT FALSE,
  
  -- Geld & Formulare
  geld_formular INTEGER NULL DEFAULT 0,
  qc_durchgefuehrt DATE NULL,
  bg_vereinbart DATE NULL,
  bg_durchgefuehrt INTEGER NULL DEFAULT 0,
  
  -- Kunde & Zahlung
  ist_kunde BOOLEAN NULL DEFAULT FALSE,
  einzahlung DATE NULL,
  bws VARCHAR(200) NULL,
  
  -- Summen (aus Header der Tabelle)
  summe INTEGER NULL DEFAULT 0,
  
  -- Metadaten
  created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
  last_scraped_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
  
  CONSTRAINT mitarbeiter_leads_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Eindeutiger Index: Lead-ID pro Mitarbeiter sollte nur 1x vorkommen
CREATE UNIQUE INDEX IF NOT EXISTS idx_mitarbeiter_leads_unique 
  ON public.mitarbeiter_leads USING btree (lead_id, mitarbeiter_id) 
  TABLESPACE pg_default;

-- Indexes für schnelle Abfragen
CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_mitarbeiter_id 
  ON public.mitarbeiter_leads USING btree (mitarbeiter_id) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_datum 
  ON public.mitarbeiter_leads USING btree (datum) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_kampagne 
  ON public.mitarbeiter_leads USING btree (kampagne) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_kunde_name 
  ON public.mitarbeiter_leads USING btree (kunde_name) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_bundesland 
  ON public.mitarbeiter_leads USING btree (bundesland) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_plz 
  ON public.mitarbeiter_leads USING btree (plz) 
  TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mitarbeiter_leads_ist_kunde 
  ON public.mitarbeiter_leads USING btree (ist_kunde) 
  TABLESPACE pg_default;

-- Trigger für updated_at
CREATE OR REPLACE FUNCTION update_mitarbeiter_leads_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_mitarbeiter_leads_timestamp
  BEFORE UPDATE ON public.mitarbeiter_leads
  FOR EACH ROW
  EXECUTE FUNCTION update_mitarbeiter_leads_timestamp();

-- View: Aggregiere Leads pro Mitarbeiter
CREATE OR REPLACE VIEW v_mitarbeiter_leads_stats AS
SELECT 
  mitarbeiter_id,
  mitarbeiter_name,
  COUNT(*) as total_leads,
  COUNT(*) FILTER (WHERE erledigt = true) as leads_erledigt,
  COUNT(*) FILTER (WHERE lead_erreicht = true) as leads_erreicht,
  COUNT(*) FILTER (WHERE termin_vereinbart IS NOT NULL) as termine_vereinbart,
  COUNT(*) FILTER (WHERE termin_durchgefuehrt IS NOT NULL) as termine_durchgefuehrt,
  COUNT(*) FILTER (WHERE formular_unterschrieben = true) as formulare_unterschrieben,
  COUNT(*) FILTER (WHERE ist_kunde = true) as kunden,
  SUM(geld_formular) as total_geld_formulare,
  SUM(bg_durchgefuehrt) as total_bg_durchgefuehrt,
  MAX(last_scraped_at) as last_update
FROM public.mitarbeiter_leads
GROUP BY mitarbeiter_id, mitarbeiter_name
ORDER BY total_leads DESC;


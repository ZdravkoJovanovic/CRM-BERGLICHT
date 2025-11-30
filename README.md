# CRM-Strom

Eine Node.js-Anwendung mit Express, TypeScript und Tailwind CSS für Project 5 Login.

## Features

- Node.js Backend mit Express
- TypeScript für Typsicherheit
- Tailwind CSS für modernes Styling
- RESTful API für Project 5 Login
- Automatischer Login & Logout mit Cookie-Verwaltung

## Installation

```bash
npm install
```

## Entwicklung

```bash
npm run dev
```

Dieser Befehl:
- Kompiliert TypeScript im Watch-Modus
- Generiert Tailwind CSS im Watch-Modus
- Startet den Server mit Nodemon (automatischer Neustart bei Änderungen)

## Produktion

```bash
npm start
```

## Verwendung

1. Starten Sie die Anwendung mit `npm run dev` oder `npm start`
2. Öffnen Sie Ihren Browser unter `http://localhost:3000`
3. Geben Sie Ihre GPNR und Ihr Passwort ein
4. Klicken Sie auf "Login & Logout"
5. Das System führt automatisch Login und Logout durch
6. Der erhaltene Cookie wird unter dem Button angezeigt

## API-Endpunkte

### POST `/api/auth`

Führt Login und Logout für Project 5 durch.

**Request Body:**
```json
{
  "gpnr": "IHRE_GPNR",
  "password": "IHR_PASSWORT"
}
```

**Response:**
```json
{
  "success": true,
  "cookie": "PHPSESSID=abc123...",
  "message": "Login und Logout erfolgreich"
}
```

## Projektstruktur

```
CRM-Strom/
├── src/
│   ├── server.ts           # Backend-Server
│   ├── routes/
│   │   └── auth.ts         # Auth API-Route
│   └── input.css           # Tailwind CSS Input
├── public/
│   ├── index.html          # Frontend
│   └── styles.css          # Generierte Tailwind CSS
├── dist/                   # Kompilierte TypeScript-Dateien
├── package.json
├── tsconfig.json
└── tailwind.config.js
```

## Sicherheitshinweise

- Die TLS-Zertifikatsprüfung ist für Entwicklungszwecke deaktiviert
- Credentials werden nicht gespeichert
- Cookies werden nach jedem Request automatisch gelöscht


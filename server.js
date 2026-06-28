// ============================================================
//  Gozem Queue — Serveur Node.js
//  Démarrer : node server.js
//  Accès agents : http://<IP_DU_PC>:3000
// ============================================================

const http = require('http');
const fs   = require('fs');
const path = require('path');

const PORT     = 3000;
const HTML_FILE = path.join(__dirname, 'gozem_queue.html');

// ---- État partagé (en mémoire) ----
let sharedState = {
  queue      : [],
  serving    : [],
  done       : [],
  financing  : [],
  ticketCounter: 0,
  agentStatus: {},   // nom -> 'disponible' | 'pause'
  agentTime  : {},   // nom -> { disponible, pause, treating, lastChange, loginTime }
};

// ---- Clients SSE connectés ----
const clients = new Set();

function broadcast(event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  clients.forEach(res => {
    try { res.write(msg); } catch(e) { clients.delete(res); }
  });
}

// ---- Serveur HTTP ----
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  const method = req.method;

  // CORS pour dev local
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── GET / → HTML ──────────────────────────────────────────
  if (method === 'GET' && url === '/') {
    fs.readFile(HTML_FILE, (err, data) => {
      if (err) { res.writeHead(500); res.end('Fichier introuvable'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // ── GET /state → état complet ──────────────────────────────
  if (method === 'GET' && url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(sharedState));
    return;
  }

  // ── GET /events → SSE (temps réel) ────────────────────────
  if (method === 'GET' && url === '/events') {
    res.writeHead(200, {
      'Content-Type'  : 'text/event-stream',
      'Cache-Control' : 'no-cache',
      'Connection'    : 'keep-alive',
    });
    res.write(`event: state\ndata: ${JSON.stringify(sharedState)}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  // ── POST /action → actions métier ─────────────────────────
  if (method === 'POST' && url === '/action') {
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      let action;
      try { action = JSON.parse(body); } catch(e) {
        res.writeHead(400); res.end('JSON invalide'); return;
      }
      handleAction(action);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  res.writeHead(404); res.end('Non trouvé');
});

// ---- Logique métier ----
function handleAction(action) {
  const now = Date.now();
  switch (action.type) {

    case 'REGISTER': {
      // Enregistrer un conducteur
      sharedState.ticketCounter++;
      const num = 'A-' + String(sharedState.ticketCounter).padStart(3, '0');
      const ticket = {
        id          : now + Math.random(),
        num,
        prenom      : action.prenom,
        nom         : action.nom,
        fullName    : action.prenom + ' ' + action.nom,
        type        : action.driverType,  // standard | vplus | client
        motif       : action.motif,
        arrivalTime : now,
        status      : action.motif === 'Financing' ? 'financing' : 'waiting',
        agent       : null,
        startServeTime: null,
        endTime     : null,
        note        : '',
      };
      if (action.motif === 'Financing') {
        sharedState.financing.push(ticket);
      } else {
        sharedState.queue.push(ticket);
        sortQueue();
      }
      broadcast('state', sharedState);
      break;
    }

    case 'CALL_NEXT': {
      // Agent appelle un conducteur
      const agentName = action.agentName;
      let idx = action.ticketId
        ? sharedState.queue.findIndex(t => t.id === action.ticketId)
        : sharedState.queue.findIndex(t => canHandle(agentName, t.motif));
      if (idx === -1) break;
      const ticket = sharedState.queue.splice(idx, 1)[0];
      ticket.status = 'serving';
      ticket.agent  = agentName;
      ticket.startServeTime = now;
      sharedState.serving.push(ticket);
      broadcast('state', sharedState);
      break;
    }

    case 'CLOSE': {
      // Clôturer un cas
      const idx = sharedState.serving.findIndex(t => t.id === action.ticketId);
      if (idx === -1) break;
      const ticket = sharedState.serving.splice(idx, 1)[0];
      ticket.status  = 'done';
      ticket.endTime = now;
      ticket.note    = action.note || '';
      sharedState.done.unshift(ticket);
      broadcast('state', sharedState);
      break;
    }

    case 'AGENT_LOGIN': {
      const n = action.agentName;
      sharedState.agentStatus[n] = 'disponible';
      sharedState.agentTime[n]   = { disponible: 0, pause: 0, lastChange: now, loginTime: now };
      broadcast('state', sharedState);
      break;
    }

    case 'TOGGLE_STATUS': {
      const n    = action.agentName;
      const prev = sharedState.agentStatus[n] || 'disponible';
      const t    = sharedState.agentTime[n];
      if (t && t.lastChange) {
        t[prev] += now - t.lastChange;
        t.lastChange = now;
      }
      sharedState.agentStatus[n] = prev === 'disponible' ? 'pause' : 'disponible';
      broadcast('state', sharedState);
      break;
    }

    default:
      console.warn('Action inconnue :', action.type);
  }
}

function sortQueue() {
  sharedState.queue.sort((a, b) => {
    const pri = t => (t.type === 'vplus' || t.type === 'client') ? 0 : 1;
    if (pri(a) !== pri(b)) return pri(a) - pri(b);
    return a.arrivalTime - b.arrivalTime;
  });
}

const AGENT_SCOPE = {
  'Jean Marie' : ['Demande d\'infos','Réclamation','Problème App','Convocation disciplinaire','Formation','Fleet info','Sensibilisation','Droit taxi'],
  'Rosette'    : ['Demande d\'infos','Réclamation','Problème App','Convocation disciplinaire','Formation','Sensibilisation'],
  'Lidvine'    : ['Demande d\'infos','Réclamation','Problème App','Convocation disciplinaire','Formation','Sensibilisation'],
  'Jacqueline' : ['Demande d\'infos','Réclamation','Problème App','Convocation disciplinaire','Formation','Sensibilisation'],
  'Eric'       : ['Demande d\'infos','Réclamation','Problème App','Convocation disciplinaire','Formation','Sensibilisation'],
  'Mathias'    : ['Droit taxi'],
  'Financing 1': ['Financing'],
  'Financing 2': ['Financing'],
};

function canHandle(agentName, motif) {
  return (AGENT_SCOPE[agentName] || []).includes(motif);
}

// ---- Démarrage ────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   Gozem Queue — Serveur démarré ✅       ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║   Local  : http://localhost:${PORT}          ║`);
  console.log('║   Réseau : http://<VOTRE_IP>:' + PORT + '       ║');
  console.log('║                                          ║');
  console.log('║   Pour trouver votre IP :                ║');
  console.log('║   → ouvrir cmd → taper : ipconfig        ║');
  console.log('║   → chercher "Adresse IPv4"              ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log('');
});

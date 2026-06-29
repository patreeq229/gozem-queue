// ============================================================
//  Gozem Queue — Serveur Node.js v2
//  Démarrer : node server.js
// ============================================================

const http = require('http');
const fs   = require('fs');
const path = require('path');

const PORT      = process.env.PORT || 3000;
const HTML_FILE = path.join(__dirname, 'gozem_queue.html');

// ---- État partagé ----
let sharedState = {
  queue: [], serving: [], done: [], financing: [],
  ticketCounter: 0, agentStatus: {}, agentTime: {},
};

// ---- Clients SSE ----
let clients = [];

function broadcast() {
  const msg = 'data: ' + JSON.stringify(sharedState) + '\n\n';
  clients = clients.filter(res => {
    try { res.write(msg); return true; }
    catch(e) { return false; }
  });
}

// ---- Serveur ----
const server = http.createServer((req, res) => {
  const url    = req.url.split('?')[0];
  const method = req.method;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // HTML principal
  if (method === 'GET' && url === '/') {
    fs.readFile(HTML_FILE, (err, data) => {
      if (err) { res.writeHead(500); res.end('Fichier introuvable'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // SSE — temps réel
  if (method === 'GET' && url === '/events') {
    res.writeHead(200, {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write('data: ' + JSON.stringify(sharedState) + '\n\n');
    clients.push(res);
    req.on('close', () => {
      clients = clients.filter(c => c !== res);
    });
    return;
  }

  // Etat complet (polling fallback)
  if (method === 'GET' && url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(sharedState));
    return;
  }

  // Actions
  if (method === 'POST' && url === '/action') {
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const action = JSON.parse(body);
        handleAction(action);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400); res.end('JSON invalide');
      }
    });
    return;
  }

  res.writeHead(404); res.end('Non trouvé');
});

// ---- Actions métier ----
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

function canHandle(agent, motif) {
  return (AGENT_SCOPE[agent] || []).includes(motif);
}

function sortQueue() {
  sharedState.queue.sort((a, b) => {
    const p = t => (t.type === 'vplus' || t.type === 'client') ? 0 : 1;
    if (p(a) !== p(b)) return p(a) - p(b);
    return a.arrivalTime - b.arrivalTime;
  });
}

function handleAction(action) {
  const now = Date.now();
  switch (action.type) {

    case 'REGISTER': {
      sharedState.ticketCounter++;
      const num = 'A-' + String(sharedState.ticketCounter).padStart(3, '0');
      const ticket = {
        id: now + '_' + Math.random().toString(36).slice(2),
        num, prenom: action.prenom, nom: action.nom,
        fullName: action.prenom + ' ' + action.nom,
        type: action.driverType, motif: action.motif,
        arrivalTime: now, status: action.motif === 'Financing' ? 'financing' : 'waiting',
        agent: null, startServeTime: null, endTime: null, note: '',
      };
      if (action.motif === 'Financing') sharedState.financing.push(ticket);
      else { sharedState.queue.push(ticket); sortQueue(); }
      broadcast();
      break;
    }

    case 'CALL_NEXT': {
      const idx = action.ticketId
        ? sharedState.queue.findIndex(t => t.id === action.ticketId)
        : sharedState.queue.findIndex(t => canHandle(action.agentName, t.motif));
      if (idx === -1) break;
      const ticket = sharedState.queue.splice(idx, 1)[0];
      ticket.status = 'serving'; ticket.agent = action.agentName;
      ticket.startServeTime = now;
      sharedState.serving.push(ticket);
      broadcast();
      break;
    }

    case 'CLOSE': {
      const idx = sharedState.serving.findIndex(t => t.id === action.ticketId);
      if (idx === -1) break;
      const ticket = sharedState.serving.splice(idx, 1)[0];
      ticket.status = 'done'; ticket.endTime = now; ticket.note = action.note || '';
      sharedState.done.unshift(ticket);
      broadcast();
      break;
    }

    case 'AGENT_LOGIN': {
      const n = action.agentName;
      sharedState.agentStatus[n] = 'disponible';
      sharedState.agentTime[n]   = { disponible: 0, pause: 0, lastChange: now, loginTime: now };
      broadcast();
      break;
    }

    case 'TOGGLE_STATUS': {
      const n    = action.agentName;
      const prev = sharedState.agentStatus[n] || 'disponible';
      const t    = sharedState.agentTime[n] || { disponible: 0, pause: 0, lastChange: now, loginTime: now };
      if (t.lastChange) t[prev] += now - t.lastChange;
      t.lastChange = now;
      sharedState.agentStatus[n] = prev === 'disponible' ? 'pause' : 'disponible';
      sharedState.agentTime[n]   = t;
      broadcast();
      break;
    }
  }
}

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   Gozem Queue — Serveur démarré v2 ✅    ║');
  console.log(`║   Port : ${PORT}                              ║`);
  console.log('╚══════════════════════════════════════════╝');
});

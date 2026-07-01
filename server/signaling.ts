import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";

/**
 * Servidor de señalización WebRTC.
 * Empareja dispositivos por "room" (código de emparejamiento) y reenvía
 * mensajes (offer/answer/candidate/location) entre los dos peers de la misma sala.
 *
 * Protocolo de mensajes (JSON):
 *  - { type: "join", room: string, role: "server"|"client" }
 *  - { type: "offer"|"answer", sdp: string }
 *  - { type: "candidate", candidate: string, sdpMid: string, sdpMLineIndex: number }
 *  - { type: "location", lat, lng, accuracy, speed, bearing, time }
 *
 * Mensajes generados por el servidor hacia los clientes:
 *  - { type: "joined", role }
 *  - { type: "peer-joined" } / { type: "peer-left" }
 *  - { type: "full" }  (la sala ya tiene 2 ocupantes)
 */

interface Peer {
  socket: WebSocket;
  role: string;
  room: string;
  alive: boolean;
}

interface Room {
  peers: Set<Peer>;
}

const rooms = new Map<string, Room>();

function getRoom(name: string): Room {
  let r = rooms.get(name);
  if (!r) {
    r = { peers: new Set() };
    rooms.set(name, r);
  }
  return r;
}

function otherPeers(room: Room, self: Peer): Peer[] {
  return Array.from(room.peers).filter((p) => p !== self);
}

function send(peer: Peer, obj: unknown) {
  if (peer.socket.readyState === WebSocket.OPEN) {
    try {
      peer.socket.send(JSON.stringify(obj));
    } catch {
      /* ignore */
    }
  }
}

export function registerSignaling(server: Server) {
  // noServer + manejo manual de upgrade para no chocar con Vite HMR ni otras rutas WS
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req, socket, head) => {
    let pathname = "";
    try {
      pathname = new URL(req.url || "", "http://localhost").pathname;
    } catch {
      pathname = req.url || "";
    }
    if (pathname === "/api/signal") {
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit("connection", ws, req);
      });
    }
    // Si no es nuestra ruta, no tocamos el socket: lo maneja Vite u otro handler.
  });

  wss.on("connection", (ws: WebSocket) => {
    let peer: Peer | null = null;

    ws.on("message", (data) => {
      let msg: any;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }

      if (msg.type === "join") {
        const roomName = String(msg.room || "").trim();
        if (!roomName) return;
        const room = getRoom(roomName);

        // Limite de 2 ocupantes por sala
        if (room.peers.size >= 2) {
          send({ socket: ws, role: "", room: roomName, alive: true }, { type: "full" });
          try { ws.close(); } catch { /* ignore */ }
          return;
        }

        peer = { socket: ws, role: String(msg.role || "client"), room: roomName, alive: true };
        room.peers.add(peer);
        send(peer, { type: "joined", role: peer.role });

        // Avisar a ambos que ya hay pareja
        const others = otherPeers(room, peer);
        if (others.length > 0) {
          send(peer, { type: "peer-joined" });
          others.forEach((o) => send(o, { type: "peer-joined" }));
        }
        return;
      }

      // Mensajes que se reenvian al otro peer de la sala
      if (peer) {
        const room = rooms.get(peer.room);
        if (!room) return;
        otherPeers(room, peer).forEach((o) => send(o, msg));
      }
    });

    ws.on("pong", () => {
      if (peer) peer.alive = true;
    });

    ws.on("close", () => {
      if (!peer) return;
      const room = rooms.get(peer.room);
      if (room) {
        room.peers.delete(peer);
        otherPeers(room, peer).forEach((o) => send(o, { type: "peer-left" }));
        if (room.peers.size === 0) rooms.delete(peer.room);
      }
      peer = null;
    });

    ws.on("error", () => {
      /* el evento close se encargara de la limpieza */
    });
  });

  // Ping periodico para detectar conexiones muertas y mantenerlas vivas
  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      try {
        ws.ping();
      } catch {
        /* ignore */
      }
    });
  }, 25000);

  wss.on("close", () => clearInterval(interval));

  console.log("[signaling] WebSocket de señalización montado en /api/signal");
}

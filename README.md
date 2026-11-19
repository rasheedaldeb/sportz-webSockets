# Sportz WebSockets

An Express + Postgres demo for **live match commentary** with a WebSocket broadcast channel. The focus is on practical WebSocket patterns rather than on sports features: heartbeats, backpressure, message validation, and subscriptions.

## Features

- **REST API (Express)** for creating and reading matches and commentary
- **WebSocket broadcast channel** that pushes new commentary to connected clients in real time
- **Subscriptions** so clients receive updates only for the matches they care about
- **Heartbeats (ping/pong)** to detect and clean up dead connections
- **Backpressure handling** so slow clients don't pile up unbounded buffered messages
- **Message validation** for all incoming WebSocket and HTTP payloads
- **Postgres + Drizzle ORM** for persistence and migrations
- **APM monitoring** via APM Insight (`apminsightnode.json`)

## Tech Stack

| Layer         | Technology              |
| ------------- | ----------------------- |
| Runtime       | Node.js                 |
| HTTP server   | Express                 |
| Realtime      | WebSockets              |
| Database      | PostgreSQL              |
| ORM/migrations| Drizzle ORM / drizzle-kit |
| Monitoring    | APM Insight             |

## Project Structure

```
.
├── drizzle/               # Generated SQL migrations
├── src/                   # Application source (server, routes, WebSocket logic, DB)
├── drizzle.config.js      # Drizzle Kit configuration
├── apminsightnode.json    # APM Insight configuration
├── package.json
└── package-lock.json
```

## Getting Started

### Prerequisites

- Node.js 18+
- A running PostgreSQL instance (local or hosted)

### Installation

```bash
git clone https://github.com/rasheedaldeb/sportz-webSockets.git
cd sportz-webSockets
npm install
```

### Environment Variables

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/sportz
PORT=8000
```

> Adjust the names and values to match what the code in `src/` reads.

### Database Setup

Generate and apply migrations with Drizzle Kit:

```bash
npx drizzle-kit generate
npx drizzle-kit migrate
```

### Run the Server

```bash
npm run dev     # development
npm start       # production
```

Check `package.json` for the exact script names.

## WebSocket Patterns

### Subscriptions

Clients connect to the WebSocket endpoint and tell the server which matches they want to follow. The server then broadcasts commentary only to clients subscribed to that match.

Example client messages (illustrative; match them to your actual schema):

```json
{ "type": "subscribe", "matchId": 1 }
{ "type": "unsubscribe", "matchId": 1 }
```

### Heartbeats

The server periodically pings every connection. Clients that fail to respond with a pong are considered dead and are terminated, which prevents leaked connections and stale subscriptions.

### Backpressure

Before sending, the server checks each socket's buffered amount. Slow consumers are skipped or dropped instead of letting memory grow without limit while they fall behind.

### Message Validation

Every incoming message is parsed and validated before it is handled. Malformed or unexpected payloads are rejected with an error response rather than crashing the handler.

## Example Usage

Connect with any WebSocket client:

```js
const ws = new WebSocket("ws://localhost:8000/ws");

ws.onopen = () => {
  ws.send(JSON.stringify({ type: "subscribe", matchId: 1 }));
};

ws.onmessage = (event) => {
  console.log("Update:", JSON.parse(event.data));
};
```

Or from the command line:

```bash
npx wscat -c ws://localhost:8000/ws
```

## Scripts

| Script        | Description                     |
| ------------- | ------------------------------- |
| `npm start`   | Start the server                |
| `npm run dev` | Start with auto-reload (if set) |

## Contributing

Issues and pull requests are welcome. For larger changes, please open an issue first to discuss what you'd like to change.

## License

Add a license of your choice (e.g. MIT) and reference it here.

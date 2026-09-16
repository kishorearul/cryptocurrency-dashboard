# cryptocurrency dashboard

![Java](https://img.shields.io/badge/Java-11%2B-orange?style=flat-square&logo=openjdk)
![JavaScript](https://img.shields.io/badge/JavaScript-ES2020-yellow?style=flat-square&logo=javascript)
![HTML5](https://img.shields.io/badge/HTML5-markup-E34F26?style=flat-square&logo=html5)
![CSS3](https://img.shields.io/badge/CSS3-terminal-1572B6?style=flat-square&logo=css3)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

A real-time, high-density institutional **Bloomberg / TradingView style** financial terminal for crypto markets. Streams live Binance prices over WebSockets with instant flash animations, smoothed sparkline charts, a simulated market-depth order book, and a terminal command-line interface — all in a dark institutional theme.

## Key Features

- **Live WebSocket streaming** — Java backend ingests Binance `!ticker@arr` (JDK `HttpClient` WebSocket), falls back to Binance REST polling every 2s, then to local simulation; rebroadcasts to browsers over a JDK-only RFC6455 WebSocket server (`ws://<host>:8081`).
- **Instant price flash animations** — green/red cell flashes (`flash-up` / `flash-down`) on every real-time tick.
- **Dynamic sparkline charts** — per-row HTML5 canvas trends with anti-aliased Catmull-Rom → Bézier smoothing, gradient fill, glow, and end-point dot.
- **Market depth / order book simulation** — side panels with bid/ask ladders and depth bars scaled proportionally to volume, spread readout, click-to-track asset selection.
- **Terminal command input** — `COMMAND >` prompt replaces search boxes (`HELP`, `FILTER`, `SORT`, `SELECT`, `PAUSE`, `SNAPSHOT`, …).
- **Institutional dark UI** — `#0b0e14` background, `#1e222d` borders, `#0ecb81` / `#f6465d` semantics, CoinGecko icons, seamless ticker tape (`BTC/USDT` format), UTC clock, connection/latency badges.

## Architecture & Tech Stack

```
Browser (HTML/CSS/JS)  <--WS :8081 / HTTP :8080-->  Java backend  <-->  Binance
```

- **Backend (`backend/`, Java 11, Maven, Jackson):** `com.sun.net.httpserver.HttpServer` serves the frontend + `GET /api/crypto` snapshot + `GET /api/health`; a hand-rolled RFC6455 `WsBroadcaster` on port **8081** pushes a ~1 msg/s JSON array (`BTC, ETH, SOL, ADA, MATIC, XRP` with `current_price`, `price_change_percentage_24h`, `high_24h`, `low_24h`, `market_cap`, `history[40]`). Ingest order: Binance WebSocket → Binance REST (2s) → in-process random-walk simulation. Single dependency: `jackson-databind`.
- **Frontend (`frontend/`, no framework):** vanilla JS WebSocket client with REST fallback polling, in-place DOM updates via cached row refs, canvas sparklines, ticker-tape marquee, depth/book renderer, command parser.

## Getting Started / How to Run

### 1. Prerequisites

- **JDK 11+** (`java -version`) and **Maven 3.6+** (`mvn -version`)
- A modern **web browser** (Chrome / Edge / Firefox)
- Optional for static-only hosting: **Python 3** (`python3 --version`) or **Node.js** (`node --version`)

### 2. Compile and launch the backend

From the repo root:

```bash
cd backend
mvn package -DskipTests
java -jar target/dashboard-1.0-SNAPSHOT.jar
```

You should see:

```
WS broadcaster on port 8081
Serving frontend from: /path/to/frontend
HTTP  http://localhost:8080
WS    ws://localhost:8081
API   http://localhost:8080/api/crypto
Binance WS connected
```

Alternative (run without packaging):

```bash
cd backend
mvn compile exec:java
```

> The canonical server source is `backend/src/main/java/Main.java`
> (mirrored at `backend/src/Main.java`).

### 3. Open the dashboard

The Java server already serves the UI — just open:

- App: <http://localhost:8080>
- Snapshot API: <http://localhost:8080/api/crypto>
- Health: <http://localhost:8080/api/health>

To serve `frontend/` statically instead (e.g. for UI-only work — the app still needs the Java backend on `:8080/:8081` for data):

```bash
# Python
cd frontend && python3 -m http.server 5173
# then open http://localhost:5173 (API/WS still come from :8080/:8081)

# or Node
npx serve frontend -l 5173
```

## Project Structure

```text
.
├── README.md
├── LICENSE                  # MIT
├── .gitignore
├── backend/
│   ├── pom.xml              # Java 11, jackson-databind, shade/exec plugins
│   └── src/
│       ├── main/java/Main.java   # HTTP :8080 + WS :8081 + Binance ingest
│       └── Main.java             # spec mirror of the above
└── frontend/
    ├── index.html           # CRYPTO DASHBOARD markup, tape, grid, COMMAND prompt
    ├── styles.css           # institutional theme, flash + tape animations
    └── app.js               # WS client, flash, smoothed sparklines, commands
```

Generated `backend/target/` (jars, `*.class`) is git-ignored.

## Usage / Commands

Click a table row (or `SELECT`) to track an asset in the depth/book panels. Click column headers to sort. Type in the `COMMAND >` box:

| Command | Effect |
|---|---|
| `HELP` | List all commands |
| `FILTER <text\|clear>` | Filter by name / symbol / pair (`FILTER btc`, `FILTER clear`) |
| `SORT <price\|change\|mcap\|name>` | Re-sort the quotes table |
| `SELECT <SYM>` | Track asset in depth/book (`SELECT ETH`) |
| `PAUSE` / `RESUME` | Freeze / resume the live stream |
| `SNAPSHOT` | Print one-line price snapshot to the console |
| `PING` | Show WS state + message count |
| `CLEAR` | Clear the command console |

## License

MIT — see [LICENSE](LICENSE).

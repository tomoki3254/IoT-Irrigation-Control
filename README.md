# IoT Irrigation Control System

A smart irrigation system for rural and mountainous areas, covering everything from the web application down to microcontroller control.
Field devices talk over 429MHz low-power radio, so farms can be monitored and controlled remotely even where network infrastructure is unstable.

## Notes for Public Repository

This is a public portfolio version.
It contains the core logic of the server and web UI.

Not included in this repository:
- Backend for user login / logout, signup and session-based authentication
- Backend for admin account management, role-based access control and owner / pin access assignment
- Device firmware (ESP32 / Arduino)
- Communication prediction module (`predict.py` / `predict.html`, still under development)
- Credentials, tokens and production configuration

The login, signup and admin **pages** (`login.html`, `signup.html`, `admin.html`) are included for reference only.
The `/login`, `/signup`, `/logout` and `/admin/*` routes they post to do not exist in this version.

## Repository Contents

| File | Description |
| --- | --- |
| `app.py` | Flask backend (REST API, SQLite storage, schedule engine) |
| `index.html` | Main dashboard: map, status, graphs, valve control and schedule calendar |
| `indextanku.html` | Dashboard variant for water tank devices (tank level, schedule) |
| `login.html` / `signup.html` | Login and signup pages (UI only) |
| `admin.html` | Admin page for user roles and pin assignment (UI only) |

## Features

### Web dashboard (`index.html`)
- Device (pin) management on a Leaflet / OpenStreetMap map
- Register or move pins by WGS84 lat/lon or by RTK coordinates (JGD2011 plane rectangular, zones 1–19)
- Per-pin settings: name, machine type and notes
- Latest status and valve state (open / closed / pending / failed / unknown)
- Time-series graphs with Chart.js (soil moisture, RSSI up/down, noise)
- Manual valve control and a monthly schedule calendar (FullCalendar)
- CSV export of logs

### Backend (`app.py`)
- Telemetry ingestion and log storage in SQLite
- Command distribution to devices, with ACK-based state tracking
- Irrigation scheduling: open at the start time, close after N minutes (1–180)
- Gateway pull API: one request returns pending commands for several pins
- Automatic one-time migration from the legacy `db.json` to SQLite

## System Architecture

```
[Browser] --HTTP--> [Flask server (app.py) + SQLite]
                          ^
                          | HTTP (token auth)
                          v
                     [Gateway] <--429MHz--> [Field devices (ESP32 / Arduino)]
```

- Backend: Flask (Python), SQLite, pyproj
- Frontend: HTML / JavaScript, Leaflet, Chart.js, FullCalendar
- Device: ESP32 / Arduino (firmware not included)
- Communication: 429MHz radio module by Circuit Design, Inc.

## Command / ACK Flow

1. The user (or the schedule engine) creates a command. Each new command gets a higher `cmd_id`.
2. The device or gateway polls for commands. A command is **pending** while `cmd_id` differs from the last ACKed `cmd_id`.
3. The device runs the command and sends an ACK with `cmd_id` and `ok`.
4. The valve state is taken from telemetry (`valve_state` / `valve_open`) when it is available. Otherwise it is estimated from the last ACKed command.

Schedules are checked whenever a device or gateway polls, so no background job is needed.
A new schedule step does not start while a command is still pending.

## API

### Web UI
| Method | Path | Description |
| --- | --- | --- |
| GET / POST | `/api/pins` | List pins / create a pin |
| PUT | `/api/pins/<pin_id>` | Update name, type, note or position |
| POST | `/api/pins/<pin_id>/move` | Move a pin |
| DELETE | `/api/pins/<pin_id>` | Delete a pin and its data |
| GET | `/api/latest/<pin_id>` | Latest telemetry |
| GET | `/api/logs/<pin_id>` | Log history |
| GET / POST | `/api/command/<pin_id>` | Get / set the current command |
| GET / POST | `/api/schedule/<pin_id>` | Get / add schedule items |
| DELETE | `/api/schedule/<pin_id>/<sid>` | Delete a schedule item |
| GET | `/api/pin_status/<pin_id>` | Valve status summary |
| GET | `/api/export/<pin_id>.csv` | CSV export |
| GET | `/api/debug/pin/<pin_id>` | Debug dump (command, ACK, schedule, latest) |
| GET | `/status` | Health check |

### Device / gateway
| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/telemetry/<pin_id>` | Send telemetry (`soil`, `rssi_up`, `rssi_down`, `noise`, `valve_state` / `valve_open`) |
| GET | `/api/device/command/<pin_id>?token=...` | Get the pending command |
| POST | `/api/device/ack/<pin_id>?token=...` | Send an ACK |
| GET | `/api/device/schedule/<pin_id>?token=...` | Get the schedule |
| GET | `/api/gateway/<gateway_id>/pull?token=...&pins=1,2,3` | Get pending commands for several pins |

Device endpoints check `token` against `DEVICE_TOKEN_GLOBAL` if it is set, or against the per-pin token in the `device_tokens` table if it is not.
The gateway pull API requires `DEVICE_TOKEN_GLOBAL`.

> In this public version, the web UI endpoints and `/api/telemetry` have no authentication.
> Do not expose it to the internet as is.

## Running Locally

```bash
pip install flask pyproj
export DEVICE_TOKEN_GLOBAL=your-device-token   # optional
python app.py                                  # http://0.0.0.0:5000
```

The SQLite database (`allcot.db`) is created on first run.
`app.py` does not serve the HTML files itself, so serve them from the same origin as the API (for example through a reverse proxy).

## Background

In rural areas, network infrastructure is often unstable, so conventional IoT solutions are hard to deploy.
This system was designed to work under those limits using low-bandwidth communication.

## Design Considerations

- Role-based access control for different users (farmers, administrators), in the private version
- Device visibility limited by assigned permissions, in the private version
- Fail-safe local control when communication is lost
- Command and ACK mechanism for reliable state management

## Future Work

- Local control using flow sensors
- AI-based irrigation optimization
- Communication strength prediction for device placement (`predict.py` / `predict.html`)

## Third-Party Components

This system uses a 429MHz wireless communication module provided by Circuit Design, Inc.
The hardware and its radio communication technology are the intellectual property of that company.

This project focuses on the system design and software built on top of these modules.

## License

This project is created for portfolio purposes.

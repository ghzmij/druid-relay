# DRUID Relay Stage 1

Render settings:
- Build: `pip install -r requirements.txt`
- Start: `python server.py`
- Health: `/health`

After deployment:
- `https://YOUR-SERVICE.onrender.com/health`
- `wss://YOUR-SERVICE.onrender.com/relay`

First WebSocket frame:
`{"type":"register","room":"PAIR_ID","role":"windows"}`
or role `android`.

After registration the relay forwards application frames unchanged and does
not persist them. Stage 1 is transport only: use the existing end-to-end
encrypted DRUID Link payloads; do not send plaintext coordinates or link keys.

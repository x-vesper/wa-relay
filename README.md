# wa-relay

Jembatan lokal: pesan WhatsApp (hanya dari owner) → webhook HTTP lokal → `opencode run` → balasan dikirim balik ke WhatsApp via `wacli send text`. Ditulis dalam **Go** (single binary, tanpa dependensi runtime).

## Cara kerja (alur)

```
Owner kirim pesan WhatsApp
        │
        ▼
wacli sync --follow  ──webhook──►  wa-relay (Go)  (127.0.0.1:8791/hook)
                                      │  verifikasi HMAC, filter owner,
                                      │  dedupe ID, respon 200 segera
                                      ▼
                                  antrean (queue) ──worker pool (num_workers)──►
                                      │
                                      ▼
                              opencode run --format json
                                      │
                                      ▼
                              parse jawaban agent ──► wacli send text ──► Owner
```

Pesan `!ping` (case-insensitive; `ping` polos juga masih diterima) dibalas `pong` langsung tanpa memanggil agent. `!help` menampilkan daftar perintah, `!model` menampilkan/mengganti model chat, `!worker` (alias `!cek`) menampilkan status slot build, dan `!reset` (alias `!restart`) menghapus konteks sesi sehingga pesan berikutnya mulai obrolan baru.

### Stepi sebagai resepsionis (serah terima ke tim teknis)

Secara default **semua** pesan ditangani stepi (lane chat) dan stepi **tidak** mengerjakan tugas
teknis sendiri. Kalau bos memerintah, stepi menyerahkannya ke tim teknis (agent `work_agent`,
default `build`):

- Pemicu default (bisa ditambah lewat `handover_keywords`): kata kerja serah terima seperti `serahin`,
  `serahkan`, `serah`, `limpahin`, `delegasikan` — bukan sekadar menyebut nama target. Contoh:
  *"serahin aja tugasnya ke Pak Budi"*. Kalimat yang dinegasi (`jangan serahin...`) atau berupa
  pertanyaan (`udah kamu serahin ke pak budi?`, `pak budi udah kerja?`) **tidak** memicu serah terima
  baru — cuma dijawab stepi sebagai obrolan biasa.
- Stepi menyusun **spesifikasi tugas dari konteks obrolan** (jadi bisa menyerahkan tugas yang
  disebut di pesan sebelumnya), mengirim ack singkat (`handover_ack`), lalu build dijalankan di
  latar belakang (lane work, workdir `work_workdirs`) dan hasilnya dirangkum stepi untuk bos.
- Kalau semua slot build terpakai, stepi mengabari bos bahwa tim sedang sibuk.
- Routing otomatis berdasarkan kata kunci teknis/panjang pesan **dimatikan secara default**
  (`auto_work_routing: false`). Set `auto_work_routing: true` untuk mengaktifkan perilaku lama
  (pesan teknis otomatis diusulkan ke lane work + minta konfirmasi "setuju?").


### Ingatan sesi (session continuity)

Relay menyimpan `sessionID` opencode di `state/session.json`. Pesan pertama membuat sesi baru
(`--title wa-relay`), lalu pesan-pesan berikutnya dijalankan dengan `opencode run --session <id>`
sehingga agent **ingat percakapan sebelumnya** (tidak lagi lupa konteks antar pesan).

- **Sesi sengaja di-reset tiap restart relay.** Saat start, id sesi lama dibuang (lihat catatan di
  `cmd/wa-relay/main.go`), jadi tiap restart selalu mulai obrolan baru. Ini disengaja supaya relay
  tidak nyangkut id sesi yang sudah dihapus dari opencode (`Session not found`).
- Kirim `!reset` atau `!restart` (boleh ada spasi) di WhatsApp untuk menghapus sesi dan mulai dari nol.
- Sesi juga otomatis di-rollover kalau nganggur >24 jam, umur >7 hari, atau >20 pesan.

### File & foto (dinamis)

Relay tidak pernah mengirim lampiran kecuali agent memintanya, jadi chat teks biasa tetap ringan.

- **Kirim (agent → WA):** kalau agent ingin mengirim sebuah file/foto, dia menulis directive di baris sendiri:
  `[[file:/path/absolut/ke/file.jpg|caption opsional]]` (caption boleh dikosongkan: `[[file:/path/file.jpg]]`).
  Relay mengirim sisa teks sebagai pesan biasa lalu lampirannya lewat `wacli send file`. Directive hanya
  diproses bila menempati satu baris penuh, path-nya absolut, dan file-nya benar-benar ada — jadi diskusi kode
  tentang directive ini tidak ikut memicu kiriman. Matikan dengan `send_file_enabled: false`.
- **Terima (owner → agent):** pesan berlampiran diunduh ke `media_dir` lewat `wacli media download`, lalu path
  lokalnya disisipkan ke prompt sehingga agent bisa membaca foto/dokumen tersebut. Matikan dengan
  `incoming_media_enabled: false`.


- Kalau id sesi tersimpan ternyata sudah hilang, relay otomatis melepas pointer dan mulai sesi baru
  (tidak macet permanen).
- Catatan: sesi yang terus lanjut akan makin panjang (token makin besar) — pakai `!reset` sesekali.

## Spesifikasi payload & signature (wacli)

- Endpoint: `POST /hook`
- Header: `X-Wacli-Signature: sha256=<hex hmac-sha256(raw body, secret)>`
- Payload pesan (flat JSON, tanpa field `EventType`):

```json
{"Chat":"62812xxxxxxx@s.whatsapp.net","ID":"3EB0...","SenderJID":"62812xxxxxxx@s.whatsapp.net","Timestamp":"2026-10-15T10:00:00Z","FromMe":false,"Text":"hi","ChatName":"Owner"}
```

- `Media`: objek atau `null`.
- Receipt punya `EventType":"receipt"`, presence `EventType":"chat_presence"` — keduanya diabaikan.

## Konfigurasi (`config.json`)

| Field | Arti |
|---|---|
| `host` / `port` | Alamat listen relay (default `127.0.0.1:8791`) |
| `webhook_secret` | Secret HMAC untuk verifikasi signature |
| `owner_jid` / `owner_phone` | JID & nomor owner (satu-satunya pengirim yang diproses) |
| `workdir` | folder kerja `opencode run` (lewat cwd proses) |
| `chat_workdir` | folder kerja stepi (lane chat), idealnya worktree terpisah dari build |
| `work_workdirs` | Daftar workdir build; satu entri = satu slot build paralel |
| `work_agent` | Agent opencode untuk tim teknis (default `build`) |
| `agent` | Nama agent opencode yang menjawab (`--agent`, default `stepi`) |
| `chat_models` | Daftar model opencode untuk lane chat/stepi; entri pertama jadi default, bisa diganti saat runtime lewat `!model` |
| `chat_model` | Model tunggal lane chat (dipakai bila `chat_models` kosong) |
| `work_model` | Model opencode untuk lane build (`--model`), mis. `opencode/deepseek-v4.1-flash-free` |
| `model` | Fallback model kalau `chat_models`/`chat_model`/`work_model` kosong; kosong = default opencode |
| `command_prefix` | Awalan perintah WhatsApp (default `!`); mis. `!help`, `!model`, `!ping`, `!reset` |
| `auto_work_routing` | Routing otomatis pesan teknis ke lane work (default `false` = stepi resepsionis) |
| `handover_keywords` | Frasa tambahan pemicu serah terima ke tim teknis (selain default) |
| `handover_ack` | Pesan singkat saat tugas diserahkan (default: "Oke Pak, tugasnya aku serahin ke Pak Budi ya.") |
| `ack_delay_seconds` | Kirim ack pertama hanya bila task masih jalan melewati detik ini (default 300 = 5 menit); `0` untuk menonaktifkan |
| `ack_messages` | Pool ack pertama (dipilih acak), dikirim setelah `ack_delay_seconds`; `[]` untuk menonaktifkan |
| `ack_followups` | Array `{after_seconds, messages}`, pesan susulan bila task belum selesai (default 600s) |
| `opencode_bin` / `wacli_bin` | Path binary |
| `num_workers` | Jumlah worker paralel yang menguras antrean (default 3) |
| `chat_workers` | Jumlah worker khusus lane chat (pesan ringan stepi), terpisah dari lane work (default 1) |
| `timeout_seconds` | Timeout `opencode run` (default 900) |
| `mark_read` | Tandai pesan owner sebagai sudah dibaca / kirim read receipt lewat `wacli chats mark-read --receipts` (default `true`); dikirim tepat sebelum balasan final, bukan saat pesan masuk |
| `send_file_enabled` | Izinkan relay mengirim file/foto ke WA saat agent menyelipkan directive lampiran (default `true`) |
| `incoming_media_enabled` | Unduh lampiran (foto/file) dari owner supaya agent bisa membacanya (default `true`) |
| `media_dir` | Direktori penyimpanan lampiran masuk (default `<base>/state/media`) |
| `log_file` | Log file relay |

`opencode.json` berisi permission untuk agent yang di-spawn relay: allow read/edit/glob/grep/list, `external_directory` allow-all, bash allow-all tapi **last-match-wins deny** untuk `rm -rf *`, `sudo *`, `git push *`. Longgarkan/ketatkan sesuai kebutuhan.

## Build & pemakaian

```bash
go build -o wa-relay ./cmd/wa-relay   # atau langsung ./start.sh (auto-build kalau binary belum ada / source berubah)
./start.sh               # restart relay + sync (delegasi ke systemd jika terpasang)
./stop.sh                # stop keduanya
go test ./...            # jalankan test
```

`start.sh` / `stop.sh` otomatis memakai **systemd user service** kalau sudah terpasang
(`systemctl --user cat wa-relay.service` berhasil); kalau belum, jatuh ke mode manual
(`setsid`/`nohup` + pidfile).

Log: `relay.log` (relay) dan `sync.log` (wacli sync). Health check: `curl http://127.0.0.1:8791/health` → `{"ok":true}`.

## Systemd (always-on)

Biar relay selalu nyala (auto-restart saat crash, auto-start saat boot, jalan tanpa login):

```bash
./systemd/install.sh     # pasang + enable + start (idempotent)
```

`install.sh` akan:
- menaruh `wa-relay.service` & `wa-relay-sync.service` ke `~/.config/systemd/user/`
  (path repo & `$HOME` di-substitusi otomatis dari `systemd/*.service.in`),
- `daemon-reload` + `enable --now` kedua service,
- `loginctl enable-linger $USER` supaya tetap jalan walau tidak ada sesi login.

Kontrol manual:

```bash
systemctl --user status wa-relay wa-relay-sync
systemctl --user restart wa-relay-sync     # sync saja
systemctl --user stop wa-relay wa-relay-sync
journalctl --user -u wa-relay -f           # log relay (streaming)
```

`wa-relay-sync.service` meng-`Requires` + `After` `wa-relay.service`, dan secret dibaca
dari `config.json` lewat `run-sync.sh`.

## Keamanan

- Hanya listen di `127.0.0.1` (localhost).
- Setiap webhook diverifikasi HMAC-SHA256; signature salah → 401.
- Hanya pesan dari `owner_jid`, bukan `FromMe`, bukan grup/channel/status, teks kosong diabaikan.
- Dedupe by message `ID` (500 terakhir, disimpan di `state/seen.json`; penulisan di-debounce tiap 2 detik).
- Sesi opencode terakhir disimpan di `state/session.json` (hapus untuk reset).
- Policy permission agent di `opencode.json` (bisa diketatkan).

## Mengubah owner / limit

- Owner: edit `owner_jid` + `owner_phone` di `config.json`, lalu `./start.sh`.
- Timeout agent: `timeout_seconds`.
- Secret: ganti `webhook_secret` (mis. `python3 -c "import secrets;print(secrets.token_hex(32))"`) di `config.json`, lalu restart: `./start.sh` (atau `systemctl --user restart wa-relay wa-relay-sync`). `start.sh`/`run-sync.sh` membacanya otomatis dari config.

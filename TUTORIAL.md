# Buku Manual wa-relay

Panduan lengkap operasional **wa-relay**: apa itu, cara pasang, cara build,
cara start/restart, cara baca log, sampai troubleshooting. Ditulis sedetail
mungkin supaya bisa diikuti tanpa asumsi.

> Semua contoh memakai repo di `/home/zakaria/projects/wa-relay`.
> Ganti path kalau repo-mu di lokasi lain. Kalau ragu, selalu mulai dengan:
>
> ```bash
> cd /home/zakaria/projects/wa-relay
> ```

---

## Daftar isi

1. [Apa itu wa-relay (dan cara kerjanya)](#1-apa-itu-wa-relay)
2. [Prasyarat & cek lingkungan](#2-prasyarat--cek-lingkungan)
3. [Peta folder repo](#3-peta-folder-repo)
4. [Cara build (go build dijelaskan tuntas)](#4-cara-build)
5. [Konfigurasi config.json](#5-konfigurasi-configjson)
6. [Cara start / restart / stop](#6-cara-start--restart--stop)
7. [Systemd: selalu nyala](#7-systemd-selalu-nyala)
8. [Verifikasi & monitoring](#8-verifikasi--monitoring)
9. [Sesi, rollover, dan working directory](#9-sesi-rollover-dan-working-directory)
10. [Alur pesan (chat vs work, delegasi, sibuk)](#10-alur-pesan)
11. [Troubleshooting](#11-troubleshooting)
12. [Bersih-bersih & uninstall](#12-bersih-bersih--uninstall)
13. [Cheat sheet](#13-cheat-sheet)

---

## 1. Apa itu wa-relay

wa-relay adalah jembatan lokal:

```
WA (khusus owner) → wacli sync → webhook lokal → opencode run → wacli send → WA
```

- Ditulis dalam **Go**, hasilnya satu binary (`wa-relay`), tanpa dependensi runtime.
- Hanya memproses pesan dari `owner_jid`.
- Dua "kanal" (lane):
  - **chat** dikerjakan **stepi** (receptionist) di `chat_workdir`.
  - **work** (tugas berat/coding) dikerjakan **tim build** di salah satu
    `work_workdirs` (satu folder = satu slot build).
- Ingatan percakapan disimpan di sesi opencode, **tapi sengaja di-reset tiap
  restart** (lihat [§9](#9-sesi-rollover-dan-working-directory)).

---

## 2. Prasyarat & cek lingkungan

Yang dibutuhkan:

| Kebutuhan | Kenapa | Cek |
|---|---|---|
| Go (1.25+) | build binary | `go version` |
| `opencode` | menjalankan agent | `opencode --version` |
| `wacli` | kirim/terima WhatsApp | `wacli --version` |
| `python3` | `run-sync.sh` baca secret dari config | `python3 --version` |
| `systemd --user` (opsional) | always-on | `systemctl --user --version` |

Cek semuanya sekaligus:

```bash
go version
which go opencode wacli python3
```

Contoh hasil sehat di mesin ini:

```
go version go1.25.0 linux/amd64
/usr/local/go/bin/go
/home/zakaria/.opencode/bin/opencode
/home/linuxbrew/.linuxbrew/bin/wacli
/usr/bin/python3
```

Catatan penting soal **PATH**: service systemd hanya punya PATH terbatas. Unit
`wa-relay.service` sudah menyetel:

```
Environment=PATH=/home/zakaria/.opencode/bin:/home/linuxbrew/.linuxbrew/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
```

Jadi `opencode` (di `~/.opencode/bin`) dan `wacli` (di linuxbrew) ketemu. Kalau
kamu pindah lokasi binary, update PATH di `systemd/*.service.in` lalu
`./systemd/install.sh` lagi.

Kalau `opencode`/`wacli` tidak ada, pasang dulu (opencode: installer resmi;
wacli: `brew install wacli` atau rilis linuxbrew), baru lanjut.

---

## 3. Peta folder repo

```
wa-relay/
├── cmd/wa-relay/main.go     # titik masuk program (wiring config, service)
├── internal/relay/          # seluruh logika relay (handler, sesi, lane, pool)
├── config.json              # KONFIGURASI AKTIF (gitignored, berisi secret)
├── config.example.json      # contoh config untuk referensi
├── opencode.json            # policy permission agent
├── start.sh / stop.sh       # shortcut start/stop
├── run-sync.sh              # wrapper `wacli sync --follow` (dipakai systemd)
├── systemd/                 # unit + install.sh
├── state/                   # RUNTIME (sesi, seen, delegasi) — gitignored
├── relay.log                # log relay (gitignored)
└── sync.log                 # log wacli sync (gitignored)
```

Yang **jangan** di-commit (sudah masuk `.gitignore`): `config.json`, `wa-relay`
(binary), `state/`, `relay.log`, `sync.log`, `*.pid`.

---

## 4. Cara build

### 4.1 Apa yang terjadi saat `go build`

`go build` meng-compile kode Go jadi binary. Repo ini pakai **Go modules**
(ada `go.mod`, module `github.com/Seronz/wa-relay`), jadi **tidak perlu GOPATH**
dan **tidak perlu internet** (tidak ada dependensi eksternal).

Titik masuk ada di paket `./cmd/wa-relay` (berisi `package main`).

### 4.2 Perintah build yang dipakai

```bash
cd /home/zakaria/projects/wa-relay
go build -o wa-relay ./cmd/wa-relay
```

Penjelasan tiap bagian:

- `go build` — perintah compile.
- `-o wa-relay` — **output** binary diberi nama `wa-relay` di folder saat ini
  (`./wa-relay`). Tanpa `-o`, nama default = nama folder paket, jadi tetap
  `wa-relay`, tapi lebih eksplisit begini.
- `./cmd/wa-relay` — paket yang dibuild (relatif ke cwd). Titik di depan artinya
  "dari folder ini".

Setelah selesai, akan muncul file `wa-relay` di root repo:

```bash
ls -la wa-relay        # -rwxr-xr-x ... wa-relay
```

### 4.3 Build semua paket / cek tanpa menulis binary

```bash
go build ./...         # build semua paket dalam module
go vet ./...           # analisa statis (cari bug umum)
gofmt -l .             # daftar file yang formatnya belum rapi (kosong = OK)
gofmt -w internal cmd  # rapikan format otomatis
```

### 4.4 Menjalankan test

```bash
go test ./...                       # jalankan semua test
go test ./internal/relay/ -v        # test package relay, verbose
go test ./internal/relay/ -run Sesi # hanya test yang namanya mengandung "Sesi"
```

### 4.5 Menjalankan tanpa build (mode develop)

```bash
cd /home/zakaria/projects/wa-relay
go run ./cmd/wa-relay -config config.json
```

`go run` compile lalu langsung jalankan di foreground (Ctrl+C untuk stop).
Bagus untuk ngoprek cepat, **tapi jangan dipakai bareng systemd** (bisa rebutan
port 8791).

### 4.6 Kesalahan umum saat build

| Gejala | Arti | Solusi |
|---|---|---|
| `go: command not found` | Go belum terpasang / tidak di PATH | pasang Go, atau pakai path lengkap `/usr/local/go/bin/go build ...` |
| `no required module provides package ...` | jalankan dari folder salah | `cd` ke root repo dulu |
| `cannot find package ... in any of` | cwd bukan root module | pastikan ada `go.mod` di cwd |
| permission denied saat tulis binary | binary sedang dipakai/di-lock | stop service dulu: `./stop.sh` |

> **PENTING:** habis mengubah kode, **selalu** `go build` ulang. `start.sh`
> **tidak** rebuild kalau binary `wa-relay` sudah ada.

---

## 5. Konfigurasi `config.json`

File ini **wajib** ada dan **gitignored** (berisi `webhook_secret`). Mulai dari
`config.example.json` kalau belum ada:

```bash
cp config.example.json config.json
```

Edit pakai editor apa pun. **Validasi** setelah edit:

```bash
python3 -c "import json;json.load(open('config.json'));print('config OK')"
```

### 5.1 Tabel field

| Field | Tipe | Default | Arti |
|---|---|---|---|
| `host` | string | `127.0.0.1` | alamat listen (biarkan localhost) |
| `port` | int | `8791` | port webhook relay |
| `webhook_secret` | string | — | secret HMAC verifikasi webhook wacli |
| `owner_jid` | string | — | JID WhatsApp owner (satu-satunya yang diproses) |
| `owner_phone` | string | — | nomor owner (target kirim balasan) |
| `workdir` | string | folder repo | fallback; folder kerja `opencode run` (cwd) |
| `chat_workdir` | string | = `workdir` | folder kerja **stepi** (chat) |
| `work_workdirs` | []string | `[workdir]` | folder kerja **tim build**; jumlah entri = jumlah slot build paralel |
| `work_busy_prompt` | string | template bawaan | prompt ke stepi saat semua slot build penuh |
| `agent` | string | `stepi` | nama agent opencode untuk chat |
| `work_agent` | string | `build` | nama agent opencode untuk tugas berat |
| `command_prefix` | string | `!` | awalan perintah WhatsApp, mis. `!help`, `!model`, `!ping`, `!reset` |
| `chat_models` | []string | default opencode | daftar model lane chat/stepi; entri pertama default, bisa diganti lewat `!model` |
| `chat_model` | string | default opencode | model tunggal lane chat (dipakai bila `chat_models` kosong) |
| `work_model` | string | default opencode | model lane build, mis. `opencode/deepseek-v4.1-flash-free` |
| `model` | string | default opencode | fallback bila model per lane kosong |
| `ack_delay_seconds` | int | `300` | kirim ack pertama hanya bila task masih jalan > detik ini (`0` = mati) |
| `ack_messages` | []string | pool bawaan | kandidat pesan ack (dipilih acak) |
| `ack_followups` | array | 1 entri | `{after_seconds, messages}` pesan susulan |
| `opencode_bin` | string | `opencode` | path binary opencode |
| `wacli_bin` | string | `wacli` | path binary wacli |
| `num_workers` | int | `3` | jumlah worker lane work |
| `chat_workers` | int | `1` | jumlah worker lane chat |
| `light_max_len` | int | `400` | pesan > sekian rune dianggap tugas berat |
| `heavy_keywords` | []string | `[]` | kata kunci tambahan pemicu lane work |
| `timeout_seconds` | int | `900` | timeout `opencode run` |
| `mark_read` | bool | `true` | kirim read receipt (centang biru) |
| `chat_session_file` | string | `state/session.json` | lokasi file sesi chat |
| `work_session_file` | string | `state/work-session.json` | lokasi file sesi work |
| `session_idle_seconds` | int | `86400` | rollover bila nganggur > ini (0 = mati) |
| `session_max_age_seconds` | int | `604800` | rollover bila umur sesi > ini (0 = mati) |
| `session_max_turns` | int | `60` (config ini pakai `20`) | rollover setelah sekian pesan (0 = mati) |
| `delegation_ttl_seconds` | int | `1800` | masa berlaku konfirmasi delegasi |
| `log_file` | string | `relay.log` | lokasi log relay |

### 5.2 Membuat `webhook_secret` baru

```bash
python3 -c "import secrets;print(secrets.token_hex(32))"
```

Tempel hasilnya ke `webhook_secret`, lalu **restart relay dan sync** (secret
harus sama di keduanya — `run-sync.sh` membacanya dari `config.json` yang sama,
jadi cukup restart).

### 5.3 Contoh `work_workdirs` (2 slot build via git worktree)

```json
"work_workdirs": [
  "/home/zakaria/projects/wa-relay-build-1",
  "/home/zakaria/projects/wa-relay-build-2"
]
```

Lihat [§9.4](#94-working-directory-stepi-vs-tim-build) untuk bikin worktree-nya.

---

## 6. Cara start / restart / stop

### 6.0 Pertanyaan penting: harus dari folder tertentu?

| Perintah | Harus dari folder repo? | Alasan |
|---|---|---|
| `go build -o wa-relay ./cmd/wa-relay` | **Ya** | `./cmd/...` relatif ke cwd |
| `./start.sh` / `./stop.sh` | **Ya**, atau panggil pakai path absolut | script `cd "$(dirname "$0")"` sendiri |
| `./wa-relay -config config.json` (manual) | **Ya** | `config.json` relatif ke cwd |
| `systemctl --user ...` | Tidak | `ExecStart` di unit pakai path absolut |
| `journalctl --user -u wa-relay -f` | Tidak | — |
| `curl http://127.0.0.1:8791/health` | Tidak | alamat jaringan |

Jadi: **build & script → dari repo; systemctl → dari mana saja.**

### 6.1 Cara paling gampang (systemd sudah terpasang)

```bash
cd /home/zakaria/projects/wa-relay
./start.sh
```

`start.sh` akan:
1. `cd` ke folder script (repo).
2. Kalau `./wa-relay` belum ada → build otomatis.
3. Kalau systemd user service terpasang → `systemctl --user restart wa-relay.service`
   dan `wa-relay-sync.service`, lalu tampilkan status + health.
4. Kalau systemd **tidak** terpasang → mode manual (`setsid`/`nohup` + pidfile).

Stop:

```bash
cd /home/zakaria/projects/wa-relay
./stop.sh
```

### 6.2 Kontrol langsung dengan systemctl (restart saja)

```bash
systemctl --user restart wa-relay.service            # relay
systemctl --user restart wa-relay-sync.service       # wacli sync
systemctl --user restart wa-relay.service wa-relay-sync.service
systemctl --user stop wa-relay-sync.service wa-relay.service
systemctl --user start  wa-relay.service wa-relay-sync.service
```

### 6.3 Alur update kode (paling sering dipakai)

```bash
cd /home/zakaria/projects/wa-relay
go build -o wa-relay ./cmd/wa-relay        # 1. build ulang
systemctl --user restart wa-relay.service  # 2. restart
curl -s http://127.0.0.1:8791/health       # 3. cek: {"ok":true}
tail -n 20 relay.log                       # 4. lihat log
```

### 6.4 Mode manual (tanpa systemd)

```bash
cd /home/zakaria/projects/wa-relay
go build -o wa-relay ./cmd/wa-relay
./stop.sh
./start.sh
```

`start.sh` mode manual menjalankan relay & sync pakai `setsid nohup`, menyimpan
PID di `relay.pid` / `sync.pid`, dan log ke `relay.log` / `sync.log`.

Cek proses manual:

```bash
pgrep -af "wa-relay"
pgrep -af "wacli sync"
```

---

## 7. Systemd: selalu nyala

### 7.1 Pasang (sekali saja, idempotent)

```bash
cd /home/zakaria/projects/wa-relay
./systemd/install.sh
```

`install.sh` akan:
- menyulih `__BASE_DIR__`/`__HOME__` di `systemd/*.service.in` → `~/.config/systemd/user/`,
- `chmod +x run-sync.sh`,
- build `wa-relay` kalau belum ada,
- `systemctl --user daemon-reload` + `enable --now` kedua service,
- `loginctl enable-linger $USER` (biar tetap jalan tanpa sesi login).

### 7.2 Dua service

| Service | Fungsi | Sumber log |
|---|---|---|
| `wa-relay.service` | proses relay (HTTP webhook) | `relay.log` (lewat `log_file`) & journal |
| `wa-relay-sync.service` | `wacli sync --follow` (webhook ke relay) | `sync.log` |

`wa-relay-sync` **Requires + After** `wa-relay`, jadi relay dulu baru sync.

### 7.3 Perintah systemctl berguna

```bash
systemctl --user status wa-relay.service wa-relay-sync.service
systemctl --user restart wa-relay.service
systemctl --user is-active wa-relay.service      # active / inactive
systemctl --user cat wa-relay.service            # lihat unit yang terpasang
journalctl --user -u wa-relay.service -n 100 --no-pager
journalctl --user -u wa-relay.service -f          # streaming
journalctl --user -u wa-relay-sync.service -f
```

Log bisa "dobel" (di `relay.log` dan journal) — itu normal; relay menulis ke
`relay.log`, systemd juga menangkap stdout.

### 7.4 Cek linger (headless)

```bash
loginctl show-user "$USER" | grep Linger     # ingin: Linger=yes
```

Kalau `no`:

```bash
sudo loginctl enable-linger "$USER"
```

---

## 8. Verifikasi & monitoring

### 8.1 Health check

```bash
curl -s http://127.0.0.1:8791/health
# {"ok":true}
```

Kalau gagal (connection refused) → relay belum jalan / port beda / crash.

### 8.2 Baris log startup yang sehat

```
INFO workdirs chat=/home/zakaria/projects/wa-relay work=[...build-1 ...build-2] (slot build=2)
INFO wa-relay listening on http://127.0.0.1:8791/hook workdir=/home/zakaria/projects/wa-relay
INFO session rollover policy idle=24h0m0s max_age=168h0m0s max_turns=20 (0 = mati)
INFO worker pool started chat=1 work=3
```

Arti:
- `workdirs chat=... work=[...] (slot build=N)` → folder stepi & tim build, dan
  jumlah build paralel.
- `session rollover policy ...` → kapan sesi di-reset otomatis.
- `worker pool started chat=1 work=3` → jumlah worker.

### 8.3 Baris log saat memproses pesan

```
INFO queued id=... len=5 lane=chat
INFO task start id=... text_len=5
INFO opencode done duration=12.3s stdout=1234B stderr=0B
INFO mark-read ok chat=628xxxx@s.whatsapp.net
INFO wacli send chunk 1/1 ok=true
INFO task done id=... reply_len=42 duration=16.4s
```

- `lane=chat` / `lane=work` → hasil klasifikasi.
- `stdout/stderr` besar kecilnya bisa jadi petunjuk.
- `session rollover ... reason=idle|age|turns` → sesi dilepas karena kebasi.

### 8.4 Melihat log real-time

```bash
tail -f relay.log
# atau via systemd:
journalctl --user -u wa-relay.service -f
```

---

## 9. Sesi, rollover, dan working directory

### 9.1 Perilaku sesi

- Sesi chat disimpan di `state/session.json`.
- **Tiap restart relay, sesi sengaja dikosongkan** (lihat catatan di
  `cmd/wa-relay/main.go`). Jadi setiap start selalu mulai obrolan baru. Ini
  mencegah relay nyangkut id sesi yang sudah dihapus dari opencode.
- Selama relay hidup, pesan berurutan melanjutkan sesi yang sama (agent ingat
  percakapan), sampai salah satu aturan rollover kena:
  - nganggur > `session_idle_seconds` (default 24 jam),
  - umur sesi > `session_max_age_seconds` (default 7 hari),
  - jumlah pesan > `session_max_turns` (default 20).
- Kalau id sesi tersimpan ternyata sudah tidak ada di opencode
  (`Session not found`), relay **otomatis** melepas pointer dan mulai sesi baru.

### 9.2 Reset manual

Dari WhatsApp:

```
!reset
```

Dari server (efektif setelah restart):

```bash
cd /home/zakaria/projects/wa-relay
rm -f state/session.json state/work-session.json
systemctl --user restart wa-relay.service
```

### 9.3 Fast-path

- Kirim `!help` → daftar perintah yang tersedia.
- Kirim `!model` → lihat model chat aktif & daftar pilihan; `!model 2` untuk ganti.
- Kirim `!worker` (alias `!cek`) → lihat slot build yang aktif/bebas.
- Kirim `!ping` → dibalas `pong` tanpa memanggil agent.
- Kirim `!reset` → sesi dihapus, balasan konfirmasi.

### 9.4 Working directory: stepi vs tim build

- `chat_workdir` → folder **stepi**.
- `work_workdirs` → folder **tim build**; satu folder = satu slot. 2 entri =
  maksimal 2 build paralel. Disarankan **git worktree** dari repo yang sama
  supaya tidak saling menimpa file/git.

Lihat & bikin worktree:

```bash
cd /home/zakaria/projects/wa-relay
git worktree list
git worktree add -b wa-build-3 /home/zakaria/projects/wa-relay-build-3
# tambahkan path barunya ke "work_workdirs" di config.json, lalu:
go build -o wa-relay ./cmd/wa-relay && systemctl --user restart wa-relay.service
```

Hapus worktree yang tidak dipakai:

```bash
git worktree remove /home/zakaria/projects/wa-relay-build-3
git branch -D wa-build-3
```

---

## 10. Alur pesan

1. Pesan owner masuk → `wacli sync` kirim ke `POST /hook`.
2. Relay verifikasi HMAC, filter (hanya owner, bukan `FromMe`, bukan grup),
   dedupe by `ID`, balas `200` segera.
3. Pesan diklasifikasi:
   - perintah cepat (`!help`, `!model`, `!worker`, `!ping`, `!reset`) → fast-path,
   - mengandung kata kunci berat **atau** lebih panjang dari `light_max_len`
     → **lane work**,
   - sisanya → **lane chat** (stepi).
4. Lane chat: stepi menjawab; balasan dikirim via `wacli send text`.
5. Lane work: stepi **meringkas requirement** dan meminta konfirmasi bos
   ("…, bos setuju?"). Setelah bos balas setuju (`ya/oke/gas/...`), relay
   menjalankan agent **build** di salah satu slot `work_workdirs`, lalu stepi
   merangkum hasilnya untuk bos.
6. **Semua slot build terpakai?** Build berikutnya tidak antre diam — stepi
   mengabari bos **"tim lagi sibuk semua"**.
7. Ack/follow-up (`ack_messages`, `ack_followups`) dikirim hanya kalau task
   masih berjalan melewati `ack_delay_seconds`.

---

## 11. Troubleshooting

### 11.1 Relay tidak jalan / `/health` gagal

```bash
systemctl --user status wa-relay.service
journalctl --user -u wa-relay.service -n 100 --no-pager
```

- **Config JSON rusak** → relay langsung exit, log `config: parse ...`.
  ```bash
  python3 -c "import json;json.load(open('config.json'))"
  ```
- **Port kepakai** → `ss -ltnp | grep 8791`. Matikan proses lama / ganti `port`.
- **Binary hilang/belum dibuild**:
  ```bash
  go build -o wa-relay ./cmd/wa-relay
  ```

### 11.2 Muncul `(error: opencode exit 1: Session not found)` di WA

Id sesi tersimpan sudah dihapus dari opencode. Versi terbaru auto-recover, tapi
kalau masih muncul:

```bash
cd /home/zakaria/projects/wa-relay
rm -f state/session.json
systemctl --user restart wa-relay.service
```

Ingat: sekarang sesi memang di-reset tiap restart, jadi ini seharusnya jarang.

### 11.3 Pesan WA masuk tapi tidak dibalas

1. Sync jalan? `systemctl --user status wa-relay-sync.service`.
2. `tail -n 50 sync.log` (koneksi WA; sering karena WA logout / HP offline).
3. Dedupe: `ID` sama hanya diproses sekali (disimpan di `state/seen.json`).
4. `bad signature` di `relay.log` → `webhook_secret` beda. Restart
   `wa-relay-sync.service` setelah ubah secret.
5. Cek relay benar-benar menerima: `grep queued relay.log | tail`.

### 11.4 Balasan error dari agent

- `(error: opencode tidak ditemukan)` → `opencode_bin` salah / PATH service.
- `(error: timeout: agent terlalu lama)` → naikkan `timeout_seconds`.
- Error lain → lihat detail `opencode failed: ...` di `relay.log` (ada stderr).

### 11.5 Build selalu "tim lagi sibuk semua"

Berarti semua slot `work_workdirs` terpakai. Tunggu, atau tambah slot (§9.4).
Kalau menurutmu tidak ada yang jalan, cek proses nyangkut:

```bash
pgrep -af "opencode run"
```

### 11.6 Mark-read (centang biru) error

- `wacli_bin` benar & WA tersambung.
- Set `mark_read: false` untuk mematikan.
- Error `failed to dial whatsapp web` = masalah koneksi, bukan relay.

### 11.7 Worktree bermasalah

```bash
git worktree list            # daftar worktree
git worktree prune           # bersihkan metadata worktree yang folder-nya hilang
```

### 11.8 Ubah kode tapi tidak ada perubahan

Hampir pasti **lupa rebuild**. `start.sh` tidak rebuild kalau binary ada:

```bash
go build -o wa-relay ./cmd/wa-relay && systemctl --user restart wa-relay.service
```

---

## 12. Bersih-bersih & uninstall

### 12.1 Reset state runtime

`state/` aman dihapus (dibuat ulang otomatis):

```bash
cd /home/zakaria/projects/wa-relay
rm -rf state/
systemctl --user restart wa-relay.service
```

- Hapus `seen.json` → relay lupa dedupe (pesan lama bisa diproses ulang).
- Hapus `delegations.json` → konfirmasi delegasi yang menggantung hilang.

### 12.2 Uninstall systemd

```bash
systemctl --user disable --now wa-relay-sync.service wa-relay.service
rm -f ~/.config/systemd/user/wa-relay.service ~/.config/systemd/user/wa-relay-sync.service
systemctl --user daemon-reload
```

---

## 13. Cheat sheet

```bash
# Masuk folder kerja
cd /home/zakaria/projects/wa-relay

# Build + restart (habis ubah kode)
go build -o wa-relay ./cmd/wa-relay && systemctl --user restart wa-relay.service

# Start / stop
./start.sh
./stop.sh

# Status & health
systemctl --user status wa-relay.service wa-relay-sync.service
curl -s http://127.0.0.1:8791/health

# Log
tail -f relay.log
journalctl --user -u wa-relay.service -f

# Test & cek kode
go test ./...
go vet ./...
gofmt -l .

# Reset sesi
rm -f state/session.json && systemctl --user restart wa-relay.service

# Worktree build
git worktree list
git worktree add -b wa-build-3 /home/zakaria/projects/wa-relay-build-3
```

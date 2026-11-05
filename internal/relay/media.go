package relay

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MediaDownloader mengunduh lampiran sebuah pesan WhatsApp ke file lokal dan
// mengembalikan path-nya, supaya agent bisa membacanya.
type MediaDownloader interface {
	Download(chat, msgID, filename string) (string, error)
}

// WacliMediaDownloader mengunduh lampiran lewat `wacli media download`.
type WacliMediaDownloader struct {
	Bin string
	Dir string
}

func (d *WacliMediaDownloader) Download(chat, msgID, filename string) (string, error) {
	if strings.TrimSpace(d.Dir) == "" {
		return "", fmt.Errorf("media dir kosong")
	}
	if err := os.MkdirAll(d.Dir, 0o755); err != nil {
		return "", fmt.Errorf("buat media dir: %w", err)
	}
	out := filepath.Join(d.Dir, safeMediaName(msgID, filename))
	ctx, cancel := context.WithTimeout(context.Background(), mediaDownloadTmo)
	defer cancel()
	// --read-only wajib: `wacli sync --follow` memegang lock store selama
	// hidupnya, jadi tanpa flag ini unduhan selalu gagal "store is locked".
	// Dengan --read-only (dan --output eksplisit) unduhan tidak menyentuh
	// store dan tetap jalan saat sync aktif.
	if err := runCommand(ctx, d.Bin, "--read-only", "media", "download", "--chat", chat, "--id", msgID, "--output", out); err != nil {
		return "", err
	}
	if info, err := os.Stat(out); err != nil || info.IsDir() {
		return "", fmt.Errorf("file media tidak ditemukan setelah unduh: %s", out)
	}
	return out, nil
}

// prepareMedia mengunduh lampiran task dan menyusun teks prompt yang memuat
// path file lokal + metadatanya, supaya agent bisa membacanya dengan tools.
func (r *Relay) prepareMedia(item taskItem) (string, error) {
	if r.media == nil {
		return "", fmt.Errorf("pengunduh lampiran tidak aktif")
	}
	name := ""
	if item.Media != nil {
		name = item.Media.Filename
	}
	path, err := r.media.Download(item.Chat, item.ID, name)
	if err != nil {
		return "", err
	}
	return mediaPromptText(item, path), nil
}

func mediaPromptText(item taskItem, path string) string {
	var b strings.Builder
	if t := strings.TrimSpace(item.Text); t != "" {
		b.WriteString(t)
		b.WriteString("\n\n")
	}
	b.WriteString(mediaAttachmentLabel)
	b.WriteString("\nFile tersimpan di: ")
	b.WriteString(path)
	if mi := item.Media; mi != nil {
		if mi.Type != "" {
			fmt.Fprintf(&b, "\nJenis: %s", mi.Type)
		}
		if mi.MimeType != "" {
			fmt.Fprintf(&b, "\nMIME: %s", mi.MimeType)
		}
		if mi.Filename != "" {
			fmt.Fprintf(&b, "\nNama file: %s", mi.Filename)
		}
		if c := strings.TrimSpace(mi.Caption); c != "" {
			fmt.Fprintf(&b, "\nCaption: %s", c)
		}
	}
	return b.String()
}

// safeMediaName menyusun nama file dari id pesan + ekstensi nama aslinya,
// membuang karakter yang tidak aman dipakai sebagai nama file.
func safeMediaName(msgID, filename string) string {
	ext := filepath.Ext(filepath.Base(filename))
	base := strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', 0:
			return '_'
		}
		return r
	}, msgID)
	base = strings.TrimSpace(base)
	if base == "" {
		base = "media"
	}
	return base + ext
}

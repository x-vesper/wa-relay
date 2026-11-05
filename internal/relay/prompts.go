package relay

// File ini memusatkan semua template prompt yang dikirim ke agent opencode.
//
// PENTING (persona): prompt relay sengaja NETRAL dan tidak memaksakan sapaan
// ("bos", "Pak", dst). Persona stepi sendiri diatur oleh agent opencode
// (mis. ~/.config/opencode/agent/stepi.md). Kalau relay ikut menyebut "bos"
// atau "Pak", stepi bisa menirunya dan keluar dari karakter. Jadi biarkan
// stepi memakai sapaan/persona-nya sendiri.

// promptTemplate membungkus pesan WhatsApp masuk untuk lane chat. Dipakai oleh
// handleMessage sebelum dijalankan lewat `opencode run`.
const promptTemplate = "Pesan WhatsApp:\n---\n%s\n---\n" +
	"Balas sebagai dirimu sendiri, singkat dan natural (bahasa Indonesia santai/gaul). " +
	"Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode." +
	"\nKalau kamu perlu mengirim file/foto, tulis directive ini di baris sendiri (path harus absolut dan file-nya benar-benar ada): " +
	"[[file:/path/absolut/ke/file.jpg|caption opsional]]. Relay akan mengirimkannya sebagai lampiran WhatsApp. " +
	"Jangan tulis directive itu kalau tidak ada file yang mau dikirim."

// delegationPromptTemplate diteruskan ke stepi (agent chat) saat ada tugas
// berat: dia meringkas requirement dan meminta konfirmasi sebelum delegasi ke
// tim teknis. Hanya dipakai saat routing otomatis (auto_work_routing) aktif.
const delegationPromptTemplate = "Pesan WhatsApp:\n---\n%s\n---\n" +
	"Tugas ini terlihat teknis/berat. Sebagai asisten eksekutif:" +
	"\n1. Ringkaskan requirement-nya sebagai daftar angka (1, 2, 3, dst), bahasa Indonesia santai dan singkat." +
	"\n2. Akhiri dengan konfirmasi bahwa tugas ini akan didelegasikan ke tim teknis, contoh: \"Aku delegasikan tugas ini ke tim teknis, setuju?\"" +
	"\nJangan mulai pengerjaan sendiri, jangan janjikan hasil, cuma rangkumin requirement dan tanya setuju nggak." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// buildPromptTemplate diteruskan ke agent teknis (work_agent) untuk
// mengerjakan tugas.
const buildPromptTemplate = "Tugas teknis berikut dibawah tangan kamu. Selesaikan sebaik-baiknya (gunakan tools bila perlu)." +
	" Hasil akhir akan dirangkum dan dikirim lewat WhatsApp, jadi fokus pada pengerjaan, bukan format balasan." +
	"\n\nTugas: %s"

// summarizePromptTemplate diteruskan ke stepi setelah agent teknis selesai,
// agar hasil teknis dirangkum jadi balasan WhatsApp.
const summarizePromptTemplate = "Hasil pengerjaan tim teknis untuk tugas:\n---\n%s\n---\n" +
	"Rangkum hasilnya jadi balasan WhatsApp, bahasa Indonesia santai, singkat dan padat." +
	" Sampaikan status selesai + poin penting saja. Jangan terlalu teknis kecuali dibutuhkan." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// failurePromptTemplate diteruskan ke stepi saat worker gagal menjalankan
// tugas, supaya penyebabnya dijelaskan dengan konteks obrolan (bukan cuma
// tempelan error mentah).
const failurePromptTemplate = "Laporan dari tim teknis: tugas yang tadi didelegasikan GAGAL.\n" +
	"Tugas:\n---\n%s\n---\n" +
	"Detail error:\n---\n%s\n---\n" +
	"Sampaikan ke bos dengan singkat dan natural (bahasa Indonesia santai/gaul) bahwa tugasnya gagal. " +
	"Jelaskan penyebabnya kalau jelas (mis. limit kuota, timeout, error provider) dan kasih saran langkah berikutnya. " +
	"Jangan bilang tugasnya selesai. Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// handoverPromptTemplate diteruskan ke stepi saat bos memerintah menyerahkan
// tugas ke tim teknis. Stepi (yang punya konteks obrolan) merumuskan
// spesifikasi tugas yang jelas untuk tim teknis.
const handoverPromptTemplate = "Ada permintaan menyerahkan tugas ke tim teknis." +
	" Pesan:\n---\n%s\n---\n" +
	"Tuliskan spesifikasi tugas yang jelas dan ringkas untuk tim teknis berdasarkan konteks percakapan." +
	" Kalau detail tugasnya sudah disebut, pakai detail itu." +
	" Balas HANYA dengan spesifikasi tugasnya, tanpa basa-basi, tanpa judul markdown, tanpa fence kode."

// busyPromptTemplate diteruskan ke stepi saat semua slot build terpakai.
const busyPromptTemplate = "Semua slot tim teknis (build) sedang terpakai, jadi tugas berat ini belum bisa didelegasikan. " +
	"Sampaikan dengan singkat dan natural (bahasa Indonesia santai/gaul) bahwa tim lagi sibuk semua " +
	"dan tugasnya akan dikerjakan begitu ada yang kosong." +
	" Langsung ke balasan, tanpa proses berpikir, tanpa judul markdown, tanpa tabel, tanpa fence kode."

// busyFallback dipakai kalau stepi gagal menyusun pesan sibuk, supaya bos tetap
// mendapat kabar.
const busyFallback = "Tim lagi sibuk semua, nanti aku kabari ya."

// defaultHandoverAck dikirim begitu tugas diserahkan ke tim teknis (bila
// config handover_ack kosong). Momen serius -> sapaan "Pak" (lihat persona
// stepi).
const defaultHandoverAck = "Oke Pak, tugasnya aku serahin ke Pak Budi ya."

// mediaAttachmentLabel adalah penanda lampiran yang disisipkan ke prompt
// supaya agent tahu ada file lokal yang bisa dibaca.
const mediaAttachmentLabel = "[lampiran diterima]"

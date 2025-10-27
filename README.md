# Distributed Synchronization System (Tugas 2 SISTER)

**Nama:** Nabil Ahmed Savero

**NIM:** 11221041

Proyek ini adalah implementasi dari tiga sistem sinkronisasi terdistribusi yang berbeda, dibangun dalam satu basis kode Python (menggunakan `aiohttp` dan `redis`) dan di-deploy menggunakan Docker.

## Fitur (Core Requirements)

Sistem ini dapat di-boot dalam tiga mode berbeda dengan mengubah *environment variable* `NODE_TYPE` di `docker-compose.yml`:

### 1. Requirement A: Distributed Lock Manager
- **Mode:** `NODE_TYPE=lock_manager`
- **Algoritma:** Menggunakan algoritma konsensus **Raft** untuk memilih *leader* dan mereplikasi *state* *lock*.
- **Fitur:** Mendukung *exclusive lock* dan *shared lock*. *Request* *lock* harus dikirim ke *node* *Leader*.

### 2. Requirement B: Distributed Queue System
- **Mode:** `NODE_TYPE=queue`
- **Algoritma:** Menggunakan **Consistent Hashing** untuk mendistribusikan *topic* antrian ke *node-node* dalam *cluster*.
- **Fitur:** Mendukung `publish`, `consume` (dengan *consumer groups*), dan `acknowledge` (menggunakan **Redis Streams** untuk persistensi dan *at-least-once delivery*). *Request* bisa dikirim ke *node* mana saja dan akan di-*forward* jika perlu.

### 3. Requirement C: Distributed Cache Coherence
- **Mode:** `NODE_TYPE=cache`
- **Algoritma:** Mengimplementasikan protokol koherensi **MESI** (Modified, Exclusive, Shared, Invalid).
- **Fitur:** *Node-node* saling berkomunikasi (`bus_read`, `invalidate`) untuk memastikan data di *cache* lokal mereka tetap koheren.

## Dokumentasi Teknis

Dokumentasi teknis lengkap terdapat di dalam folder `docs/`:
- **`docs/architecture.md`**: Penjelasan arsitektur, diagram, dan algoritma (Raft, Consistent Hashing, MESI).
- **`docs/api_spec.yaml`**: Spesifikasi OpenAPI 3.0 untuk semua *endpoint* (`/lock`, `/queue`, `/cache`).
- **`docs/deployment_guide.md`**: Panduan cara menjalankan dan menguji setiap skenario menggunakan Docker Compose.

## Pengujian Performa

Pengujian performa (beban) dilakukan menggunakan **Locust**. *Script* pengujian dapat ditemukan di `benchmarks/load_test_scenarios.py`. Laporan lengkap hasil pengujian (RPS, Latency) tersedia di `LaporanTugas2.pdf`.

## Cara Menjalankan

Lihat `docs/deployment_guide.md` untuk instruksi lengkap menjalankan setiap skenario.
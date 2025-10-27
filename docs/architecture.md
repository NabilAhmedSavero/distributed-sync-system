# Arsitektur Sistem

Sistem ini dirancang sebagai cluster terdistribusi yang berjalan di dalam container Docker, terdiri dari 3 node aplikasi dan 1 node database Redis.

## Komponen Inti
- **Redis:** Berfungsi sebagai database terpusat untuk menyimpan *distributed state* (status *lock*, *log* Raft, *stream* antrian, dan memori utama *cache*).
- **Node Aplikasi (x3):** Tiga container (`node1`, `node2`, `node3`) yang identik, dibangun dari `Dockerfile.node` yang sama.
- **Jaringan:** Semua container terhubung melalui jaringan bridge Docker kustom (`dist_net`), yang memungkinkan mereka berkomunikasi menggunakan nama *service*.

## Struktur Node (BaseNode)
Setiap node adalah turunan dari `BaseNode`, yang menyediakan fungsionalitas dasar:
- **Server HTTP (aiohttp):** Berjalan di port unik (8001, 8002, 8003) untuk menerima *request* API eksternal dan RPC internal.
- **Klien HTTP (AsyncHttpClient):** Digunakan untuk mengirim RPC (`_send_rpc`) ke *node* lain.
- **Koneksi Redis:** Setiap *node* terhubung ke *instance* Redis yang sama.

## Diagram Arsitektur

![Diagram Arsitektur](images/sister.drawio.png)

Diagram di atas menunjukkan arsitektur infrastruktur dasar. Bergantung pada skenario pengujian, ketiga 'Aplikasi Node' ini akan memuat peran spesifik berdasarkan *environment variable* `NODE_TYPE`:

- **`NODE_TYPE=lock_manager`:** Membentuk *cluster* Raft untuk Requirement A.
- **`NODE_TYPE=queue`:** Membentuk *cluster* *consistent hashing* untuk Requirement B.
- **`NODE_TYPE=cache`:** Membentuk *cluster* dengan protokol koherensi MESI untuk Requirement C.

## Penjelasan Algoritma

### 1. Raft Consensus (Untuk Lock Manager)
Untuk menjamin konsistensi *lock*, sistem ini mengimplementasikan Raft.
- **Leader Election:** Node *follower* menjadi *candidate* setelah *timeout*, meminta *vote* (`/raft-vote`). Jika mendapat suara mayoritas, ia menjadi *leader* dan mengirim *heartbeat* (`/raft-append`).
- **Log Replication:** Perubahan *state* (misal `/lock/acquire`) dikirim sebagai *command* oleh *Leader* ke *Follower*. Setelah mayoritas mengonfirmasi (*committed*), *command* dieksekusi (`_apply_raft_command`) di setiap *node* untuk mengubah *state* di Redis.

### 2. Consistent Hashing (Untuk Queue System)
Untuk mendistribusikan *topic* antrian (Requirement B).
- **Distribusi Topic:** Ketiga *node* memetakan diri ke "cincin" *hash*. *Topic* di-*hash* ke cincin tersebut dan di-assign ke *node* terdekat.
- **Forwarding Request:** *Client* bisa mengirim *request* (misal `/queue/publish`) ke *node* mana saja. Jika *node* penerima bukan pemilik *topic*, *request* akan di-*forward* secara internal ke *node* yang benar.
- **Persistence:** Menggunakan Redis Streams (`XADD`, `XREADGROUP`, `XACK`) untuk menjamin *at-least-once delivery*.

### 3. Cache Coherence (MESI)
Untuk Requirement C, protokol MESI digunakan.
- **State:** Cache line lokal bisa *Modified*, *Exclusive*, *Shared*, atau *Invalid*.
- **Read Miss:** Node mengirim `bus_read` (`/cache/bus_read`) ke *peer*. Jika *peer* memiliki data (status `M`, `E`, atau `S`), ia akan membagikan data dan semua *copy* menjadi `Shared`.
- **Write Miss/Shared:** Node mengirim `invalidation` (`/cache/invalidate`) ke *peer*, memaksa *copy* mereka menjadi `Invalid`. Node kemudian menulis data ke *cache* lokal (status `Modified`) dan ke Redis (Main Memory).
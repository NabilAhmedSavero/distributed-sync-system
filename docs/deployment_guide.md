# Deployment Guide

Sistem ini dirancang untuk di-deploy menggunakan Docker dan Docker Compose.

## Prasyarat
- Docker
- Docker Compose
- `git` (untuk kloning repositori)

## Struktur Deployment
- `Dockerfile.node`: Membangun image aplikasi Python.
- `docker-compose.yml`: Mengorkestrasi 4 service: `redis`, `node1`, `node2`, `node3`.
- `dist_net`: Jaringan bridge kustom tempat semua container berkomunikasi.

## Cara Menjalankan (Per Skenario)

Konfigurasi utama diatur melalui *environment variable* `NODE_TYPE` di dalam `docker-compose.yml`.

### 1. Uji Requirement A (Lock Manager)
1.  Buka `docker/docker-compose.yml`.
2.  Atur `NODE_TYPE=lock_manager` untuk `node1`, `node2`, dan `node3`.
3.  Jalankan dari root proyek:
    ```bash
    docker-compose -f docker/docker-compose.yml up --build
    ```
4.  Amati log untuk melihat node mana yang memenangkan election (misal `[node2] won election...`).
5.  Kirim request API (misal `/lock/acquire`) ke **port leader** (misal `http://localhost:8002/lock/acquire`).

### 2. Uji Requirement B (Queue System)
1.  Buka `docker/docker-compose.yml`.
2.  Atur `NODE_TYPE=queue` untuk `node1`, `node2`, dan `node3`.
3.  Jalankan dari root proyek:
    ```bash
    docker-compose -f docker/docker-compose.yml up --build
    ```
4.  Kirim request API (misal `/queue/publish`) ke port node mana saja (misal `http://localhost:8001/queue/publish`).

### 3. Uji Requirement C (Cache Coherence)
1.  Buka `docker/docker-compose.yml`.
2.  Atur `NODE_TYPE=cache` untuk `node1`, `node2`, dan `node3`.
3.  Jalankan dari root proyek:
    ```bash
    docker-compose -f docker/docker-compose.yml up --build
    ```
4.  Kirim request API untuk menguji koherensi (misal `write` ke `8001`, lalu `read` dari `8002`).

### 4. Menjalankan Performance Testing (Locust)
1.  Pastikan cluster berjalan (misal untuk `lock_manager`).
2.  Pastikan `locust` terinstal di `venv` Anda (`pip install locust`).
3.  Tentukan *leader* (misal `node1` di `8001`).
4.  Buka terminal baru, aktifkan `venv`, dan `cd` ke folder `benchmarks/`.
5.  Jalankan Locust dengan *flag* `--host`:
    ```bash
    locust -f load_test_scenarios.py --host http://localhost:8001
    ```
6.  Buka `http://localhost:8089` di browser Anda.
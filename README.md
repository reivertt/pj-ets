# Pengumpulan Tugas 3

| Nama | NRP |
| --- | --- |
| Muhammad Nabil Akhtar Raya Amoriza | 5025221021 |

---

## Penambahan Dua Fungsi Utama:

### 1. Upload
#### Server
Sebagai endpoint untuk mengupload file ke server.  
Input: "POST {namafile} {isifile}"  
Output: Sebuah dict berisi status dan data keterangan

#### Client
Mengakses endpoint post pada server.  
Input: namafile  
Output: Boolean (success or not)

### 2. Delete
#### Server
Sebagai endpoint untuk menghapus file dari server.  
Input: "DELETE {namafile}"  
Output: Sebuah dict berisi status dan data keterangan
#### Client
Mengakses endpoint delete pada server.  
Input: namafile  
Output: Sebuah dict berisi status dan data keterangan

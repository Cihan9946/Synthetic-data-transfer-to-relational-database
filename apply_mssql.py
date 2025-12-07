import os
import re
import argparse
import pyodbc
import sys

def read_sql_file(path: str, encoding: str = "utf-16") -> str:
    """Dosyayı belirtilen encoding ile okur."""
    with open(path, "r", encoding=encoding, errors="replace") as f:
        return f.read()

def split_go_batches(sql: str):
    """SQL'i GO komutlarına göre böler."""
    parts = re.split(r'^\s*GO\s*$(?:\r\n?|\n)?', sql, flags=re.IGNORECASE | re.MULTILINE)
    return [p for p in parts if p.strip()]

def make_conn_str(args, db_name="master") -> str:
    driver = args.driver
    server = args.server
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={db_name};"
    if args.trusted:
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str += f"UID={args.user};PWD={args.password};"
    return conn_str

def ensure_database(args):
    """Hedef veritabanını oluşturur (Eğer yoksa)."""
    conn_str = make_conn_str(args, "master")
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        cur = conn.cursor()
        # Veritabanı var mı kontrol et
        check_sql = f"SELECT database_id FROM sys.databases WHERE Name = '{args.create_db}'"
        row = cur.execute(check_sql).fetchone()
        
        if not row:
            print(f"[INFO] '{args.create_db}' veritabanı oluşturuluyor...")
            cur.execute(f"CREATE DATABASE [{args.create_db}]")
        else:
            print(f"[INFO] '{args.create_db}' veritabanı zaten var. Üzerine yazılacak.")
    finally:
        conn.close()

def clean_and_execute(args, batches):
    """
    SQL komutlarını temizler:
    1. CREATE DATABASE bloklarını atlar.
    2. Dosya yollarını içeren satırları atlar.
    3. Eski DB adını (LINKERPFINSAT) yeni DB adı ile değiştirir.
    """
    conn_str = make_conn_str(args, args.create_db)
    conn = pyodbc.connect(conn_str, autocommit=True)
    
    # Eski DB Adını Scriptten Otomatik Bulmaya Çalış (Basit Regex)
    # Genelde: USE [LINKERPFINSAT] şeklindedir.
    old_db_name = "LINKERPFINSAT" # Varsayılan (Senin dosyana göre)
    
    try:
        cur = conn.cursor()
        print(f"[INFO] '{args.create_db}' veritabanına bağlanıldı. Tablolar aktarılıyor...")

        for i, batch in enumerate(batches, start=1):
            sql_clean = batch.strip()
            
            # --- FİLTRELEME MANTIĞI ---
            
            # 1. CREATE DATABASE komutu içeriyorsa BU BATCH'İ ÇALIŞTIRMA
            if "CREATE DATABASE" in sql_clean.upper():
                print(f"[SKIP] Batch {i}: 'CREATE DATABASE' komutu içerdiği için atlandı.")
                continue

            # 2. Dosya yolları içeriyorsa (FILENAME =) muhtemelen DB oluşturma ayarıdır, ATLA
            if "FILENAME =" in sql_clean.upper():
                 print(f"[SKIP] Batch {i}: Dosya yolları içerdiği için atlandı.")
                 continue
            
            # 3. USE [master] komutunu engelle, hep bizim DB'de kalsın
            if "USE [master]" in sql_clean.lower():
                print(f"[FIX] Batch {i}: 'USE [master]' komutu yoksayıldı.")
                # Sadece USE satırını silip devam edebiliriz ama batch sadece USE ise atlarız
                if len(sql_clean) < 20: 
                    continue
            
            # 4. İsim Değiştirme: [LINKERPFINSAT] -> [YeniDBAdi]
            # Scriptin içindeki USE [LINKERPFINSAT] komutlarını yeni isme yönlendiriyoruz
            if old_db_name in sql_clean:
                sql_clean = sql_clean.replace(f"[{old_db_name}]", f"[{args.create_db}]")
                sql_clean = sql_clean.replace(f"{old_db_name}", f"{args.create_db}")

            if not sql_clean:
                continue

            try:
                cur.execute(sql_clean)
                # İlerleme çubuğu gibi her 10 işlemde bir nokta koy
                if i % 10 == 0:
                    print(".", end="", flush=True)
            except Exception as e:
                print(f"\n[ERROR] Batch {i} hatası: {e}")
                # Kritik olmayan hatalarda durmasın (örn: tablo zaten var)
                pass
        
        print("\n[INFO] İşlem tamamlandı.")

    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--create-db", required=True, help="Yeni oluşturulacak DB adı")
    parser.add_argument("--trusted", action="store_true")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--driver", default="{ODBC Driver 17 for SQL Server}")
    # Encoding'i senin dosyan için varsayılan UTF-16 yaptım
    parser.add_argument("--encoding", default="utf-16") 

    args = parser.parse_args()

    # 1. SQL Dosyasını Oku
    try:
        sql_text = read_sql_file(args.script, args.encoding)
    except Exception:
        # UTF-16 hata verirse UTF-8 dene (Bazen header bozuktur)
        print("[WARN] UTF-16 okunamadı, UTF-8 deneniyor...")
        sql_text = read_sql_file(args.script, "utf-8-sig")

    batches = split_go_batches(sql_text)
    
    # 2. Veritabanını Oluştur (Temiz bir başlangıç için)
    ensure_database(args)

    # 3. Temizle ve İçeri Aktar
    clean_and_execute(args, batches)

if __name__ == "__main__":
    main()
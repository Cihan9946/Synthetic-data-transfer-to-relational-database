import sqlalchemy
from sqlalchemy import create_engine, text
from faker import Faker
import pandas as pd
import urllib
import random
import uuid
import logging
import time
from datetime import datetime
import re

# --- LOG AYARLARI ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# --- AYARLAR ---
ROW_COUNT = 15
DB_NAME = "LinkErpTest"
SERVER_NAME = "DESKTOP-OUI1G5B\\SQLEXPRESS"

# Atlanacak Tablolar
SKIP_TABLES = ['__EFMigrationsHistory', 'sysdiagrams', 'dtproperties']
# Atlanacak Kolonlar (Sistem kolonları)
SKIP_COLS = ['LogId', 'CreateDate', 'CreatedBy', 'UpdateDate', 'UpdatedBy']

# Güvenli Veri Tipleri (Bunun dışındakilere veri basmayız)
SAFE_TYPES = [
    'int', 'bigint', 'smallint', 'tinyint', 'bit', 
    'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real',
    'datetime', 'smalldatetime', 'date', 'time', 'datetime2',
    'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext',
    'uniqueidentifier'
]

TARGET_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DB_NAME};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

fake = Faker('tr_TR')
ID_CACHE = {}

# --- TÜRKÇE ERP SÖZLÜĞÜ ---
# Kolon adında bu kelimeler geçerse özel üretici kullanılır
KEYWORD_MAP = {
    'TCKN': lambda: str(random.randint(10000000000, 99999999999)),
    'VKN': lambda: str(random.randint(1000000000, 9999999999)),
    'VERGI': lambda: str(random.randint(1000000000, 9999999999)),
    'IBAN': lambda: fake.iban(),
    'MAIL': lambda: fake.email(),
    'EPOSTA': lambda: fake.email(),
    'TEL': lambda: "05" + str(random.randint(300000000, 599999999)),
    'GSM': lambda: "05" + str(random.randint(300000000, 599999999)),
    'UNVAN': lambda: fake.company(),
    'SIRKET': lambda: fake.company(),
    'AD': lambda: fake.first_name(),
    'SOYAD': lambda: fake.last_name(),
    'ADRES': lambda: fake.address().replace("\n", " ")[:100],
    'SEHIR': lambda: fake.city(),
    'IL': lambda: fake.city(),
    'ILCE': lambda: fake.city(), # Faker'da ilçe bazen tutmuyor, şehir basmak güvenli
    'ULKE': lambda: "Türkiye",
    'ACIKLAMA': lambda: fake.sentence(nb_words=5),
    'NOT': lambda: fake.sentence(nb_words=3),
    'BARKOD': lambda: fake.ean13(),
    'STOKADI': lambda: f"{random.choice(['Kırmızı', 'Mavi', 'Çelik', 'Ahşap', 'Lüks'])} {random.choice(['Masa', 'Sandalye', 'Vida', 'Laptop', 'Kablo'])}",
    'URUNADI': lambda: f"{random.choice(['Kırmızı', 'Mavi', 'Çelik', 'Ahşap', 'Lüks'])} {random.choice(['Masa', 'Sandalye', 'Vida', 'Laptop', 'Kablo'])}",
    'KOD': lambda: f"AUTO-{random.randint(1000, 9999)}",
    'FIYAT': lambda: round(random.uniform(10, 5000), 2),
    'TUTAR': lambda: round(random.uniform(10, 5000), 2),
    'MIKTAR': lambda: random.randint(1, 100),
    'WEB': lambda: fake.url(),
    'URL': lambda: fake.url()
}

def get_engine():
    params = urllib.parse.quote_plus(TARGET_CONN_STR)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", connect_args={'timeout': 10})

def get_table_info(conn, table_name):
    """Tablo kolonlarını ve tiplerini güvenli şekilde çeker"""
    sql = text("""
        SELECT 
            c.COLUMN_NAME, 
            c.DATA_TYPE, 
            c.IS_NULLABLE, 
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            COLUMNPROPERTY(object_id(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity,
            COLUMNPROPERTY(object_id(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsComputed') AS IsComputed
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME = :t_name
    """)
    try:
        result = conn.execute(sql, {"t_name": table_name}).fetchall()
        columns = {}
        for row in result:
            col_type = str(row[1]).lower()
            if col_type not in SAFE_TYPES: continue # Güvenli olmayan tipleri baştan ele

            columns[row[0]] = {
                "name": row[0],
                "type": col_type,
                "nullable": row[2] == 'YES',
                "length": row[3],
                "precision": row[4],
                "scale": row[5],
                "is_identity": row[6] == 1,
                "is_computed": row[7] == 1
            }
        return columns
    except Exception as e:
        logger.error(f"   ⚠️ {table_name} bilgisi okunamadı: {e}")
        return {}

def get_fk_map(conn):
    """Hangi tablo kime bağlı? Foreign Key haritasını çıkarır."""
    sql = text("""
        SELECT 
            OBJECT_NAME(f.parent_object_id) AS TableName,
            COL_NAME(fc.parent_object_id,fc.parent_column_id) AS ColumnName,
            OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName
        FROM sys.foreign_keys AS f
        INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
    """)
    fk_map = {} # {'StokHareket': {'StokId': 'Stok'}}
    try:
        result = conn.execute(sql).fetchall()
        for row in result:
            tbl, col, ref = row
            if tbl not in fk_map: fk_map[tbl] = {}
            fk_map[tbl][col] = ref
    except: pass
    return fk_map

def fetch_ids(conn, table_name):
    try:
        # PK bul
        pk_sql = text(f"SELECT TOP 1 c.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME JOIN INFORMATION_SCHEMA.COLUMNS c ON c.TABLE_NAME = ccu.TABLE_NAME AND c.COLUMN_NAME = ccu.COLUMN_NAME WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = :t_name")
        res = conn.execute(pk_sql, {"t_name": table_name}).fetchone()
        pk_col = res[0] if res else "Id"
        
        sql = text(f"SELECT TOP 1000 [{pk_col}] FROM [{table_name}] WITH (NOLOCK)")
        res = conn.execute(sql).fetchall()
        ID_CACHE[table_name] = [r[0] for r in res]
    except:
        ID_CACHE[table_name] = []

def generate_smart_value(col_name, col_info, fk_ref_table):
    # 1. Foreign Key ise (Öncelikli)
    if fk_ref_table:
        if fk_ref_table in ID_CACHE and ID_CACHE[fk_ref_table]:
            return random.choice(ID_CACHE[fk_ref_table])
        # İlişki var ama veri yoksa, veri tipine göre uydur (Fallback)
        if 'uniqueidentifier' in col_info['type']: return str(uuid.uuid4())
        return random.randint(1, 10)

    col_name_upper = col_name.upper()
    col_type = col_info['type']

    # 2. Türkçe Keyword Kontrolü
    for key, generator in KEYWORD_MAP.items():
        if key in col_name_upper:
            val = generator()
            # Eğer kolon sayısal ama biz string ürettiysek (örn TCKN), int'e çevir
            if 'int' in col_type or 'decimal' in col_type:
                try: return int(val)
                except: pass
            return val

    # 3. Veri Tipine Göre Standart Üretim
    if 'bit' in col_type: return random.choice([0, 1])
    
    if 'int' in col_type or 'tinyint' in col_type or 'smallint' in col_type:
        limit = 255 if 'tinyint' in col_type else 32000 if 'smallint' in col_type else 100000
        return random.randint(0, limit)
        
    if 'decimal' in col_type or 'numeric' in col_type or 'money' in col_type:
        prec = col_info['precision'] or 18
        scale = col_info['scale'] or 2
        max_val = (10 ** (prec - scale)) - 1
        return round(random.uniform(0, min(max_val, 10000)), scale)
        
    if 'date' in col_type or 'time' in col_type:
        return datetime.now()
        
    if 'uniqueidentifier' in col_type:
        return str(uuid.uuid4())
        
    # String / Text
    length = col_info['length'] or 50
    if length == -1: length = 100 # MAX
    
    # Kelime mi cümle mi?
    if length < 10: return fake.lexify('????')
    if length < 50: return fake.word().title()
    return fake.sentence(nb_words=5)[:length]

def main():
    engine = get_engine()
    
    # Global FK Haritasını Çıkar
    with engine.connect() as conn:
        logger.info("🔗 İlişki haritası (FK) çıkarılıyor...")
        FK_MAP = get_fk_map(conn)
        
        # Tablo listesi (Bağımlılık sırasına gerek yok, retry mantığı ile çözeceğiz veya ID check ile)
        # Basit olsun diye DB'deki sırayla alıp, ID'leri buldukça kullanacağız.
        tables_res = conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")).fetchall()
        all_tables = [r[0] for r in tables_res]

    logger.info(f"🚀 {len(all_tables)} tablo için Türkçe veri üretimi başlıyor...")

    for i, table in enumerate(all_tables, 1):
        if any(x in table for x in SKIP_TABLES) or 'AspNet' in table: continue
        
        # Tek tek her tablo için işlem (Transaction per table)
        try:
            with engine.begin() as conn:
                # 1. Kilitleri Aç (Sadece bu tablo için değil global açmak daha güvenli ama burada connection bazlı)
                conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'"))
                conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? DISABLE TRIGGER all'"))

                # 2. Kolonları Analiz Et
                col_infos = get_table_info(conn, table)
                if not col_infos: continue
                
                # 3. Parent ID Hazırlığı (Bu tablonun FK'ları kim?)
                my_fks = FK_MAP.get(table, {})
                for col, parent in my_fks.items():
                    if parent not in ID_CACHE: fetch_ids(conn, parent)

                data_list = []
                for _ in range(ROW_COUNT):
                    row = {}
                    for col, info in col_infos.items():
                        if info['is_identity'] or info['is_computed']: continue
                        if col in SKIP_COLS: continue
                        
                        # FK Referansı var mı?
                        fk_ref = my_fks.get(col)
                        
                        val = generate_smart_value(col, info, fk_ref)
                        
                        # String Kırpma (Güvenlik)
                        if 'char' in info['type'] and isinstance(val, str) and info['length'] > 0:
                            val = val[:info['length']]
                            
                        row[col] = val
                    data_list.append(row)
                
                if data_list:
                    df = pd.DataFrame(data_list)
                    df.to_sql(table, conn, if_exists='append', index=False)
                    logger.info(f"✅ ({i}/{len(all_tables)}) {table}: {len(df)} kayıt basıldı.")
                else:
                    logger.warning(f"⚠️ ({i}/{len(all_tables)}) {table}: Veri üretilemedi.")
                
                # Bitiş: ID'leri hafızaya al (Diğer tablolar kullansın)
                fetch_ids(conn, table)

        except Exception as e:
            err = str(e).split(']')[0]
            logger.error(f"❌ {table}: {err}")
            
    # En son kilitleri kapat
    try:
        with engine.begin() as conn:
            logger.info("🔒 Sistem kilitleri kapatılıyor...")
            conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT all'"))
            conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? ENABLE TRIGGER all'"))
    except: pass
    
    logger.info("🏁 İŞLEM TAMAMLANDI.")

if __name__ == "__main__":
    main()
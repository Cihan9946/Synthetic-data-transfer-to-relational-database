import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from faker import Faker
import networkx as nx
import random
import uuid
from datetime import datetime, timedelta
import urllib

# --- AYARLAR ---

# Senin verdiğin ham bağlantı cümlesi
RAW_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-OUI1G5B\\SQLEXPRESS;"
    "DATABASE=LinkErpTest;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "MultipleActiveResultSets=True;"
)

# Her tabloya kaç satır basılsın?
ROW_COUNT = 10 

fake = Faker('tr_TR')  # Türkçe veri üretmesi için

def get_engine():
    # SQLAlchemy'nin bu stringi tanıması için URL encode yapıyoruz
    params = urllib.parse.quote_plus(RAW_CONN_STR)
    # mssql+pyodbc protokolü ile odbc_connect parametresini birleştiriyoruz
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(conn_str)

def disable_constraints(conn):
    print("🔓 Tüm Trigger ve Constraint'ler devre dışı bırakılıyor...")
    # SQL Server'da sp_msforeachtable kullanarak tüm tabloları geziyoruz
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'"))
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? DISABLE TRIGGER all'"))

def enable_constraints(conn):
    print("🔒 Tüm Trigger ve Constraint'ler tekrar açılıyor...")
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT all'"))
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? ENABLE TRIGGER all'"))

def get_sorted_tables(engine):
    """Tabloları bağımlılık sırasına göre (Parent -> Child) sıralar."""
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    
    G = nx.DiGraph()
    G.add_nodes_from(table_names)
    
    print("🕸️  Tablo ilişkileri analiz ediliyor...")
    for table in table_names:
        fks = inspector.get_foreign_keys(table)
        for fk in fks:
            parent = fk['referred_table']
            if parent != table:
                G.add_edge(parent, table)
    
    try:
        ordered = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        print("⚠️ Döngüsel ilişki tespit edildi, alfabetik sıraya yakın işlem yapılacak.")
        ordered = table_names
        
    return ordered

def generate_value(column_info):
    """Kolon tipine göre rastgele ama mantıklı veri üretir."""
    col_type = str(column_info['type']).upper()
    col_name = column_info['name']
    
    # 1. UUID / GUID
    if 'UNIQUEIDENTIFIER' in col_type:
        return str(uuid.uuid4())
    
    # 2. Sayısal Değerler
    elif 'INT' in col_type or 'SMALLINT' in col_type or 'TINYINT' in col_type:
        # Kod veya ID gibi duruyorsa pozitif olsun
        return random.randint(1, 1000)
    
    # 3. Boolean
    elif 'BIT' in col_type:
        return random.choice([0, 1])
    
    # 4. Tarih / Saat
    elif 'DATE' in col_type or 'TIME' in col_type:
        return datetime.now() - timedelta(days=random.randint(0, 365))
    
    # 5. Ondalıklı Sayılar (Para vb.)
    elif 'DECIMAL' in col_type or 'NUMERIC' in col_type or 'REAL' in col_type or 'FLOAT' in col_type:
        return round(random.uniform(10, 5000), 2)
    
    # 6. Metin (String)
    elif 'CHAR' in col_type or 'TEXT' in col_type:
        # Özel alan isimlerine göre mantıklı veri üretme
        upper_name = col_name.upper()
        
        if 'MAIL' in upper_name:
            return fake.email()
        if 'TEL' in upper_name or 'GSM' in upper_name:
            return fake.phone_number()[:14]
        if 'ADRES' in upper_name:
            return fake.address()[:100]
        if 'AD' in upper_name and 'SOYAD' not in upper_name: # İsim
            return fake.first_name()
        if 'SOYAD' in upper_name:
            return fake.last_name()
        if 'TCKN' in upper_name or 'VKN' in upper_name:
            return str(random.randint(10000000000, 99999999999))
        if 'VERGIDAIRESI' in upper_name:
            return fake.city() + " V.D."
            
        # Uzunluk kontrolü
        length = getattr(column_info['type'], 'length', 50)
        if length is None: length = 50
        
        if length > 20:
            text_val = fake.text(max_nb_chars=length)
        else:
            text_val = fake.lexify('????')
            
        return text_val[:length]
        
    return None

def fill_tables():
    engine = get_engine()
    
    try:
        # Bağlantıyı test et
        with engine.connect() as conn:
            pass
        print("✅ Veritabanı bağlantısı başarılı.")
    except Exception as e:
        print(f"❌ Bağlantı hatası: {e}")
        return

    inspector = inspect(engine)
    
    # 1. Tabloları İlişki Sırasına Göre Diz
    sorted_tables = get_sorted_tables(engine)
    
    with engine.begin() as conn:
        # 2. Trigger ve Constraintleri Kapat
        disable_constraints(conn)
        
        print(f"\n🚀 {len(sorted_tables)} tablo doldurulmaya başlanıyor...")
        
        for table_name in sorted_tables:
            # sysdiagrams vb. sistem tablolarını atla
            if 'sys' in table_name or 'Migration' in table_name:
                continue

            try:
                columns = inspector.get_columns(table_name)
                data_to_insert = []
                
                for _ in range(ROW_COUNT):
                    row = {}
                    for col in columns:
                        # Otomatik artan (Identity) kolonlara değer gönderme
                        if col.get('autoincrement', False):
                            continue
                            
                        val = generate_value(col)
                        if val is not None:
                            row[col['name']] = val
                    data_to_insert.append(row)
                
                if data_to_insert:
                    df = pd.DataFrame(data_to_insert)
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"✅ {table_name}: {len(df)} satır eklendi.")
                    
            except Exception as e:
                # Hata mesajını kısaltarak göster
                print(f"❌ {table_name} hatası: {str(e).split(']')[0]}")
        
        # 3. İşlem Bitince Korumaları Geri Aç
        enable_constraints(conn)
        print("\n🏁 İşlem Tamamlandı!")

if __name__ == "__main__":
    fill_tables()
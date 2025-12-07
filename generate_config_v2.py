import json
import os

# --- AYARLAR ---
INPUT_FILE = "schema (2).jsonl"
OUTPUT_FILE = "data_rules.json"

def detect_provider(col_data, table_name):
    """
    JSONL'deki kolon verisine (açıklama, ad, tip) bakarak Faker kuralı belirler.
    """
    col_name = col_data.get('column_name', '').upper()
    description = col_data.get('description_tr', '').lower() # Türkçe açıklama
    data_type = col_data.get('data_type', '').lower()
    
    # 1. Foreign Key Kontrolü (Caller fonksiyonda FK listesi varsa oradan gelecek ama burada isimden tahmin)
    # JSONL yapısında foreign_keys tablonun üst seviyesinde tanımlı, burada kolon bazlı bakıyoruz.
    # Bu yüzden ID kelimesi geçenleri ve INT/GUID olanları potansiyel ID olarak işaretleyelim.
    if ('ID' in col_name or 'KOD' in col_name) and table_name.upper() not in col_name:
         # Eğer kolon adı tablo adıyla aynı değilse (örn: Banka tablosunda BankaId değilse)
         # muhtemelen başka tabloya referanstır.
         if 'INT' in data_type: return "random_int:1,100"
         if 'UNIQUEIDENTIFIER' in data_type: return "uuid4"

    # 2. Açıklama (Description) Bazlı Akıllı Tespit
    if 'telefon' in description or 'gsm' in description: return "phone_number"
    if 'email' in description or 'e-posta' in description: return "email"
    if 'adres' in description: return "address"
    if 'iban' in description: return "iban"
    if 'tc kimlik' in description or 'tckn' in description: return "numerify:###########"
    if 'vergi no' in description or 'vkn' in description: return "numerify:##########"
    if 'şehir' in description or 'il ' in description: return "city"
    if 'ülke' in description: return "country"
    if 'tarih' in description: return "date_this_decade"
    if 'fiyat' in description or 'tutar' in description or 'bakiye' in description: 
        return "pyfloat:right_digits=2,positive=True,min_value=10,max_value=50000"
    if 'miktar' in description: return "random_int:1,1000"
    if 'şirket' in description or 'firma' in description or 'unvan' in description: return "company"
    if 'ad ' in description and 'soyad' not in description: return "first_name"
    if 'soyad' in description: return "last_name"
    if 'açıklama' in description or 'not' in description: return "sentence:10"
    
    # 3. Kolon Adı Bazlı Yedek Kontrol (Açıklama yetersizse)
    if 'TEL' in col_name: return "phone_number"
    if 'MAIL' in col_name: return "email"
    if 'BARKOD' in col_name: return "ean13"
    if 'VERGIDAIRESI' in col_name: return "city_suffix" # Şehir ismi + V.D. mantığı engine tarafında yoksa city basar
    if 'DURUM' in col_name or 'AKTIF' in col_name: return "boolean"
    if 'PARA' in col_name or 'DOVIZ' in col_name: return "currency_code"
    
    # 4. Veri Tipine Göre Varsayılanlar
    if 'bit' in data_type or 'boolean' in data_type: return "boolean"
    if 'date' in data_type or 'time' in data_type: return "date_this_decade"
    if 'int' in data_type or 'smallint' in data_type: return "random_int:0,100"
    if 'decimal' in data_type or 'numeric' in data_type or 'float' in data_type:
        return "pyfloat:right_digits=2,positive=True"
    if 'uniqueidentifier' in data_type: return "uuid4"
    
    # Hiçbiri değilse rastgele kelime
    return "word"

def generate_config():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ '{INPUT_FILE}' bulunamadı!")
        return

    config = {}
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                table_data = json.loads(line)
                table_name = table_data.get('table_name')
                columns = table_data.get('columns', [])
                foreign_keys = table_data.get('foreign_keys', [])
                
                if not table_name: continue
                
                config[table_name] = {}
                
                # Önce Foreign Keyleri İşle (En garantisi budur)
                fk_map = {fk['column']: fk['references'].split('.')[0] for fk in foreign_keys if 'references' in fk}
                
                for col in columns:
                    col_name = col.get('column_name')
                    
                    # Eğer bu kolon FK listesindeyse
                    if col_name in fk_map:
                        parent_table = fk_map[col_name]
                        config[table_name][col_name] = f"foreign_key:{parent_table}"
                    else:
                        # Değilse akıllı tespit yap
                        config[table_name][col_name] = detect_provider(col, table_name)
                        
            except json.JSONDecodeError:
                print("⚠️ Satır okuma hatası, geçiliyor.")
                continue

    # JSON Kaydet
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
    
    print(f"✅ Kurallar '{INPUT_FILE}' dosyasındaki açıklamalara göre oluşturuldu -> {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_config()
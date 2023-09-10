import os
import time
import concurrent.futures
import psycopg2
from lxml import etree


def process_xml_file(dosya_adi, db_connection, processed_count, start_time):
    dosya_yolu = os.path.join(klasor_yolu, dosya_adi)
    try:
        parser = etree.XMLParser(recover=True)  # Hata bulunan XML dosyalarını işlemek için recover=True kullan
        tree = etree.parse(dosya_yolu, parser=parser)
        root = tree.getroot()

        rows = []  # Her dosyadan gelen satırları saklamak için bir liste oluştur

        for release in root.xpath(".//release"):
            release_id = release.get("id")
            status = release.get("status")
            title = release.findtext("title") or ""
            artist_id = release.findtext(".//artist/id") or ""
            artist_name = release.findtext(".//artist/name") or ""
            label_elem = release.find(".//labels/label")
            if label_elem is not None:
                label_name = label_elem.get("name")
                label_id = label_elem.get("id")
            else:
                label_name = ""
                label_id = ""

            format_elem = release.find(".//formats/format")
            if format_elem is not None:
                format = format_elem.get("name")
            else:
                format = ""

            genre = release.findtext(".//genres/genre") or ""
            style = release.findtext(".//styles/style") or ""
            country = release.findtext("country") or ""
            release_date = release.findtext("released") or ""
            notes = release.findtext("notes") or ""

            master_id_elem = release.find(".//master_id[@is_main_release='true']")
            if master_id_elem is not None:
                master_id = master_id_elem.text
            else:
                master_id = ""

            video_elem = release.find(".//videos/video")
            if video_elem is not None:
                video_url = video_elem.get("src")
            else:
                video_url = ""

            company_name_elem = release.find(".//companies/company/name")
            if company_name_elem is not None:
                company_name = company_name_elem.text
            else:
                company_name = ""

            # Tarihleri yıl olarak sakla
            release_date_str = release_date.split('-')[0]
            if len(release_date_str) == 4 and release_date_str.isdigit():
                release_date = int(release_date_str)
            else:
                release_date = None

            # Her satırı bir sözlük olarak sakla
            row = {
                "release_id": release_id, "status": status, "title": title, "artist_id": artist_id,
                "artist_name": artist_name, "label_name": label_name, "label_id": label_id,
                "format": format, "genre": genre, "style": style, "country": country,
                "release_date": release_date, "notes": notes, "master_id": master_id,
                "video_url": video_url, "company_name": company_name
            }
            rows.append(row)

        # Veritabanına verileri ekle
        insert_data_to_db(db_connection, rows)

        # Dosya işlendiğinde geçen süreyi hesapla
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(
            f"{processed_count} dosya işlendi - Geçen süre: {int(hours)} saat, {int(minutes)} dakika, {int(seconds)} saniye")

        return True
    except Exception as e:
        print(f'Hata: {dosya_adi} işlenirken bir hata oluştu: {str(e)}')
        return False


def insert_data_to_db(db_connection, rows):
    try:
        cursor = db_connection.cursor()
        for row in rows:
            # SQL ekleme sorgusu oluştur ve çalıştır
            sql = "INSERT INTO discogs (release_id, status, title, artist_id, artist_name, label_name, label_id, format, genre, style, country, release_date, notes, master_id, video_url, company_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (
            row["release_id"], row["status"], row["title"], row["artist_id"], row["artist_name"], row["label_name"],
            row["label_id"], row["format"], row["genre"], row["style"], row["country"], row["release_date"],
            row["notes"], row["master_id"], row["video_url"], row["company_name"]))
        db_connection.commit()
        cursor.close()
    except Exception as e:
        print(f'Hata: Veritabanına veri eklenirken bir hata oluştu: {str(e)}')


# PostgreSQL veritabanı bağlantısı oluştur
db_connection = psycopg2.connect(
    host="localhost",
    database="discogs",
    user="postgres",
    password="******"
)

try:
    cursor = db_connection.cursor()

    # Veritabanında oluşturmak istediğim tablonun yapısını tanımla
    create_table_query = """
    CREATE TABLE IF NOT EXISTS discogs (
        id SERIAL PRIMARY KEY,
        release_id VARCHAR(255),
        status VARCHAR(255),
        title TEXT,
        artist_id VARCHAR(255),
        artist_name TEXT,
        label_name TEXT,
        label_id VARCHAR(255),
        format TEXT,
        genre TEXT,
        style TEXT,
        country VARCHAR(255),
        release_date VARCHAR(4),
        notes TEXT,
        master_id VARCHAR(255),
        video_url TEXT,
        company_name TEXT
    )
    """

    # Tabloyu oluştur
    cursor.execute(create_table_query)
    db_connection.commit()
    print("Tablo oluşturuldu.")

except Exception as e:
    print(f'Hata: Tablo oluşturulurken bir hata oluştu: {str(e)}')

# İşlem yapmak istediğim klasörün yolu
klasor_yolu = 'chunked'

# Klasördeki XML dosyalarını listele
dosya_listesi = [dosya for dosya in os.listdir(klasor_yolu) if dosya.endswith('.xml')]

# Dosya sayacı
dosya_sayaci = 0

start_time = time.time()  # İşlem süresi başlangıcı

# Paralel işlem için ThreadPoolExecutor kullan
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []

    for dosya_adi in dosya_listesi:
        dosya_sayaci += 1
        print(f"{dosya_sayaci}. dosya işleniyor...")

        # İşlemi paralel olarak başlatın
        future = executor.submit(process_xml_file, dosya_adi, db_connection, dosya_sayaci, start_time)
        futures.append(future)

    processed_count = 0  # İşlenen dosya sayısını takip etmek için sayaç

    for future in concurrent.futures.as_completed(futures):
        if future.result():
            processed_count += 1

# Veritabanı bağlantısını kapat
db_connection.close()

# İşlem süresini ve dosya sayısını yazdır
print(f"Toplam {dosya_sayaci} XML dosyası işlendi.")
elapsed_time = time.time() - start_time
hours, remainder = divmod(elapsed_time, 3600)
minutes, seconds = divmod(remainder, 60)
print(f"Toplam geçen süre: {int(hours)} saat, {int(minutes)} dakika, {int(seconds)} saniye")

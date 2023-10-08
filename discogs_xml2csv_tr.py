import os
import polars as pl
from lxml import etree
import time
import concurrent.futures


def process_xml_file(dosya_adi):
    dosya_yolu = os.path.join(klasor_yolu, dosya_adi)
    try:
        parser = etree.XMLParser(recover=True)  # Hata bulunan XML dosyalarını işlemek için recover=True kullanın
        tree = etree.parse(dosya_yolu, parser=parser)
        root = tree.getroot()

        rows = []  # Her dosyadan gelen satırları saklamak için bir liste oluşturun

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

            # Her satırı bir sözlük olarak saklayın
            row = {
                "release_id": release_id, "status": status, "title": title, "artist_id": artist_id,
                "artist_name": artist_name, "label_name": label_name, "label_id": label_id,
                "format": format, "genre": genre, "style": style, "country": country,
                "release_date": release_date, "notes": notes, "master_id": master_id,
                "video_url": video_url, "company_name": company_name
            }
            rows.append(row)

        return rows
    except Exception as e:
        print(f'Hata: {dosya_adi} işlenirken bir hata oluştu: {str(e)}')
        return None


def print_processed_count(processed_count):
    print(f"{processed_count} dosya işlendi...")


# İşlem yapmak istediğiniz klasörün yolu
klasor_yolu = 'chunked'

# Klasördeki XML dosyalarını listeleyin
dosya_listesi = [dosya for dosya in os.listdir(klasor_yolu) if dosya.endswith('.xml')]

# Dosya sayacı
dosya_sayaci = 0

# İşlem süre sayaçları
toplam_islem_suresi = 0

# Verileri saklamak için bir liste oluşturun
data_list = []

# Paralel işlem için ThreadPoolExecutor kullanın
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []

    for dosya_adi in dosya_listesi:
        dosya_sayaci += 1
        print(f"{dosya_sayaci}. dosya işleniyor...")

        baslangic_zamani = time.time()  # İşlem süresi başlangıcı

        # İşlemi paralel olarak başlatın
        future = executor.submit(process_xml_file, dosya_adi)
        futures.append(future)

    processed_count = 0  # İşlenen dosya sayısını takip etmek için sayaç

    # İşlem tamamlandığında işlenen dosya sayısını hızlıca takip etmek için kullanılacak bir set oluşturun
    processed_set = set()

    for future in concurrent.futures.as_completed(futures):
        rows = future.result()
        if rows is not None:
            # Liste içindeki her satırı data_list'e ekleyin
            data_list.extend(rows)
            processed_count += 1
            processed_set.add(processed_count)  # İşlenen dosya sayısını ekleyin
            print_processed_count(processed_count)  # İşlenen dosya sayısını ekrana yazdırın

# data_list'i bir Polars DataFrame'e dönüştürün
df = pl.DataFrame(data_list)

# Polars DataFrame'i bir CSV dosyasına kaydedin
df.write_csv('discogs.csv')

# İşlem süresini ve dosya sayısını yazdırın
print(f"Toplam {dosya_sayaci} XML dosyası işlendi.")
print(f"Toplam işlem süresi: {toplam_islem_suresi:.2f} saniye")


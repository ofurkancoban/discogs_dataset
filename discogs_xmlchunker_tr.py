import os
import time

def main():
    start_time = time.time()
    album_counter = 0
    record_counter = 0
    output_folder = "chunked"
    output_file = None
    records_per_file = 10000  # Her parçada oluşturulacak kayıt sayısı
    file_counter = 0  # Oluşturulan dosya sayısı

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open("discogs_20230601_releases.xml", "r", encoding="utf-8") as data_file:
        for line in data_file:
            if "<release id=" in line:
                album_counter += 1
                if record_counter % records_per_file == 0:
                    if output_file:
                        output_file.write(f"</root>\n")
                        output_file.close()
                        file_counter += 1
                        end_time = time.time()
                        elapsed_time = end_time - start_time
                        print(f"{file_counter} Dosya Oluşturuldu: chunk_{file_counter}.xml, Toplam İşlenen Albüm Sayısı: {album_counter}, Geçen Süre: {elapsed_time/60:.0f} dakika")
                    album_filename = os.path.join(output_folder, f"chunk_{file_counter}.xml")
                    output_file = open(album_filename, "w", encoding="utf-8")
                    output_file.write(f"<root>\n")
                record_counter += 1
            if output_file:
                output_file.write(line)

    if output_file:
        output_file.write(f"</root>\n")
        output_file.close()
        file_counter += 1
        print(f"Dosya Oluşturuldu: album_{file_counter}.xml")
        print(f"Toplam İşlenen Albüm Sayısı: {album_counter}")
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Geçen Süre: {elapsed_time/60:.0f} dakika")
        print(f"Çıkan Dosya Sayısı: {file_counter}")

if __name__ == "__main__":
    main()

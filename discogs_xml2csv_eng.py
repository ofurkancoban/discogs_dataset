import os
import polars as pl
from lxml import etree
import time
import concurrent.futures


def process_xml_file(file_name):
    file_path = os.path.join(folder_path, file_name)
    try:
        parser = etree.XMLParser(recover=True)  # Use recover=True to process XML files with errors
        tree = etree.parse(file_path, parser=parser)
        root = tree.getroot()

        rows = []  # Create a list to store rows from each file

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

            # Store each row as a dictionary
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
        print(f'Error: An error occurred while processing {file_name}: {str(e)}')
        return None


def print_processed_count(processed_count):
    print(f"{processed_count} files processed...")


# Path to the folder you want to process
folder_path = 'chunked'

# List XML files in the folder
file_list = [file for file in os.listdir(folder_path) if file.endswith('.xml')]

# File counter
file_counter = 0

# Elapsed time counters
total_elapsed_time = 0

# Create a list to store data
data_list = []

# Use ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []

    for file_name in file_list:
        file_counter += 1
        print(f"Processing file {file_counter}...")

        start_time = time.time()  # Start time of processing

        # Start processing in parallel
        future = executor.submit(process_xml_file, file_name)
        futures.append(future)

    processed_count = 0  # Counter to track processed files

    # Create a set to quickly track the processed file count when processing is completed
    processed_set = set()

    for future in concurrent.futures.as_completed(futures):
        rows = future.result()
        if rows is not None:
            # Append each row in the list to data_list
            data_list.extend(rows)
            processed_count += 1
            processed_set.add(processed_count)  # Add the processed file count
            print_processed_count(processed_count)  # Print the processed file count to the screen

# Convert data_list to a Polars DataFrame
df = pl.DataFrame(data_list)

# Write the Polars DataFrame to a CSV file
df.write_csv('discogs.csv', overwrite=True)

# Print the processing time and file count
print(f"Total {file_counter} XML files processed.")
print(f"Total processing time: {total_elapsed_time:.2f} seconds")

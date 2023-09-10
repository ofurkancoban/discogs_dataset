import os
import time
import concurrent.futures
import psycopg2
from lxml import etree


def process_xml_file(file_name, db_connection, processed_count, start_time):
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

            # Store dates as years
            release_date_str = release_date.split('-')[0]
            if len(release_date_str) == 4 and release_date_str.isdigit():
                release_date = int(release_date_str)
            else:
                release_date = None

            # Store each row as a dictionary
            row = {
                "release_id": release_id, "status": status, "title": title, "artist_id": artist_id,
                "artist_name": artist_name, "label_name": label_name, "label_id": label_id,
                "format": format, "genre": genre, "style": style, "country": country,
                "release_date": release_date, "notes": notes, "master_id": master_id,
                "video_url": video_url, "company_name": company_name
            }
            rows.append(row)

        # Add data to the database
        insert_data_to_db(db_connection, rows)

        # Calculate the elapsed time when the file is processed
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(
            f"{processed_count} files processed - Elapsed time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

        return True
    except Exception as e:
        print(f'Error: An error occurred while processing {file_name}: {str(e)}')
        return False


def insert_data_to_db(db_connection, rows):
    try:
        cursor = db_connection.cursor()
        for row in rows:
            # Create and execute the SQL insertion query
            sql = "INSERT INTO discogs (release_id, status, title, artist_id, artist_name, label_name, label_id, format, genre, style, country, release_date, notes, master_id, video_url, company_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (
            row["release_id"], row["status"], row["title"], row["artist_id"], row["artist_name"], row["label_name"],
            row["label_id"], row["format"], row["genre"], row["style"], row["country"], row["release_date"],
            row["notes"], row["master_id"], row["video_url"], row["company_name"]))
        db_connection.commit()
        cursor.close()
    except Exception as e:
        print(f'Error: An error occurred while inserting data into the database: {str(e)}')


# Create a PostgreSQL database connection
db_connection = psycopg2.connect(
    host="localhost",
    database="discogs",
    user="postgres",
    password="******"
)

try:
    cursor = db_connection.cursor()

    # Define the structure of the table you want to create in the database
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

    # Create the table
    cursor.execute(create_table_query)
    db_connection.commit()
    print("Table created.")

except Exception as e:
    print(f'Error: An error occurred while creating the table: {str(e)}')

# Path to the folder you want to process
folder_path = 'chunked'

# List XML files in the folder
file_list = [file for file in os.listdir(folder_path) if file.endswith('.xml')]

# File counter
file_counter = 0

start_time = time.time()  # Start time of processing

# Use ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []

    for file_name in file_list:
        file_counter += 1
        print(f"Processing file {file_counter}...")

        # Start processing in parallel
        future = executor.submit(process_xml_file, file_name, db_connection, file_counter, start_time)
        futures.append(future)

    processed_count = 0  # Counter to track processed files

    for future in concurrent.futures.as_completed(futures):
        if future.result():
            processed_count += 1

# Close the database connection
db_connection.close()

# Print the processing time and file count
print(f"Total {file_counter} XML files processed.")
elapsed_time = time.time() - start_time
hours, remainder = divmod(elapsed_time, 3600)
minutes, seconds = divmod(remainder, 60)
print(f"Total elapsed time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

![Discogs](https://github.com/ofurkancoban/discogs_dataset/blob/main/img/discogs.png)
# "Vinyl Enthusiasm Meets Big Data: Converting 70 GB-sized Discogs Dataset into CSV with Python”

**"Discogs is a platform that music enthusiasts, especially vinyl collectors, are well-acquainted with, standing as one of the world's largest music databases. This platform encompasses a vast range of information related to music, from artists to albums, from vinyls to cassettes. Often, in my deep dives into my vinyl collection, I've knocked on the doors of Discogs. Thus, when I had the opportunity to access the massive dataset named "releases" on Discogs, embarking on this project was not just a technical challenge for me, but also a source of personal excitement."**

---

## **📌** Accessing the Data Set

Discogs periodically updates and releases its "releases" data set, which contains rich information about the world of music. To take advantage of this continuously updated data source, I downloaded the latest update from [this URL](https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2023/discogs_20230601_releases.xml.gz) in .gz format.

After the download process, I unzipped the file using an archive manager, revealing a massive XML file of 70 GB. Handling such an extensive data set was going to be an exciting experience. It also implied that I needed detailed planning and strategy development for the subsequent steps. Now, I was ready to deeply analyze this data set and embark on the task of converting it into a more accessible format.

---

## **📌** Inspecting the Dataset with EmEditor:

Before diving into the process of chunking, it was crucial to get a preliminary understanding of the XML data structure and its scope. For this purpose, I turned to [EmEditor](https://www.emeditor.com/download/), a high-performance text editor adept at handling extremely large files. Opening our 70 GB XML file with EmEditor, I was able to smoothly navigate through its contents without any lags or memory strains.

It was particularly beneficial to gauge the number of records present. A preliminary scan revealed the sheer magnitude of the dataset – displaying numerous "release" tags signifying each unique album release on Discogs. EmEditor's advanced search functionalities aided in counting these "release" tags, giving me a clear idea of the total number of records in the XML file.

Having a precise count was vital as it informed my chunking strategy – ensuring I create XML chunks that are both manageable in size and meaningful in content.

![EmEditor](C:\Users\ofurkancoban\Desktop\Discogs Project\img\1.gif)

---

## 📌Chunking the Massive XML Dataset

Working with vast amounts of data often means confronting challenges related to the efficient processing and analysis of that data. The Discogs 'Releases' XML data set, with its sizable 70 GB content, is one such mammoth that requires a thoughtful approach to manage.

### ➡️ Objective

**Our goal is to divide the colossal XML file into more manageable chunks, with each chunk containing a specified number of 'release' records. We aim to achieve this without loading the entire file into memory, to avoid memory errors.**

### ➡️ Setting Up

Before diving into the code, we define some initial parameters.

```python
start_time = time.time()
album_counter = 0
record_counter = 0
output_folder = "chunked"
output_file = None
records_per_file = 10000  # Number of records to create in each chunk
file_counter = 0  # Number of files created
```

Here, we're initializing counters and setting the number of records per output file to 10,000.

### ➡️ Preparing the Output Directory

Ensuring the output directory exists is our next step. If it doesn't, we'll create one.

```python
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
```

---

## 📌 Reading and Processing the XML File

Handling massive XML files demands a meticulous approach, especially when one aims to process the data without overburdening the system memory. Our strategy involves reading the XML file line by line and writing the data to smaller XML chunks based on a predetermined number of 'release' records.

### ➡️ Opening the File

We commence by opening our large XML file with read permissions. Utilizing Python's with statement ensures that the file is properly closed after processing, reducing the risk of file corruption.

```python
with open("discogs_20230601_releases.xml", "r", encoding="utf-8") as data_file:
```

### ➡️ Iterating Through the File

As we iterate over each line of the file, our primary lookout is the <release id= tag, signifying the start of a new album record.

```python
for line in data_file:
    if "<release id=" in line:
        album_counter += 1
```

Each time we encounter this tag, the album_counter is incremented, keeping track of the total number of albums we've processed.

### ➡️ Chunking the Data

The heart of our operation lies in creating manageable chunks of the XML data. Once our running record_counter matches our predetermined records_per_file (set to 10,000 in our script), it's time to start a new chunk.

```python
if record_counter % records_per_file == 0:
    if output_file:  # Finalizing the current file if it exists
        output_file.write(f"</root>\n")
        output_file.close()
    # Preparing for the new chunk
    file_counter += 1
    album_filename = os.path.join(output_folder, f"chunk_{file_counter}.xml")
    output_file = open(album_filename, "w", encoding="utf-8")
    output_file.write(f"<root>\n")
```

During this process, we close any currently open output file and start a new one. We also ensure that each new file starts with a root tag, ensuring the resulting XML is well-formed.

### ➡️ Writing the Data

After preparing our output file, we continuously write each line from the original XML into our smaller chunk until the next splitting condition is met.

```python
if output_file:
    output_file.write(line)
```

Through this approach, we ensure that each chunk is a standalone, well-formed XML file.

## 📌 Wrapping Up

After meticulously iterating through the entire XML file and distributing the data into smaller chunks, it's essential to ensure that all resources are released properly and that we have a summary of the work accomplished.

### ➡️ Finalizing the Last Chunk

First and foremost, after the iteration is completed, there might still be data in the current chunk that hasn't reached the threshold for a new chunk but still needs to be closed properly.

```python
if output_file:
    output_file.write(f"</root>\n")
    output_file.close()
```

In the code above, we close the XML root tag of our chunk. The root tag ensures the XML's structural integrity. After this, we close the file to release the system resources.

### ➡️ Statistics and Summary

It's beneficial to have a summary at the end of such a process, giving insights into the work that has been done. Therefore, we print out some essential statistics.

```python
print(f"Number of Output Files: {file_counter}")
print(f"Total Processed Album Count: {album_counter}")
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time/60:.0f} minutes")
```

Here, we're detailing:

The total number of output XML chunks (file_counter).

The total number of album records processed (album_counter).

The total time taken for the script to run, which is calculated by deducting the start_time from the current time (end_time), and then displaying this elapsed time in minutes.

With these final steps, our script not only ensures that the data is processed efficiently but also provides a concise summary of its operations. This combination of precision processing and feedback makes the script robust and user-friendly.

![Chunking Process](C:\Users\ofurkancoban\Desktop\Discogs Project\img\2.gif)

### ➡️ Conclusion

With this Python script, we've successfully transformed a behemoth XML file into manageable chunks, ready for subsequent analysis or processing. This approach demonstrates the importance of strategic data management, especially when dealing with large-scale data sets.

---

---

## 📌 Handling Chunked XML Data: Parsing and Transformation

After the painstaking process of dividing our gigantic XML data into consumable chunks, we now direct our focus on parsing these XML chunks and transforming them into a structured format.

### ➡️ Setting Up the XML Parser

Given the possibility of inconsistencies or errors in such large datasets, we resort to the lxml library for XML parsing, which provides the flexibility to deal with malformed XML content:

```python
parser = etree.XMLParser(recover=True)  # Use recover=True to process XML files with errors
tree = etree.parse(file_path, parser=parser)
```

### ➡️ Traversing the XML Structure

Navigating XML structures requires understanding the underlying hierarchy and the relationships between tags. In our case, the XML data is centered around the "release" tag, making it the cornerstone for our data extraction.

### 💠 Root Element and Release Tag

At the core of our XML data, the "release" tag serves as the primary data entity. Before diving into individual "release" tags, we first establish a connection with the root of the XML file:

```python
root = tree.getroot()
```

After connecting with the root, we loop through each "release" tag:

```python
for release in root.xpath(".//release"):
```

This line effectively locates all instances of the "release" tag nested within our XML.

### 💠 Extracting Data from Nested Tags

Within each "release", there are nested tags, each holding specific details. For instance:

To get the release ID, we use:

```python
release_id = release.get("id")
```

To retrieve the title of the release, the code is:

```python
title = release.findtext("title") or ""
```

The "or" clause ensures that if a title isn't found, we default to an empty string, preserving data integrity.

### 💠 Handling Complex Nested Structures

Sometimes, our data may reside deeper, requiring us to traverse multiple nested tags. Consider extracting information about an artist associated with a release:

```python
artist_id = release.findtext(".//artist/id") or ""
artist_name = release.findtext(".//artist/name") or ""
```

Here, we've dived into the "artist" tag nested within "release" and extracted both the artist's ID and name.

### 💠 Handling Optional Tags

In large datasets, some tags might be optional, appearing in some "releases" but not in others. To ensure our code doesn't break, we must account for these scenarios. Taking the "label" tag as an example:

```python
label_elem = release.find(".//labels/label")
if label_elem is not None:
    label_name = label_elem.get("name")
    label_id = label_elem.get("id")
else:
    label_name = ""
    label_id = ""
```

The conditional check ensures we handle cases where the "label" tag might be absent.

Traversing XML structures necessitates a blend of meticulousness and foresight, especially when dealing with complex nested hierarchies. With the correct approach, as demonstrated, even intricate XML data can be navigated with precision and ease.

### 💠 Parallel Processing with ThreadPoolExecutor

Dealing with numerous XML chunks and striving for efficiency calls for parallel processing. With the ThreadPoolExecutor, we can process multiple XML chunks in parallel, ensuring faster processing.

### ➡️ Consolidating Data

After parsing individual XML chunks, it's crucial to bring together the extracted data into a single, unified structure for subsequent operations or analyses. Here's a deep dive into how we consolidated our data:

### 💠 Accumulating Data from Chunks

Every chunked XML file is processed separately, producing a list of dictionaries, where each dictionary represents a 'release'. As each file gets processed, these lists are accumulated into a master list (data_list):

```python
data_list.extend(rows)
```

Here, rows represents the list of dictionaries obtained from processing a single XML chunk. The extend method efficiently appends the contents of rows to our data_list.

> Why not just append?
> 

Appending would add the rows list as a single element in the data_list. By using extend, we ensure each dictionary in rows becomes an individual element in data_list.

### 💠 Parallel Processing Impact

Given the use of concurrent.futures.ThreadPoolExecutor for parallel processing, the order of processing and completing XML chunks is not guaranteed. However, the beauty of the data consolidation process here is its order-agnostic nature. Whether the 1st XML chunk finishes processing before the 2nd or after the 10th, the data_list will consistently capture all data:

```python
for future in concurrent.futures.as_completed(futures):
    rows = future.result()
    if rows is not None:
        data_list.extend(rows)
```

The above snippet iterates over completed futures (processed XML chunks) and consolidates the data.

> Benefits of Consolidation:
> 
- Unified Data: Aggregating the data from individual chunks into a single structure offers a holistic view of the entire dataset.
- Ease of Analysis: With all the data in one place, various operations such as filtering, aggregation, and analysis become straightforward.
- Flexibility for Export: Consolidation ensures the data is primed for export to different formats (like CSV, SQL, etc.) or to be fed into different systems or applications.

### ➡️ Transforming Data into DataFrame with Polars

When dealing with large datasets, efficiency is crucial. Python’s go-to library, Pandas, is undoubtedly powerful, but when it comes to processing massive amounts of data, it can run into memory constraints and performance issues. That's where Polars comes into play.

I chose Polars for this project for several compelling reasons:

- **Memory Efficiency:** Polars is designed to be memory-efficient, which makes it especially suitable for handling enormous datasets like ours without compromising performance.
- **Speed:** Polars leverages Arrow Arrays, which allows it to perform operations incredibly fast. Its lazy evaluation mechanism ensures computations are only executed when needed, further optimizing performance.
- **Easy Integration:** Polars seamlessly integrates with existing Python code, making the transition from other libraries smooth and straightforward.
- **Concise Syntax:** Polars offers an intuitive and concise syntax, making data manipulation tasks straightforward.

By utilizing Polars, I was able to transform the extracted data into structured DataFrames effortlessly and, most importantly, without running into memory issues.

```python
df = pl.DataFrame(data_list)
```

The beauty of Polars lies in its ability to handle data at scale while maintaining simplicity and speed, a combination that proved invaluable for this project.

![XML Processing](C:\Users\ofurkancoban\Desktop\Discogs Project\img\3.gif)

### ➡️ Exporting to CSV

The final stage of our endeavor is to store our processed data. With the data comfortably resting in a DataFrame, exporting it to a CSV is seamless:

```python
df.write_csv('discogs.csv')
```

---

## 📌A Final Note for Large Data Handling

After the entire process, we arrived at a raw dataset comprising a staggering 16 million rows. Such a colossal amount of data holds immense potential for various analyses and research endeavors.

While transforming and exporting the dataset into CSV, some might run into memory issues due to the sheer size of the dataset. To mitigate this, I've crafted a script that pushes the dataset into a database first and then facilitates exporting from there. This approach is more memory-efficient and should overcome common memory challenges.

I have personally uploaded the script to my GitHub repository for those who wish to take this approach. You can access it [here](https://github.com/ofurkancoban/discogs_dataset).

Moreover, for the convenience of data enthusiasts and to foster further exploration, I've also made the datasets available on my Kaggle profile. You can dive into this vast sea of data by visiting [this Kaggle link](https://www.kaggle.com/datasets/ofurkancoban/discogs-releases-dataset).

As we wrap up this exploration, I genuinely hope that this dataset serves as a valuable resource in your future projects. There's a myriad of insights waiting to be discovered, and I'm excited to see the innovative ways in which this data might be employed by the community.

Thank you for accompanying me on this journey. Here's to leveraging data for greater insights and creativity in all our endeavors!ct

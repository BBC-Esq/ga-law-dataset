# Harvard's Dataset Creation Script

This script is what harvard originally used to create the dataset located at:

https://huggingface.co/datasets/harvard-lil/cold-cases

The original ```export.py``` script is located here but the repository was shuttered in October, 2024:

https://github.com/harvard-lil/cold-cases-export/blob/main/cold_cases_export/export.py

<details><summary>AI Summary of Harvard's Script</summary></summary>
Certainly! Let's break down the provided PySpark script to understand how it processes multiple `.csv.bz2` files, extracts relevant information, joins different datasets, and ultimately creates a comprehensive dataset. Here's a step-by-step explanation:

## **1. Overview**

The script is designed to:

1. **Read Compressed CSV Files**: Specifically, `.csv.bz2` files containing various legal data.
2. **Convert to Parquet Format**: For optimized storage and faster processing.
3. **Process and Clean Data**: Extract relevant columns, perform type casting, and clean textual data.
4. **Join Multiple DataFrames**: Combine different datasets based on common keys.
5. **Create Final Dataset**: Consolidate all information into a single dataset, which is then saved in Parquet and JSONL formats.

## **2. Reading and Converting CSV.bz2 Files**

### **Function: `parquetify`**

- **Purpose**: Converts the latest `.csv.bz2` file with a given nickname in the specified directory to a Parquet file.
- **Process**:
  1. **Find Latest File**: Uses `find_latest` to identify the most recent file based on lexicographical order (relying on ISO date formatting within filenames).
  2. **Check Parquet Existence**: If the corresponding Parquet file doesn't exist, it reads the CSV and writes it as Parquet.
  3. **Read Parquet**: Returns a DataFrame by reading the Parquet file.

- **CSV Options**:
  - `header`: `true` (indicates that the first line contains column headers).
  - `multiLine`: `true` (handles multi-line fields).
  - `quote` and `escape`: Set to handle quoted strings properly.

### **Function: `find_latest`**

- **Purpose**: Identifies the latest file based on prefix and extension.
- **Process**:
  1. **List Directory**: Retrieves all files in the specified directory.
  2. **Filter Files**: Selects files that start with the given prefix and end with the specified extension.
  3. **Sort and Select**: Sorts the filtered list lexicographically and picks the last one (assumed to be the latest).

## **3. Processing Individual DataFrames**

The script defines separate functions to process different types of data. Each function selects relevant columns, casts data types, performs data cleaning, and drops unnecessary columns.

### **A. Opinions Data**

#### **Function: `get_opinions`**

- **Input**: DataFrame from `opinions.csv.bz2`.
- **Extracted Columns**:
  - **Selected Columns**: `id`, `author_str`, `per_curiam`, `type`, `page_count`, `download_url`, `plain_text`, `html`, `html_lawbox`, `html_columbia`, `html_anon_2020`, `xml_harvard`, `html_with_citations`, `extracted_by_ocr`, `author_id`, `cluster_id`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on essential columns for opinions.
  2. **Determine Opinion Text**:
     - **Priority Order**: `html_with_citations` > `plain_text` > `html` > `html_lawbox` > `html_columbia` > `xml_harvard` > `html_anon_2020`.
     - **Use `coalesce`**: Selects the first non-null value based on priority.
     - **Clean Text**: Removes HTML tags using `regexp_replace`.
  3. **Type Casting**:
     - `page_count`: Cast to `IntegerType`.
     - `opinion_cluster_id`, `author_id`, `opinion_id`: Cast to `IntegerType`.
  4. **Boolean Columns**:
     - `ocr`: Converts `"t"` to `True`, else `False`.
     - `per_curiam`: Converts `"t"` to `True`, else `False`.
  5. **Drop Unnecessary Columns**: Removes `extracted_by_ocr`, `cluster_id`, and all text columns used in `opinion_text`.

- **Final Columns**:
  - `author_str`, `per_curiam`, `type`, `page_count`, `download_url`, `author_id`, `opinion_text`, `ocr`, `opinion_id`.

### **B. Opinion Clusters Data**

#### **Function: `get_opinion_clusters`**

- **Input**: DataFrame from `opinion-clusters.csv.bz2`.
- **Extracted Columns**:
  - **Selected Columns**: `id`, `judges`, `date_filed`, `date_filed_is_approximate`, `slug`, `case_name_short`, `case_name`, `case_name_full`, `attorneys`, `nature_of_suit`, `posture`, `syllabus`, `headnotes`, `summary`, `disposition`, `history`, `other_dates`, `cross_reference`, `correction`, `citation_count`, `precedential_status`, `blocked`, `docket_id`, `arguments`, `headmatter`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on essential columns for opinion clusters.
  2. **Filter Blocked Records**: Keeps only records where `blocked` is `"f"` (false).
  3. **Type Casting**:
     - `date_filed`: Cast to `DateType`.
     - `date_filed_is_approximate`: Converts `"t"` to `True`, else `False`.
     - `citation_count`: Cast to `IntegerType`.
     - `id`: Cast to `IntegerType`.
  4. **Clean Text**: Removes HTML tags from `summary`.
  5. **Rename Columns**:
     - `docket_id` to `opinion_cluster_docket_id`.
  6. **Drop Unnecessary Columns**: Removes `blocked` and `docket_id`.

- **Final Columns**:
  - `judges`, `date_filed`, `date_filed_is_approximate`, `slug`, `case_name_short`, `case_name`, `case_name_full`, `attorneys`, `nature_of_suit`, `posture`, `syllabus`, `headnotes`, `summary`, `disposition`, `history`, `other_dates`, `cross_reference`, `correction`, `citation_count`, `precedential_status`, `arguments`, `headmatter`, `opinion_cluster_docket_id`.

### **C. Citations Data**

#### **Function: `get_citations`**

- **Input**: DataFrame from `citations.csv.bz2`.
- **Extracted Columns**:
  - **Selected Columns**: `id`, `volume`, `reporter`, `page`, `cluster_id`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on essential columns for citations.
  2. **Create Citation Text**: Concatenates `volume`, `reporter`, and `page` with spaces (e.g., "123 ABC 456").
  3. **Type Casting**:
     - `citation_cluster_id`: Cast `cluster_id` to `IntegerType`.
     - `id`: Cast to `IntegerType`.
  4. **Drop Unnecessary Columns**: Removes `volume`, `reporter`, `page`, `cluster_id`.

- **Final Columns**:
  - `citation_text`, `citation_cluster_id`, `id`.

### **D. Dockets Data**

#### **Function: `get_dockets`**

- **Input**: DataFrame from `dockets.csv.bz2`.
- **Extracted Columns**:
  - **Selected Columns**: `id`, `court_id`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on essential columns for dockets.
  2. **Type Casting**:
     - `id`: Cast to `IntegerType`.
  3. **Rename Columns**:
     - `court_id` to `docket_court_id`.

- **Final Columns**:
  - `id`, `docket_court_id`.

### **E. Courts Data**

#### **Function: `get_courts`**

- **Input**: DataFrame from `courts.csv.bz2`.
- **Extracted Columns**:
  - **Selected Columns**: `id`, `short_name`, `full_name`, `jurisdiction`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on essential columns for courts.
  2. **Rename Columns**:
     - `short_name` to `court_short_name`.
     - `full_name` to `court_full_name`.
     - `jurisdiction` to `court_type`.
     - `id` to `court_id`.
  3. **Drop Original `id`**: Removes the original `id` column after renaming.

- **Final Columns**:
  - `court_short_name`, `court_full_name`, `court_type`, `court_id`.

### **F. Court Information Data**

#### **Function: `get_court_info`**

- **Input**: `court-info.csv` from the data directory.
- **Extracted Columns**:
  - **Selected Columns**: `court_full_name`, `jurisdiction`.
  
- **Processing Steps**:
  1. **Select Relevant Columns**: Focuses on court names and their jurisdictions.
  2. **Rename Columns**:
     - `court_full_name` to `court_info_full_name`.
     - `jurisdiction` to `court_jurisdiction`.

- **Final Columns**:
  - `court_info_full_name`, `court_jurisdiction`.

## **4. Joining DataFrames**

### **Function: `group`**

- **Purpose**: Joins all processed DataFrames to create a unified dataset.
- **Input DataFrames**:
  - `citations`
  - `opinions`
  - `opinion_clusters`
  - `dockets`
  - `courts`
  - `court_info`

- **Joining Steps**:

  1. **Join Courts with Court Info**:
     - **Condition**: `court_info.court_info_full_name` == `courts.court_full_name`.
     - **Result**: Adds `court_jurisdiction` to the `courts` DataFrame.
     - **Drop**: `court_info_full_name` to avoid duplication.

  2. **Join Dockets with Courts and Court Info**:
     - **Condition**: `dockets.docket_court_id` == `courts.court_id`.
     - **Join Type**: Left join (to retain all dockets).
     - **Result**: Combines docket information with court details.
     - **Rename**: `id` to `dockets_and_courts_id` for clarity.

  3. **Aggregate Citations**:
     - **Group By**: `citation_cluster_id`.
     - **Aggregation**: Collect all `citation_text` into a list (`citations`).
     - **Result**: Each citation cluster has an array of citation texts.

  4. **Aggregate Opinions**:
     - **Group By**: `opinion_cluster_id`.
     - **Aggregation**: Collect all relevant opinion columns into a list of structs (`opinions`).
     - **Result**: Each opinion cluster has an array of opinion details.

  5. **Join Citations with Opinion Clusters**:
     - **Condition**: `opinion_clusters.id` == `citations_arrays.citation_cluster_id`.
     - **Join Type**: Left join (to include clusters without citations).
     - **Result**: Adds `citations` array to opinion clusters.
     - **Rename**: Ensures the citations column is appropriately named.

  6. **Join Opinions with Opinion Clusters**:
     - **Condition**: `opinion_clusters.id` == `opinions_arrays.opinion_cluster_id`.
     - **Join Type**: Left join (to include clusters without opinions).
     - **Result**: Adds `opinions` array to opinion clusters.
     - **Rename**: Ensures the opinions column is appropriately named.

  7. **Join with Dockets and Courts**:
     - **Condition**: `dockets_and_courts.dockets_and_courts_id` == `opinion_clusters.opinion_cluster_docket_id`.
     - **Join Type**: Left join (to retain all opinion clusters).
     - **Result**: Combines docket and court information with opinion clusters.

  8. **Final Cleanup**:
     - **Drop Unnecessary Columns**: Removes intermediate keys used for joining to maintain cleanliness.

- **Final Columns in the Joined DataFrame**:
  - All columns from `opinion_clusters` (e.g., `judges`, `date_filed`, etc.)
  - `citations` (array of citation texts)
  - `opinions` (array of opinion details)
  - Court and docket information (e.g., `court_short_name`, `court_full_name`, `court_jurisdiction`, etc.)

## **5. Running the Data Processing Pipeline**

### **Function: `run`**

- **Purpose**: Orchestrates the entire data processing workflow.
- **Process**:
  1. **Initialize Spark Session**: Configures Spark with necessary settings.
  2. **Prepare Data Directory Path**: Ensures the directory path does not end with a slash.
  3. **Process Each Data Type**:
     - **Citations**: `get_citations(parquetify(..., "citations"))`
     - **Opinions**: `get_opinions(parquetify(..., "opinions"))`
     - **Opinion Clusters**: `get_opinion_clusters(parquetify(..., "opinion-clusters"))`
     - **Courts**: `get_courts(parquetify(..., "courts"))`
     - **Dockets**: `get_dockets(parquetify(..., "dockets"))`
     - **Court Info**: `get_court_info(...)`
  4. **Join DataFrames**: Uses the `group` function to combine all processed DataFrames.
  5. **Optimize and Save Final Dataset**:
     - **Explain Plan**: Prints the execution plan for debugging/optimization.
     - **Write to Parquet**: Saves the final dataset as `cold.parquet` with GZIP compression, split into `OUTPUT_FILES` (32) for parallelism.
     - **Convert to JSONL**: Reads the Parquet file and writes it as `cold.jsonl` with GZIP compression.
  6. **Terminate Spark Session**: Closes the Spark session gracefully.

## **6. Final Dataset Structure**

The final dataset (`cold.parquet` and `cold.jsonl`) contains a comprehensive view of legal opinions and related information. Here's an outline of the key columns and their descriptions:

- **Opinion Cluster Details**:
  - `id`: Unique identifier for the opinion cluster.
  - `judges`: Judges involved in the case.
  - `date_filed`: Date when the opinion was filed.
  - `date_filed_is_approximate`: Boolean indicating if the filing date is approximate.
  - `slug`, `case_name_short`, `case_name`, `case_name_full`: Various representations of the case name.
  - `attorneys`: Attorneys involved in the case.
  - `nature_of_suit`, `posture`, `syllabus`, `headnotes`, `summary`, `disposition`, `history`, `other_dates`, `cross_reference`, `correction`, `arguments`, `headmatter`: Detailed case information.
  - `citation_count`: Number of citations in the case.
  - `precedential_status`: Whether the case is precedential.
  
- **Citations**:
  - `citations`: Array of citation texts (e.g., ["123 ABC 456", "789 DEF 012"]).

- **Opinions**:
  - `opinions`: Array of opinion details, each containing:
    - `author_str`: Author's name as a string.
    - `per_curiam`: Boolean indicating if the opinion is per curiam.
    - `type`: Type of opinion.
    - `page_count`: Number of pages in the opinion.
    - `download_url`: URL to download the opinion.
    - `author_id`: Identifier for the author.
    - `opinion_text`: Cleaned text of the opinion.
    - `ocr`: Boolean indicating if the opinion was extracted via OCR.
    - `opinion_id`: Unique identifier for the opinion.

- **Court and Docket Information**:
  - `court_short_name`, `court_full_name`, `court_jurisdiction`, `court_type`: Details about the court handling the case.
  - `dockets_and_courts_id`: Identifier linking to docket and court information.

## **7. Summary of Data Extraction and Joining**

1. **Data Extraction**:
   - **Multiple CSV.bz2 Files**: The script processes multiple compressed CSV files (`citations.csv.bz2`, `opinions.csv.bz2`, `opinion-clusters.csv.bz2`, `dockets.csv.bz2`, `courts.csv.bz2`, and `court-info.csv`).
   - **Parquet Conversion**: Each CSV is converted to Parquet for efficiency.

2. **Data Cleaning and Transformation**:
   - **Selecting Relevant Columns**: Each dataset extracts only the necessary columns.
   - **Type Casting**: Ensures data types are appropriate (e.g., integers, dates, booleans).
   - **Text Cleaning**: Removes HTML tags and other unwanted characters from textual data.
   - **Aggregations**: Groups citations and opinions into arrays for each opinion cluster.

3. **Data Joining**:
   - **Common Keys**:
     - `cluster_id`: Links citations to opinion clusters.
     - `opinion_cluster_id`: Links opinions to opinion clusters.
     - `court_id` and `docket_court_id`: Links dockets to courts.
     - `court_info_full_name`: Links court information to courts.
   - **Join Operations**: Combines datasets based on these keys to enrich the final dataset with comprehensive information.

4. **Final Dataset Creation**:
   - **Unified Structure**: Merges all relevant information into a single DataFrame.
   - **Output Formats**: Saves the final dataset in both Parquet (`cold.parquet`) and JSONL (`cold.jsonl`) formats with GZIP compression for storage efficiency.

## **8. Conclusion**

This script effectively orchestrates the extraction, transformation, and loading (ETL) of legal data from multiple compressed CSV files. By converting to Parquet, it optimizes storage and processing speed. The meticulous selection, cleaning, and joining of various datasets result in a rich, unified dataset that can be used for further analysis, reporting, or machine learning applications.

If you have any specific questions about particular sections or need further clarification on any part of the script, feel free to ask!
</details>

The problem is, Harvard stopped creating the dataset at a certain point in time and subsequently changed somewhat the structure of the data dump such that the ```export.py``` no longer works with the more recent .bz2/.csv data files.

For example, ```export.py``` relies on a file named ```court-info.csv``` (look at line 182) but the data dumps located at https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/ don't include this specific file...

With that being said, my investigation so far indicates that the information contained in the former ```court-info.csv``` is subsumed into one of the other .bz2/.csv files...but I'm having trouble determining which one.  If/when I do that, it'd be relatively easy to create a new "dataset" with the same information as Harvard's...which has the most crucial info for cases.

# My Script to Investigave where the missing .csv file information went:

The above script named ```analyze_schema_syntax3.py``` analyzes and pinpoints the structure of the .csv files (i.e. after extracted from a .bz2 file).  It outputs something like this:

```
Inspecting file: D:\Scripts\Scrape_Caselaw\FLP Bulk Data\parentheticals-2024-10-31.csv
--------------------------------------------------

Header Analysis:
Number of header fields: 6

Quote Character Usage in Data:
'`' appears 48 times
'"' appears 8 times
''' appears 1 times

File Structure Conclusion:
------------------------
- Detected delimiter: ','
- Detected quote character: '`'
- Fields may contain delimiters within quoted values

Recommended Dask/Pandas Read Settings:
python
df = dd.read_csv(file_path,
               delimiter=',',
               quotechar='`',
               escapechar='\\',
               on_bad_lines='skip')

Attempting to read column headers using detected settings...

Successfully read columns:
1. id
2. text
3. score
4. described_opinion_id
5. describing_opinion_id
6. group_id
```

I used this script to try and determine where the data from ```court-info.csv``` might have been moved to to, the goal being to use a modified version of Harvard's ```export.py``` if possible to save time.

Here are all the columns/structure from all the .csv files but I'm not sure I'm tracing it correctly...

```
"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\citation-map-2024-10-31.csv"

1. id
2. depth
3. cited_opinion_id
4. citing_opinion_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\citations-2024-10-31.csv"

1. id
2. volume
3. reporter
4. page
5. type
6. cluster_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\courts-2024-10-31.csv"

1. id
2. pacer_court_id
3. pacer_has_rss_feed
4. pacer_rss_entry_types
5. date_last_pacer_contact
6. fjc_court_id
7. date_modified
8. in_use
9. has_opinion_scraper
10. has_oral_argument_scraper
11. position
12. citation_string
13. short_name
14. full_name
15. url
16. start_date
17. end_date
18. jurisdiction
19. notes
20. parent_court_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\dockets-2024-10-31.csv"

1. id
2. date_created
3. date_modified
4. source
5. appeal_from_str
6. assigned_to_str
7. referred_to_str
8. panel_str
9. date_last_index
10. date_cert_granted
11. date_cert_denied
12. date_argued
13. date_reargued
14. date_reargument_denied
15. date_filed
16. date_terminated
17. date_last_filing
18. case_name_short
19. case_name
20. case_name_full
21. slug
22. docket_number
23. docket_number_core
24. pacer_case_id
25. cause
26. nature_of_suit
27. jury_demand
28. jurisdiction_type
29. appellate_fee_status
30. appellate_case_type_information
31. mdl_status
32. filepath_local
33. filepath_ia
34. filepath_ia_json
35. ia_upload_failure_count
36. ia_needs_upload
37. ia_date_first_change
38. view_count
39. date_blocked
40. blocked
41. appeal_from_id
42. assigned_to_id
43. court_id
44. idb_data_id
45. originating_court_information_id
46. referred_to_id
47. federal_dn_case_type
48. federal_dn_office_code
49. federal_dn_judge_initials_assigned
50. federal_dn_judge_initials_referred
51. federal_defendant_number
52. parent_docket_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\opinion-clusters-2024-10-31.csv"

1. id
2. date_created
3. date_modified
4. judges
5. date_filed
6. date_filed_is_approximate
7. slug
8. case_name_short
9. case_name
10. case_name_full
11. scdb_id
12. scdb_decision_direction
13. scdb_votes_majority
14. scdb_votes_minority
15. source
16. procedural_history
17. attorneys
18. nature_of_suit
19. posture
20. syllabus
21. headnotes
22. summary
23. disposition
24. history
25. other_dates
26. cross_reference
27. correction
28. citation_count
29. precedential_status
30. date_blocked
31. blocked
32. filepath_json_harvard
33. filepath_pdf_harvard
34. docket_id
35. arguments
36. headmatter

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\opinions-2024-10-31.csv"

1. id
2. date_created
3. date_modified
4. author_str
5. per_curiam
6. joined_by_str
7. type
8. sha1
9. page_count
10. download_url
11. local_path
12. plain_text
13. html
14. html_lawbox
15. html_columbia
16. html_anon_2020
17. xml_harvard
18. html_with_citations
19. extracted_by_ocr
20. author_id
21. cluster_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\originating-court-information-2024-10-31.csv"

1. id
2. date_created
3. date_modified
4. docket_number
5. assigned_to_str
6. ordering_judge_str
7. court_reporter
8. date_disposed
9. date_filed
10. date_judgment
11. date_judgment_eod
12. date_filed_noa
13. date_received_coa
14. assigned_to_id
15. ordering_judge_id

"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\parentheticals-2024-10-31.csv"

1. id
2. text
3. score
4. described_opinion_id
5. describing_opinion_id
6. group_id
```

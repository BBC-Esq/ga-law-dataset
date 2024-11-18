import dask.dataframe as dd

def inspect_csv_structure(csv_filepath):
    print(f"\nInspecting file: {csv_filepath}")
    print("-" * 50)
    
    # Read first few lines directly to analyze structure
    with open(csv_filepath, 'r', encoding='utf-8') as f:
        first_lines = [next(f) for _ in range(5)]
    
    # Analyze header
    header = first_lines[0].strip()
    
    # Test different delimiters
    delimiters = [',', '\t', '|', ';']
    delimiter_counts = {}
    for d in delimiters:
        header_count = len(header.split(d))
        if header_count > 1:  # If splits into multiple fields
            delimiter_counts[d] = header_count
    
    # Find most likely delimiter (one that creates most fields)
    detected_delimiter = max(delimiter_counts.items(), key=lambda x: x[1])[0] if delimiter_counts else ','
    header_fields = header.split(detected_delimiter)
    
    print(f"\nHeader Analysis:")
    print(f"Number of header fields: {len(header_fields)}")
    
    # Analyze quote characters
    quotes_used = {'`': 0, '"': 0, "'": 0}
    for line in first_lines[1:]:  # Skip header
        for quote in quotes_used:
            quotes_used[quote] += line.count(quote)
    
    # Determine most used quote character
    detected_quote = max(quotes_used.items(), key=lambda x: x[1])[0] if any(quotes_used.values()) else None
    
    print("\nQuote Character Usage in Data:")
    for quote, count in quotes_used.items():
        if count > 0:
            print(f"'{quote}' appears {count} times")
    
    print("\nFile Structure Conclusion:")
    print("------------------------")
    print(f"- Detected delimiter: '{detected_delimiter}'")
    print(f"- Detected quote character: '{detected_quote}'" if detected_quote else "- No quote character detected")
    print("- Fields may contain delimiters within quoted values")
    
    print("\nRecommended Dask/Pandas Read Settings:")
    print("```python")
    print("df = dd.read_csv(file_path,")
    print(f"               delimiter='{detected_delimiter}',")
    if detected_quote:
        print(f"               quotechar='{detected_quote}',")
        print("               escapechar='\\\\',")
    print("               on_bad_lines='skip')")
    print("```")
    
    print("\nAttempting to read column headers using detected settings...")
    try:
        # Use detected settings to read the file with larger sample size
        read_settings = {
            'delimiter': detected_delimiter,
            'on_bad_lines': 'skip',
            'blocksize': '256MB',  # Increased blocksize
            'sample': 1024 * 1024 * 100  # 100MB sample size
        }
        if detected_quote:
            read_settings['quotechar'] = detected_quote
            read_settings['escapechar'] = '\\'
        
        ddf = dd.read_csv(csv_filepath, **read_settings)
        
        print("\nSuccessfully read columns:")
        for i, column in enumerate(ddf.columns, 1):
            print(f"{i}. {column}")
            
    except Exception as e:
        print(f"\nError reading columns: {str(e)}")
        print("\nTrying alternative method to read headers...")
        try:
            # Fallback method: read just the header row
            with open(csv_filepath, 'r', encoding='utf-8') as f:
                header_line = f.readline().strip()
                columns = [col.strip() for col in header_line.split(detected_delimiter)]
                print("\nColumns found in header:")
                for i, column in enumerate(columns, 1):
                    print(f"{i}. {column}")
        except Exception as e2:
            print(f"\nError with alternative method: {str(e2)}")
            print("\nYou may need to adjust the reading settings manually.")

# Use the function
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\citation-map-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\citations-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\courts-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\dockets-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\opinion-clusters-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\opinions-2024-10-31.csv"
# csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\originating-court-information-2024-10-31.csv"
csv_file = r"D:\Scripts\Scrape_Caselaw\FLP Bulk Data\parentheticals-2024-10-31.csv"

inspect_csv_structure(csv_file)
import json
import csv

def update_json_attribute(json_file, csv_file, output_file):
    # Load JSON data
    with open(json_file, 'r') as jf:
        data = json.load(jf)
    
    # Load CSV mappings
    mappings = {}
    with open(csv_file, 'r') as cf:
        csv_reader = csv.reader(cf)
        next(csv_reader)  # Skip header if present
        for row in csv_reader:
            if len(row) >= 2:
                old_value = row[0].strip()
                new_value = row[1].strip()
                mappings[old_value] = new_value
    
    # Update JSON data
    def update_series_name(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == 'SeriesName' and value in mappings:
                    obj[key] = mappings[value]
                else:
                    update_series_name(value)
        elif isinstance(obj, list):
            for item in obj:
                update_series_name(item)
    
    update_series_name(data)
    
    # Save updated JSON
    with open(output_file, 'w') as jf:
        json.dump(data, jf, indent=4)
    
    print(f"Updated JSON saved to {output_file}")

# Example usage
json_file = 'data.json'       # Input JSON file
csv_file = 'mapping.csv'      # Input CSV file
output_file = 'updated_data.json'  # Output JSON file

update_json_attribute(json_file, csv_file, output_file)

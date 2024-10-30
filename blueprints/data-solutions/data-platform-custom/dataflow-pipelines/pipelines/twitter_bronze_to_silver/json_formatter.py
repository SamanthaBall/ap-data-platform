import json
import argparse

def convert_json_array_to_line_delimited(input_file):
    # Load the JSON array from the input file
    with open(input_file, 'r', encoding='utf-8') as infile:
        data = json.load(infile)  # This reads the JSON array into a Python list

    output_file = input_file.rsplit(".", 1)[0] + "_delimited.json"
    
    # Write each object in the list as a separate line in the output file
    with open(output_file, 'w') as outfile:
        for item in data:
            json_line = json.dumps(item)
            outfile.write(json_line + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON array file to line-delimited JSON format")
    parser.add_argument("input_file", help="Path to the input JSON array file")

    args = parser.parse_args()
    
    convert_json_array_to_line_delimited(args.input_file)

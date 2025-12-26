#!/usr/bin/env python3
import json
import os

def convert_to_single_line(input_file, output_file):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    cleaned_data = []

    with open(input_file, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:  # skip first line if it's a comment/header
            line = line.strip()
            if line:
                try:
                    cleaned_data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid line:\n{line}\nError: {e}\n")

    with open(output_file, 'w') as f:
        for record in cleaned_data:
            f.write(json.dumps(record) + '\n')

    print(f"{len(cleaned_data)} records written to {output_file}")

# Process files
convert_to_single_line(
    'data/supply_chain_data.json',
    'output/supply_chain_data.json'
)

convert_to_single_line(
    'data/financial_data.json',
    'output/financial_data.json'
)

print("JSON files converted to single-line format for Spark")
import re

# It can be used to split the JSON array into small pieces of desired size

def extract_first_n_mb(input_file_path, output_file_path, n_mb=10):
    n_bytes = n_mb * 1024 * 1024  # Convert MB to bytes

    try:
        with open(input_file_path, 'rb') as infile:
            # Read the first n bytes from the file
            chunk = infile.read(n_bytes).decode('utf-8', errors='ignore')

        # Check if the chunk starts with "[" and remove it
        if chunk.lstrip().startswith('['):
            chunk = chunk.lstrip()[1:]

        # Now we need to reverse the chunk and find the last complete JSON object with "batch_event_index"
        reversed_chunk = chunk[::-1]
        end_of_last_object = None

        # Regex pattern to find a complete "batch_event_index" in reverse
        pattern = re.compile(r'"\d+"\s*:\s*"xedni_tneve_hctab"')

        # Searching for the last occurrence of "batch_event_index" in reverse
        match = pattern.search(reversed_chunk)

        if match:
            # Get the position of the match and calculate the real position in the forward chunk
            end_of_last_object = len(reversed_chunk) - match.start()

            # Trim the chunk to include up to the last complete object
            trimmed_chunk = chunk[:end_of_last_object]

            # Now close the array properly by adding a closing bracket
            trimmed_chunk = "[\n" + trimmed_chunk.strip() + "\n\t}\n]"

            # Write the valid JSON array to the output file
            with open(output_file_path, 'w', encoding='utf-8') as outfile:
                outfile.write(trimmed_chunk)

            print(f"Successfully wrote first {n_mb} MB with complete elements to {output_file_path}.")
        else:
            print("Error: Could not find a complete 'batch_event_index' field in the chunk.")

    except Exception as e:
        print(f"An error occurred: {e}")


# Example usage
input_file = 'merged_data.json'
output_file = 'merged_data_1000mb.json'
extract_first_n_mb(input_file, output_file, n_mb=1000)

import os
from gpx_converter import Converter

directory = '/Users/ajit/Desktop/Airflow Project/export_100231341/activities_1'
output_directory = '/Users/ajit/Desktop/Airflow Project/export_100231341/OutputFiles/'

# Ensure the output directory exists or create it
os.makedirs(output_directory, exist_ok=True)

# Iterate over files in the GPX directory
for filename in os.listdir(directory):
    full_path = os.path.join(directory, filename)

    # Check if the current item is a file and has a '.gpx' extension
    if os.path.isfile(full_path) and filename.endswith('.gpx'):
        try:
            # Convert GPX file to DataFrame
            df = Converter(input_file=full_path).gpx_to_dataframe()

            # Add a 'filename' column and save as CSV
            df['filename'] = filename
            csv_output_path = os.path.join(output_directory, f'{filename}.csv')
            df.to_csv(csv_output_path, encoding='utf-8', sep=',')

            print(f"Conversion successful for {filename}")

        except Exception as e:
            print(f"Error processing file {full_path}: {e}")
            continue  # Skip to the next file

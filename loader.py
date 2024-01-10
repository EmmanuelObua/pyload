import os
import gzip
import csv
import shutil
import zipfile
import tempfile
import pandas as pd
import subprocess
import random
import string
import constants
import logging

# Configure logging to a file
logging.basicConfig(filename='pyload.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_random_string(length: int = 8) -> str:
    """
    Generate a random string of specified length.

    Parameters:
    - length (int): The length of the random string. Default is 8.

    Returns:
    - str: The generated random string.
    """
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def get_file_info(folder_name):

	file_types_info = {
		'postauth': {
			'column_names': constants.postauth,
			'table_name': 'postauth'
		},
	}

	info = file_types_info.get(folder_name, {'column_names': None, 'table_name': None})

	return info['column_names'], info['table_name']

def process_zip_file(zip_file_path, temp_dir, folder, ext='.zip'):

	try:

		# Determine the column_names and table_name for each file
		column_names, table_name = get_file_info(folder)

		file_name = ''

		if ext == '.zip':
			with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
				output_file = zip_file.extractall(temp_dir)

				for file_name in output_file.namelist():
					file_name = file_name
		else:

			output_file = os.path.join(temp_dir, os.path.splitext(os.path.basename(zip_file_path))[0])

			with gzip.open(zip_file_path, 'rb') as f_in:
				with open(output_file, 'wb') as f_out:
					shutil.copyfileobj(f_in, f_out)

			file_name = os.path.basename(output_file)

		if column_names:
			return file_name, table_name, column_names
		else:
			print(f"Unknown file type for {file_name}")

	except zipfile.BadZipFile:
		print(f"Error: Not a valid zip file - {zip_file_path}")
	except Exception as e:
		print(f"Error: {e}")

def list_folders(directory_path, excluded_folder):
    """
    Lists folders in the specified directory, excluding a specified folder.

    Args:
        directory_path (str): The path to the directory.
        excluded_folder (str): The folder to exclude from the list.

    Returns:
        list: A list of folder names.
    """
    folders = [
        folder
        for folder in os.listdir(directory_path)
        if os.path.isdir(os.path.join(directory_path, folder)) and folder != excluded_folder
    ]
    return folders

def load(env):

	try:

		zipfiles_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs'))
		loaded_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs/loaded'))
		temp_dir = tempfile.mkdtemp()

		print(temp_dir)

		folders = list_folders(zipfiles_dir, excluded_folder = 'loaded')
		for folder in folders:

			folder_files = os.path.abspath(os.path.join(os.path.dirname(__file__), f'cdrs/{folder}'))

			for file_name in os.listdir(folder_files):

				random_string_name = generate_random_string()
				cleaned_file_path = os.path.join(temp_dir, f'{random_string_name}_cleaned_file.csv')

				zip_file_path = os.path.join(folder_files, file_name)

				if file_name.endswith(".zip"):
					unzipped_file_name, table_name, column_names = process_zip_file(zip_file_path, temp_dir, folder, ext = '.zip')
				elif file_name.endswith(".gz"):
					unzipped_file_name, table_name, column_names = process_zip_file(zip_file_path, temp_dir, folder, ext = '.gz')
				else:
					return

				file_path = os.path.join(temp_dir, unzipped_file_name)

				transformed_file_path = os.path.join(temp_dir, f"{random_string_name}.csv")

				# Read the content of the file and append unzipped_file_name to each record
				with open(file_path, 'r') as source_file:
					records = source_file.readlines()[1:]
					records_with_prefix = [f"{unzipped_file_name}|{record.rstrip()}" for record in records]

				# Write the transformed records to the transformed_file_path
				with open(transformed_file_path, 'w') as transformed_file:
					csv_writer = csv.writer(transformed_file, delimiter=',')
					for record in records_with_prefix:
					    csv_writer.writerow(record.split('|'))

				# Clean the transformed file with pandas to remove empty rows and columns  
				df = pd.read_csv(transformed_file_path, sep=',')
				df = df.dropna(axis=1, how='any')
				df.to_csv(cleaned_file_path, index=False, sep=',')

				# Run the loader command to load the clean transformed files to the database
				command = f"mysql --local-infile -h {env['MYSQL_HOST']} -u {env['MYSQL_USER_NAME']} -p -D {env['MYSQL_DATABASE']} -e \"LOAD DATA LOCAL INFILE '{cleaned_file_path.replace('\\', '\\\\')}' INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';\""
				subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

				# Move loaded dataset to the loaded folder.
				shutil.move(zip_file_path, loaded_dir)

		# Delete the temp_dir after all the operations are done
		shutil.rmtree(temp_dir)

	except subprocess.CalledProcessError as e:

		print("Command Output (if any):")
		print(e.stdout)
		print("Command Error (if any):")
		print(e.stderr)

		print(f"Error: {e}")

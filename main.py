#!C:\Program Files\Python310

import os
import tempfile
import shutil
import subprocess
from dotenv import dotenv_values
from loader import write_log,sendEmail,process_zip_file,list_folders,generate_random_string,read_and_transform_file,clean_transformed_file,load_data_to_database,move_file_to_loaded

env = dotenv_values(".env")

def is_folder_empty(folder):
    """
    Check if the specified folder is empty.
    """
    return len([file for file in os.listdir(folder) if not file.startswith('.')]) == 0

def run_etl():
	"""
    Process files and load to respective databases.
    """
	try:
		zipfiles_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs'))
		loaded_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs/loaded'))
		temp_dir = tempfile.mkdtemp()

		folders = list_folders(zipfiles_dir, excluded_folder='loaded')
		print(f'running')
		
		for folder in folders:

			success_msg = ''
			failure_msg = ''

			if(folder == 'postauth'):
				success_msg = 'Radius access attempts ETL loaded successfully.'
				failure_msg = 'Radius access attempts ETL loading failed, no postauth cdr file(s) found.'

			folder_files = os.path.abspath(os.path.join(os.path.dirname(__file__), f'cdrs/{folder}'))

			if is_folder_empty(folder_files):
				sendEmail(failure_msg)
				continue
			else:

				file_count = 0
				record_count = 0

				for file_name in [file for file in os.listdir(folder_files) if not file.startswith('.')]:
					
					random_string_name = generate_random_string()
					cleaned_file_path = os.path.join(temp_dir, f'{random_string_name}_cleaned_file.csv')
					transformed_file_path = os.path.join(temp_dir, f"{random_string_name}.csv")

					zip_file_path = os.path.join(folder_files, file_name)

					if file_name.endswith(".zip"):
						unzipped_file_name, table_name, column_names, database_name = process_zip_file(zip_file_path, temp_dir, folder, ext='.zip')
					elif file_name.endswith(".gz"):
						unzipped_file_name, table_name, column_names, database_name = process_zip_file(zip_file_path, temp_dir, folder, ext='.gz')

					file_path = os.path.join(temp_dir, unzipped_file_name)

					records_with_prefix = read_and_transform_file(file_path, unzipped_file_name)

					file_count += 1
					record_count += len(records_with_prefix)

					if not records_with_prefix:
						print("File is empty. Skipping further processing.")
					else:
						clean_transformed_file(transformed_file_path, cleaned_file_path, records_with_prefix)
						load_data_to_database(env, cleaned_file_path, table_name, column_names, database_name)
						move_file_to_loaded(zip_file_path, loaded_dir)

				sendEmail(success_msg + f'{file_count:,} files and {record_count:,} records loaded.')

		# Delete the temp_dir after all the operations are done
		shutil.rmtree(temp_dir)

		return True

	except subprocess.CalledProcessError as e:

		write_log("Error:", f"Error loading radius access attempts ETL: {e}")
		write_log("Command Error (if any):", f"Error loading radius access attempts ETL: {e.stderr}")
		write_log("Command Output (if any):", f"Error loading radius access attempts ETL: {e.stdout}")

		message = f'Radius access attempts ETL loading failed with an Exception: {e}'
		sendEmail(message, Cc = False)

		return False

	except Exception as e:

		write_log("Error:", f"Error loading radius access attempts ETL: {e}")
		message = f'Radius access attempts ETL loading failed with an Exception: {e}'
		sendEmail(message, Cc = False)
		return False

run_etl()
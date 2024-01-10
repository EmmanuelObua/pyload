import os
import tempfile
import shutil
import subprocess
from dotenv import dotenv_values
from loader import process_zip_file,list_folders,generate_random_string,read_and_transform_file,clean_transformed_file,load_data_to_database,move_file_to_loaded

env = dotenv_values(".env")

try:
    zipfiles_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs'))
    loaded_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs/loaded'))
    temp_dir = tempfile.mkdtemp()

    print(temp_dir)

    folders = list_folders(zipfiles_dir, excluded_folder='loaded')
    for folder in folders:
        folder_files = os.path.abspath(os.path.join(os.path.dirname(__file__), f'cdrs/{folder}'))

        for file_name in [file for file in os.listdir(folder_files) if not file.startswith('.')]:
        	
            random_string_name = generate_random_string()
            cleaned_file_path = os.path.join(temp_dir, f'{random_string_name}_cleaned_file.csv')

            zip_file_path = os.path.join(folder_files, file_name)

            if file_name.endswith(".zip"):
                unzipped_file_name, table_name, column_names = process_zip_file(zip_file_path, temp_dir, folder, ext='.zip')
            elif file_name.endswith(".gz"):
                unzipped_file_name, table_name, column_names = process_zip_file(zip_file_path, temp_dir, folder, ext='.gz')

            file_path = os.path.join(temp_dir, unzipped_file_name)
            transformed_file_path = os.path.join(temp_dir, f"{random_string_name}.csv")

            records_with_prefix = read_and_transform_file(file_path, unzipped_file_name)
            clean_transformed_file(transformed_file_path, cleaned_file_path, records_with_prefix)
            load_data_to_database(env, cleaned_file_path, table_name)
            move_file_to_loaded(zip_file_path, loaded_dir)

    # Delete the temp_dir after all the operations are done
    shutil.rmtree(temp_dir)

except subprocess.CalledProcessError as e:
    print("Command Output (if any):")
    print(e.stdout)
    print("Command Error (if any):")
    print(e.stderr)
    print(f"Error: {e}")
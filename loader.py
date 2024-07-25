import os
import gzip
import csv
import shutil
import zipfile
import pandas as pd
import subprocess
import random
import string
import constants
from typing import Tuple, Optional
import datetime
from time import time, ctime
import json
import requests
import mysql.connector
import config as cfg

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

def get_file_info(folder_name: str) -> Tuple[Optional[list], Optional[str]]:
	"""
	Retrieve column names and table name information for a given folder.

	Parameters:
	- folder_name (str): The name of the folder.

	Returns:
	- Tuple[Optional[list], Optional[str]]: A tuple containing column names and table name information.
	  Returns (None, None) if the folder name is not recognized.
	"""
	file_types_info = {
		'postauth': {
			'column_names': constants.postauth,
			'table_name': 'postauth',
			'database_name': 'radius_data'
		},
	}

	info = file_types_info.get(folder_name, {'column_names': None, 'table_name': None})

	return info['column_names'], info['table_name'], info['database_name']

def process_zip_file(zip_file_path, temp_dir, folder, ext = '.zip'):
	"""
	Process a zip or gzip file, extracting information about column names and table name.

	Parameters:
	- zip_file_path (str): Path to the input zip or gzip file.
	- temp_dir (str): Temporary directory for extracting files.
	- folder (str): Folder name for organizing extracted files.
	- ext (str): File extension, defaults to '.zip'.

	Returns:
	- tuple: A tuple containing file name, table name, and column names.
	"""
	try:
		# Determine the column names and table name for each file
		column_names, table_name, database_name = get_file_info(folder)

		file_name = ''

		if ext == '.zip':
			with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
				# Extract all files from the zip archive to the temporary directory
				output_file = zip_file.extractall(temp_dir)

				# Use the first file name in the archive (assuming it's relevant)
				file_name = output_file.namelist()[0] if output_file.namelist() else ''
		else:
			# Extract file from gzip archive to the temporary directory
			output_file = os.path.join(temp_dir, os.path.splitext(os.path.basename(zip_file_path))[0])

			with gzip.open(zip_file_path, 'rb') as f_in:
				with open(output_file, 'wb') as f_out:
					shutil.copyfileobj(f_in, f_out)

			file_name = os.path.basename(output_file)

		if column_names:
			return file_name, table_name, column_names, database_name
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

def read_and_transform_file(file_path, unzipped_file_name):
	"""Read the content of the file and append unzipped_file_name to each record"""

	# Remove '_postauth.txt' to get the date string
	date_str = unzipped_file_name.replace('_postauth.txt', '')

	# Format the date string to 'YYYY-MM-DD'
	processed_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

	with open(file_path, 'r') as source_file:
		records = source_file.readlines()[1:]
		records_with_prefix = [f"{unzipped_file_name}|{record.rstrip()}|{processed_date}" for record in records]

	return records_with_prefix

def clean_transformed_file(transformed_file_path, cleaned_file_path, records_with_prefix):
	# Write the transformed records to the transformed_file_path
	with open(transformed_file_path, 'w') as transformed_file:
		csv_writer = csv.writer(transformed_file, delimiter=',')
		for record in records_with_prefix:
			csv_writer.writerow(record.split('|'))

	# Clean the transformed file with pandas to remove empty rows and columns  
	df = pd.read_csv(transformed_file_path, sep=',')
	df = df.dropna(axis=1, how='any')
	df.to_csv(cleaned_file_path, index=False, sep=',')

def load_data_to_database(env, cleaned_file_path, table_name, column_names, database_name):
	"""Load cleaned transformed files to the database."""

	connection = mysql.connector.connect(
		host = cfg.tMysqlHost,
		port = cfg.tMysqlPort,
		user = cfg.tMysqlUser,
		password = cfg.tMysqlPassword,
		database = database_name,
		charset='utf8',
		allow_local_infile = True
	)

	cursor = connection.cursor()

	cleaned_file_path = cleaned_file_path.replace('\\', '\\\\')

	# command = f"mysql --local-infile -h {env['MYSQL_HOST']} -u {env['MYSQL_USER_NAME']} -p -P {env['MYSQL_PORT']} -D {database_name} -e \"LOAD DATA LOCAL INFILE '{cleaned_file_path}' INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';\""
	# subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

	query = f"LOAD DATA LOCAL INFILE '{cleaned_file_path}' INTO TABLE {table_name} FIELDS TERMINATED BY ',' ENCLOSED BY '\"'"

	write_log("Info:", f"Loading data from {cleaned_file_path} into table {table_name}...")

	cursor.execute(query)

	# Commit the transaction
	connection.commit()

	write_log("Info:", f"Data loaded successfully from {cleaned_file_path} into table {table_name}.")

	# Close the cursor
	cursor.close()

def move_file_to_loaded(zip_file_path, loaded_dir):
	"""Move loaded dataset to the loaded folder."""
	shutil.move(zip_file_path, loaded_dir)

#create day log file and write to it if it exists write to it
def write_log(messageType,message):
	try:
		today = datetime.date.today()
		current_date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		log_file = os.path.join(cfg.logPath, f"bb_general_etl_{today}.log")
		if not os.path.exists(log_file):
			with open(log_file, 'w') as f:
				#write timestamp: message type: message
				f.write(f"{current_date_time} - {messageType}: {message}")
		else:
			with open(log_file, 'a') as f:
				#write timestamp: message type: message
				f.write(f"\n{current_date_time} - {messageType}: {message}")
	except Exception as e:
		print("Error creating log file:", e)   

def sendEmail(msg, Cc = True):
	try:
		conf = cfg.emailConfig

		body = {
		"Dest": conf['Dest'],
		"From": conf['From'],
		"To": conf['To'],
		"Sub": "Notification: Radius Access Attempts ETL Update",
		"Msg": (ctime(time()) + " - " + str(msg))
		}

		if Cc:
			body["Cc"] = conf['Cc']

		headers = {"Content-Type": "application/json"}

		response = requests.post(conf['Url'], data=json.dumps(body), headers=headers,
			auth=(conf['username'], conf['password']))

		return response

	except Exception as e:
		print("Error sending email:", e)
		write_log("Error:", f"Error sending email: {e}")
		return False
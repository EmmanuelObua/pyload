import os
import shutil
import config as cfg
import json

# Define the directory containing the loaded files
loaded_cdr_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), 'cdrs/loaded'))

def clear_folder(folder_path):
    """
    Clears all files in the specified folder.

    Parameters:
    folder_path (str): The path to the folder to be cleared.
    """
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    # List all files in the folder
    for filename in os.listdir(folder_path):
        if filename.startswith('.'):
            # Ignore files starting with a dot
            print(f"Ignored file: {filename}")
            continue

        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove the file or symbolic link
                print(f"Deleted file: {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove the directory
                print(f"Deleted directory: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    # sendEmail(message = 'Uploaded radius post auth ETL files deleted')

def sendEmail(msg):
    try:
        conf = cfg.emailConfig

        body = {
        "Dest": conf['Dest'],
        "From": conf['From'],
        "To": conf['To'],
        "Sub": "Notification: Clean Uploaded Radius Post Auth ETL Files",
        "Msg": (ctime(time()) + " - " + str(msg))
        }

        headers = {"Content-Type": "application/json"}

        response = requests.post(conf['Url'], data=json.dumps(body), headers=headers,
            auth=(conf['username'], conf['password']))

        return response

    except Exception as e:
        print("Error sending email:", e)
        return False

if __name__ == "__main__":
    clear_folder(loaded_cdr_folder)
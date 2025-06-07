import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base URL for downloading prerequisites
url_base = "https://huggingface.co/IAHispano/Applio/resolve/main/Resources"


models_list = [("predictors/", ["rmvpe.pt", "fcpe.pt"])]
embedders_list = [("embedders/contentvec/", ["pytorch_model.bin", "config.json"])]
executables_list = [
    ("", ["ffmpeg.exe", "ffprobe.exe"]),
    ("formant/", ["stftpitchshift.exe" if os.name == "nt" else "stftpitchshift"]),
]

# Define folder mappings relative to ~/.bfrvc/models/
base_path = os.path.expanduser("~/.bfrvc")
folder_mapping_list = {
    "embedders/contentvec/": os.path.join(base_path, "models/embedders/contentvec/"),
    "predictors/": os.path.join(base_path, "models/predictors/"),
    "formant/": os.path.join(base_path, "models/formant/"),
}

def get_file_size_if_missing(file_list):
    """
    Calculate the total size of files to be downloaded only if they do not exist locally.

    Args:
        file_list (list): List of tuples containing remote folder and file names.

    Returns:
        int: Total size in bytes of files to be downloaded.
    """
    total_size = 0
    for remote_folder, files in file_list:
        local_folder = folder_mapping_list.get(remote_folder, "")
        for file in files:
            destination_path = os.path.join(local_folder, file)
            if not os.path.exists(destination_path):
                url = f"{url_base}/{remote_folder}{file}"
                try:
                    response = requests.head(url, timeout=10)
                    response.raise_for_status()
                    total_size += int(response.headers.get("content-length", 0))
                except requests.RequestException as error:
                    logging.error(f"Failed to get size for {url}: {error}")
    return total_size

def download_file(url, destination_path, global_bar):
    """
    Download a file from the given URL to the specified destination path,
    updating the global progress bar as data is downloaded.

    Args:
        url (str): URL of the file to download.
        destination_path (str): Local path to save the file.
        global_bar (tqdm): Progress bar to update.
    """
    try:
        dir_name = os.path.dirname(destination_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        block_size = 1024
        with open(destination_path, "wb") as file:
            for data in response.iter_content(block_size):
                file.write(data)
                global_bar.update(len(data))
    except requests.RequestException as error:
        logging.error(f"Failed to download {url}: {error}")

def download_mapping_files(file_mapping_list, global_bar):
    """
    Download all files in the provided file mapping list using a thread pool executor,
    and update the global progress bar as downloads progress.

    Args:
        file_mapping_list (list): List of tuples containing remote folder and file names.
        global_bar (tqdm): Progress bar to update.
    """
    with ThreadPoolExecutor() as executor:
        futures = []
        for remote_folder, file_list in file_mapping_list:
            local_folder = folder_mapping_list.get(remote_folder, base_path)
            for file in file_list:
                destination_path = os.path.join(local_folder, file)
                if not os.path.exists(destination_path):
                    url = f"{url_base}/{remote_folder}{file}"
                    futures.append(
                        executor.submit(
                            download_file, url, destination_path, global_bar
                        )
                    )
        for future in futures:
            try:
                future.result()
            except Exception as error:
                logging.error(f"Error in download thread: {error}")


def calculate_total_size(models, exe):
    """
    Calculate the total size of all files to be downloaded based on selected categories.

    Args:
        models (bool): Whether to include predictor and embedder models.
        exe (bool): Whether to include executables.

    Returns:
        int: Total size in bytes of files to be downloaded.
    """
    total_size = 0
    if models:
        total_size += get_file_size_if_missing(models_list)
        total_size += get_file_size_if_missing(embedders_list)
    if exe:
        total_size += get_file_size_if_missing(executables_list)
    return total_size

def model_need(models, exe):
  
    """
    Manage the download pipeline for different categories of files.

    Args:
        models (bool): Whether to download predictor and embedder models.
        exe (bool): Whether to download executables.

    Returns:
        str: Status message indicating completion or error.
    """
    try:
        total_size = calculate_total_size(models, exe)
        if total_size == 0:
            logging.info("All prerequisites are already downloaded.")
            return "All prerequisites are already downloaded."

        with tqdm(
            total=total_size, unit="iB", unit_scale=True, desc="Downloading prerequisites"
        ) as global_bar:
            if models:
                download_mapping_files(models_list, global_bar)
                download_mapping_files(embedders_list, global_bar)
            if exe:
                if os.name == "nt":
                    download_mapping_files(executables_list, global_bar)
                else:
                    logging.info("Skipping executables download on non-Windows platform")
            
        logging.info("Prerequisites downloaded successfully.")
        return "Prerequisites downloaded successfully."
    except Exception as error:
        logging.error(f"Error downloading prerequisites: {error}")
        return f"Error: {error}"

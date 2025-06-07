import os
import re
import shutil
import zipfile
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote
from tqdm import tqdm
import logging

from bfrvc.units.utils import format_title
from bfrvc.units.tools import gdown

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Use user-specific directory for logs and zips
base_path = os.path.expanduser("~/.bfrvc")
logs_path = os.path.join(base_path, "logs")
zips_path = os.path.join(logs_path, "zips")
os.makedirs(zips_path, exist_ok=True)

def search_pth_index(folder):
    """
    Search for .pth and .index files in a folder.

    Args:
        folder (str): Path to the folder to search.

    Returns:
        tuple: Lists of .pth and .index file paths.
    """
    if not os.path.exists(folder):
        logging.error(f"Folder not found: {folder}")
        return [], []

    pth_paths = [
        os.path.join(folder, file)
        for file in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, file)) and file.endswith(".pth")
    ]
    index_paths = [
        os.path.join(folder, file)
        for file in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, file)) and file.endswith(".index")
    ]
    return pth_paths, index_paths

def download_from_url(url):
    """
    Download a file from a URL, supporting Google Drive and Hugging Face.

    Args:
        url (str): URL of the file to download.

    Returns:
        str: Status of the download ('downloaded' or None).
    """
    original_dir = os.getcwd()
    try:
        os.chdir(zips_path)
        if "drive.google.com" in url:
            file_id = extract_google_drive_id(url)
            if file_id:
                gdown.download(
                    url=f"https://drive.google.com/uc?id={file_id}",
                    quiet=False,
                    fuzzy=True,
                )
            else:
                logging.error("Invalid Google Drive URL")
                return None
        elif "/blob/" in url or "/resolve/" in url:
            download_blob_or_resolve(url)
        elif "/tree/main" in url:
            download_from_huggingface(url)
        else:
            download_file(url)
        rename_downloaded_files()
        return "downloaded"
    except Exception as error:
        logging.error(f"An error occurred downloading the file: {error}")
        return None
    finally:
        os.chdir(original_dir)

def extract_google_drive_id(url):
    """
    Extract the file ID from a Google Drive URL.

    Args:
        url (str): Google Drive URL.

    Returns:
        str or None: File ID if found, else None.
    """
    if "file/d/" in url:
        return url.split("file/d/")[1].split("/")[0]
    if "id=" in url:
        return url.split("id=")[1].split("&")[0]
    return None

def download_blob_or_resolve(url):
    """
    Download a file from a Hugging Face blob or resolve URL.

    Args:
        url (str): URL to download.

    Raises:
        ValueError: If the download fails.
    """
    if "/blob/" in url:
        url = url.replace("/blob/", "/resolve/")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        save_response_content(response)
    else:
        raise ValueError(f"Download failed with status code: {response.status_code}")

def save_response_content(response):
    """
    Save the content of a response to a file with progress bar.

    Args:
        response: HTTP response object.
    """
    content_disposition = unquote(response.headers.get("Content-Disposition", ""))
    file_name = (
        re.search(r'filename="([^"]+)"', content_disposition)
        .group(1)
        .replace(os.path.sep, "_")
        if content_disposition and re.search(r'filename="([^"]+)"', content_disposition)
        else "downloaded_file"
    )

    total_size = int(response.headers.get("Content-Length", 0))
    chunk_size = 1024

    with open(os.path.join(zips_path, file_name), "wb") as file, tqdm(
        total=total_size, unit="B", unit_scale=True, desc=file_name
    ) as progress_bar:
        for data in response.iter_content(chunk_size):
            file.write(data)
            progress_bar.update(len(data))

def download_from_huggingface(url):
    """
    Download a zip file from a Hugging Face repository.

    Args:
        url (str): Hugging Face repository URL.

    Raises:
        ValueError: If no zip file is found or download fails.
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    temp_url = next(
        (
            link["href"]
            for link in soup.find_all("a", href=True)
            if link["href"].endswith(".zip")
        ),
        None,
    )
    if temp_url:
        url = temp_url.replace("blob", "resolve")
        if "huggingface.co" not in url:
            url = "https://huggingface.co" + url
        download_file(url)
    else:
        raise ValueError("No zip file found in Huggingface URL")

def download_file(url):
    """
    Download a file from a generic URL.

    Args:
        url (str): URL to download.

    Raises:
        ValueError: If the download fails.
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        save_response_content(response)
    else:
        raise ValueError(f"Download failed with status code: {response.status_code}")

def rename_downloaded_files():
    """
    Rename downloaded files to remove path separators.
    """
    for current_path, _, zip_files in os.walk(zips_path):
        for file in zip_files:
            file_name, extension = os.path.splitext(file)
            real_path = os.path.join(current_path, file)
            new_path = os.path.join(current_path, file_name.replace(os.path.sep, "_") + extension)
            if real_path != new_path:
                os.rename(real_path, new_path)

def extract(zipfile_path, unzips_path):
    """
    Extract a zip file to a specified directory.

    Args:
        zipfile_path (str): Path to the zip file.
        unzips_path (str): Destination directory for extracted files.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        with zipfile.ZipFile(zipfile_path, "r") as zip_ref:
            zip_ref.extractall(unzips_path)
        os.remove(zipfile_path)
        return True
    except Exception as error:
        logging.error(f"An error occurred extracting the zip file: {error}")
        return False

def unzip_file(zip_path, zip_file_name):
    """
    Extract a specific zip file to a directory.

    Args:
        zip_path (str): Path to the zip file directory.
        zip_file_name (str): Name of the zip file without extension.
    """
    zip_file_path = os.path.join(zip_path, zip_file_name + ".zip")
    extract_path = os.path.join(logs_path, zip_file_name)
    if not os.path.exists(zip_file_path):
        logging.error(f"Zip file not found: {zip_file_path}")
        return
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    os.remove(zip_file_path)

def model_download(url: str):
    """
    Pipeline to download and extract a model from a URL.

    Args:
        url (str): URL of the model to download.

    Returns:
        tuple or str: Paths to .pth and .index files if successful, 'Error' otherwise.
    """
    try:
        result = download_from_url(url)
        if result == "downloaded":
            return handle_extraction_process()
        else:
            return "Error"
    except Exception as error:
        logging.error(f"An unexpected error occurred: {error}")
        return "Error"

def handle_extraction_process():
    """
    Handle the extraction of downloaded zip files and clean up.

    Returns:
        tuple or str: Paths to .pth and .index files if successful, 'Error' otherwise.
    """
    extract_folder_path = ""
    for filename in os.listdir(zips_path):
        if filename.endswith(".zip"):
            zipfile_path = os.path.join(zips_path, filename)
            model_name = format_title(os.path.basename(zipfile_path).split(".zip")[0])
            extract_folder_path = os.path.join(logs_path, model_name)
            success = extract(zipfile_path, extract_folder_path)
            if success:
                clean_extracted_files(extract_folder_path, model_name)
                logging.info(f"Model {model_name} downloaded!")
                return search_pth_index(extract_folder_path)
            else:
                logging.error(f"Error downloading {model_name}")
                return "Error"
    if not extract_folder_path:
        logging.error("Zip file was not found.")
        return "Error"
    return search_pth_index(extract_folder_path)

def clean_extracted_files(extract_folder_path, model_name):
    """
    Clean up extracted files by removing unnecessary folders and renaming files.

    Args:
        extract_folder_path (str): Path to the extracted files.
        model_name (str): Name of the model for renaming files.
    """
    macosx_path = os.path.join(extract_folder_path, "__MACOSX")
    if os.path.exists(macosx_path):
        shutil.rmtree(macosx_path)

    subfolders = [
        f
        for f in os.listdir(extract_folder_path)
        if os.path.isdir(os.path.join(extract_folder_path, f))
    ]
    if len(subfolders) == 1:
        subfolder_path = os.path.join(extract_folder_path, subfolders[0])
        for item in os.listdir(subfolder_path):
            shutil.move(
                os.path.join(subfolder_path, item),
                os.path.join(extract_folder_path, item),
            )
        os.rmdir(subfolder_path)

    for item in os.listdir(extract_folder_path):
        source_path = os.path.join(extract_folder_path, item)
        if ".pth" in item:
            new_file_name = model_name + ".pth"
        elif ".index" in item:
            new_file_name = model_name + ".index"
        else:
            continue
        destination_path = os.path.join(extract_folder_path, new_file_name)
        if not os.path.exists(destination_path):
            os.rename(source_path, destination_path)

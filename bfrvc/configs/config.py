import torch
import json
import os
import logging
import pkg_resources

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define JSON config files
version_config_paths = [
    "configs/48000.json",
    "configs/40000.json",
    "configs/32000.json",
]

def singleton(cls):
    """
    Singleton decorator to ensure a single instance of a class.

    Args:
        cls: Class to be instantiated as a singleton.

    Returns:
        function: Function that returns the singleton instance.
    """
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance

@singleton
class Config:
    """
    Configuration class for device settings and JSON config loading.

    Attributes:
        device (str): Device to use ('cuda:0' or 'cpu').
        gpu_name (str or None): Name of the GPU if available.
        gpu_mem (int or None): GPU memory in GB if available.
        json_config (dict): Loaded JSON configuration data.
        x_pad (int): Padding parameter for device config.
        x_query (int): Query parameter for device config.
        x_center (int): Center parameter for device config.
        x_max (int): Max parameter for device config.
    """
    def __init__(self):
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
        self.gpu_name = (
            torch.cuda.get_device_name(int(self.device.split(":")[-1]))
            if self.device.startswith("cuda")
            else None
        )
        self.json_config = self.load_config_json()
        self.gpu_mem = None
        self.x_pad, self.x_query, self.x_center, self.x_max = self.device_config()
        logging.info(f"Initialized Config with device: {self.device}, GPU: {self.gpu_name}")

    def load_config_json(self):
        """
        Load JSON configuration files from the package.

        Returns:
            dict: Dictionary mapping config file names to their contents.

        Raises:
            FileNotFoundError: If a config file is missing.
            json.JSONDecodeError: If a config file is invalid.
        """
        configs = {}
        for config_file in version_config_paths:
            try:
                config_path = pkg_resources.resource_filename("bfrvc", config_file)
                with open(config_path, "r") as f:
                    configs[os.path.basename(config_file)] = json.load(f)
            except FileNotFoundError:
                logging.error(f"Config file not found: {config_file}. Ensure it is included in the package.")
                raise
            except json.JSONDecodeError as error:
                logging.error(f"Invalid JSON in {config_file}: {error}")
                raise
        return configs

    def device_config(self):
        """
        Configure device-specific parameters based on GPU memory.

        Returns:
            tuple: (x_pad, x_query, x_center, x_max) parameters.
        """
        if self.device.startswith("cuda"):
            self.set_cuda_config()
        else:
            self.device = "cpu"
            logging.info("Using CPU for computations")

        # Default configuration (assumes >=6GB GPU memory or CPU)
        x_pad, x_query, x_center, x_max = (1, 6, 38, 41)
        if self.gpu_mem is not None and self.gpu_mem <= 4:
            # Configuration for <=4GB GPU memory
            x_pad, x_query, x_center, x_max = (1, 5, 30, 32)
            logging.info(f"Adjusted device config for low GPU memory ({self.gpu_mem} GB)")

        return x_pad, x_query, x_center, x_max

    def set_cuda_config(self):
        """
        Set CUDA-specific configuration (GPU name and memory).

        Raises:
            RuntimeError: If CUDA device is invalid.
        """
        try:
            i_device = int(self.device.split(":")[-1])
            self.gpu_name = torch.cuda.get_device_name(i_device)
            self.gpu_mem = torch.cuda.get_device_properties(i_device).total_memory // (
                1024**3
            )
            logging.info(f"CUDA device {i_device}: {self.gpu_name} ({self.gpu_mem} GB)")
        except (ValueError, RuntimeError) as error:
            logging.error(f"Failed to set CUDA config: {error}")
            self.device = "cpu"
            self.gpu_name = None
            self.gpu_mem = None

def max_vram_gpu(gpu):
    """
    Get the VRAM of a specified GPU in GB.

    Args:
        gpu (int): GPU index.

    Returns:
        str or int: GPU memory in GB, or '8' if no GPU is available.
    """
    if torch.cuda.is_available():
        try:
            gpu_properties = torch.cuda.get_device_properties(gpu)
            total_memory_gb = round(gpu_properties.total_memory / 1024 / 1024 / 1024)
            return total_memory_gb
        except RuntimeError as error:
            logging.error(f"Failed to get VRAM for GPU {gpu}: {error}")
            return "8"
    return "8"

def get_gpu_info():
    """
    Get information about available GPUs.

    Returns:
        str: Formatted string with GPU information or a message if no GPUs are available.
    """
    ngpu = torch.cuda.device_count()
    gpu_infos = []
    if torch.cuda.is_available() and ngpu > 0:
        for i in range(ngpu):
            gpu_name = torch.cuda.get_device_name(i)
            mem = int(
                torch.cuda.get_device_properties(i).total_memory / 1024 / 1024 / 1024
                + 0.4
            )
            gpu_infos.append(f"{i}: {gpu_name} ({mem} GB)")
        return "\n".join(gpu_infos)
    return "No compatible GPU available."

def get_number_of_gpus():
    """
    Get a string representing available GPU indices.

    Returns:
        str: Dash-separated GPU indices (e.g., '0-1-2') or '-' if no GPUs.
    """
    if torch.cuda.is_available():
        num_gpus = torch.cuda.device_count()
        return "-".join(map(str, range(num_gpus)))
    return "-"

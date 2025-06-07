import os
import sys
import logging
import warnings
import pkg_resources
import numpy as np
import re
import unicodedata
import wget
from torch import nn
import librosa
import soundfile as sf
import soxr
from transformers import HubertModel

# Suppress warnings from specific libraries
warnings.filterwarnings("ignore")
logging.getLogger("fairseq").setLevel(logging.ERROR)
logging.getLogger("faiss.loader").setLevel(logging.ERROR)
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("torch").setLevel(logging.ERROR)

# Determine platform-specific stftpitchshift executable
try:
    stft_resource = pkg_resources.resource_filename('bfrvc.unit.tools', 'stftpitchshift')
    stft = stft_resource + '.exe' if sys.platform == 'win32' else stft_resource
except Exception as e:
    logging.error(f"Could not locate stftpitchshift: {e}")
    stft = None

class HubertModelWithFinalProj(HubertModel):
    def __init__(self, config):
        super().__init__(config)
        self.final_proj = nn.Linear(config.hidden_size, config.classifier_proj_size)

def load_audio(file, sample_rate):
    """
    Load an audio file and resample it to the specified sample rate.
    
    Args:
        file (str): Path to the audio file.
        sample_rate (int): Target sample rate.
        
    Returns:
        np.ndarray: Flattened audio array.
        
    Raises:
        RuntimeError: If an error occurs during audio loading.
    """
    try:
        file = file.strip(" ").strip('"').strip("\n").strip('"').strip(" ")
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        audio, sr = sf.read(file)
        if len(audio.shape) > 1:
            audio = librosa.to_mono(audio.T)
        if sr != sample_rate:
            audio = librosa.resample(
                audio, orig_sr=sr, target_sr=sample_rate, res_type="soxr_vhq"
            )
        return audio.flatten()
    except Exception as error:
        raise RuntimeError(f"An error occurred loading the audio: {error}")

def load_audio_infer(file, sample_rate, **kwargs):
    """
    Load and process an audio file for inference, optionally applying formant shifting.
    
    Args:
        file (str): Path to the audio file.
        sample_rate (int): Target sample rate.
        **kwargs: Additional arguments including formant_shifting, formant_qfrency, formant_timbre.
        
    Returns:
        np.ndarray: Flattened audio array.
        
    Raises:
        RuntimeError: If an error occurs during audio loading or processing.
    """
    formant_shifting = kwargs.get("formant_shifting", False)
    try:
        file = file.strip(" ").strip('"').strip("\n").strip('"').strip(" ")
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        audio, sr = sf.read(file)
        if len(audio.shape) > 1:
            audio = librosa.to_mono(audio.T)
        if sr != sample_rate:
            audio = librosa.resample(
                audio, orig_sr=sr, target_sr=sample_rate, res_type="soxr_vhq"
            )
        if formant_shifting and stft:
            formant_qfrency = kwargs.get("formant_qfrency", 0.8)
            formant_timbre = kwargs.get("formant_timbre", 0.8)
            try:
                from stftpitchshift import StftPitchShift
                pitchshifter = StftPitchShift(1024, 32, sample_rate)
                audio = pitchshifter.shiftpitch(
                    audio,
                    factors=1,
                    quefrency=formant_qfrency * 1e-3,
                    distortion=formant_timbre,
                )
            except ImportError:
                logging.warning("stftpitchshift not installed; skipping formant shifting.")
        return np.array(audio).flatten()
    except Exception as error:
        raise RuntimeError(f"An error occurred loading the audio: {error}")

def format_title(title):
    """
    Format a title string for safe use in filenames.
    
    Args:
        title (str): Input title string.
        
    Returns:
        str: Formatted title string.
    """
    formatted_title = unicodedata.normalize("NFC", title)
    formatted_title = re.sub(r"[\u2500-\u257F]+", "", formatted_title)
    formatted_title = re.sub(r"[^\w\s.-]", "", formatted_title, flags=re.UNICODE)
    formatted_title = re.sub(r"\s+", "_", formatted_title)
    return formatted_title

def load_embedding(embedder_model, custom_embedder=None):
    """
    Load a speaker embedding model.
    
    Args:
        embedder_model (str): Name of the embedder model (e.g., 'contentvec').
        custom_embedder (str, optional): Path to a custom embedder model.
        
    Returns:
        HubertModelWithFinalProj: Loaded embedding model.
    """
    embedder_root = os.path.expanduser("~/.bfrvc/models/embedders")
    embedding_list = {
        "contentvec": os.path.join(embedder_root, "contentvec"),
        "chinese-hubert-base": os.path.join(embedder_root, "chinese_hubert_base"),
        "japanese-hubert-base": os.path.join(embedder_root, "japanese_hubert_base"),
        "korean-hubert-base": os.path.join(embedder_root, "korean_hubert_base"),
    }

    online_embedders = {
        "contentvec": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/contentvec/pytorch_model.bin",
        "chinese-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/chinese_hubert_base/pytorch_model.bin",
        "japanese-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/japanese_hubert_base/pytorch_model.bin",
        "korean-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/korean_hubert_base/pytorch_model.bin",
    }

    config_files = {
        "contentvec": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/contentvec/config.json",
        "chinese-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/chinese_hubert_base/config.json",
        "japanese-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/japanese_hubert_base/config.json",
        "korean-hubert-base": "https://huggingface.co/IAHispano/Applio/resolve/main/Resources/embedders/korean_hubert_base/config.json",
    }

    if embedder_model == "custom":
        if custom_embedder and os.path.exists(custom_embedder):
            model_path = custom_embedder
        else:
            logging.warning(f"Custom embedder not found: {custom_embedder}, falling back to contentvec")
            model_path = embedding_list["contentvec"]
    else:
        model_path = embedding_list[embedder_model]
        bin_file = os.path.join(model_path, "pytorch_model.bin")
        json_file = os.path.join(model_path, "config.json")
        os.makedirs(model_path, exist_ok=True)
        if not os.path.exists(bin_file):
            url = online_embedders[embedder_model]
            logging.info(f"Downloading {url} to {bin_file}...")
            wget.download(url, out=bin_file)
        if not os.path.exists(json_file):
            url = config_files[embedder_model]
            logging.info(f"Downloading {url} to {json_file}...")
            wget.download(url, out=json_file)

    try:
        models = HubertModelWithFinalProj.from_pretrained(model_path)
        return models
    except Exception as error:
        raise RuntimeError(f"Failed to load embedding model: {error}")

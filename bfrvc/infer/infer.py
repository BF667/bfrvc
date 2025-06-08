import os
import time
import logging
import traceback
import numpy as np
import librosa
import soundfile as sf
import soxr
import noisereduce as nr
import torch
from tempfile import gettempdir

from bfrvc.configs.config import Config
from bfrvc.unit.utils import load_audio_infer, load_embedding
from bfrvc.unit.tools.split_audio import process_audio, merge_audio
from bfrvc.infer.pipeline import Pipeline
from bfrvc.unit.algorithm.synthesizers import Synthesizer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("faiss").setLevel(logging.WARNING)
logging.getLogger("faiss.loader").setLevel(logging.WARNING)

# Base path for temporary files
base_path = os.path.expanduser("~/.bfrvc")

class VoiceConverter:
    """
    A class for performing voice conversion using Retrieval-Based Voice Conversion (RVC).
    """
    def __init__(self):
        """
        Initialize the VoiceConverter with configuration and model placeholders.

        Attributes:
            config: Configuration object.
            hubert_model: HuBERT model for speaker embedding extraction.
            last_embedder_model: Last used embedder model path.
            tgt_sr: Target sampling rate for output audio.
            net_g: Generator network for voice conversion.
            vc: Voice conversion pipeline instance.
            cpt: Checkpoint for model weights.
            version: Model version.
            n_spk: Number of speakers in the model.
            use_f0: Whether the model uses F0.
        """
        self.config = Config()
        self.hubert_model = None
        self.last_embedder_model = None
        self.tgt_sr = None
        self.net_g = None
        self.vc = None
        self.cpt = None
        self.version = None
        self.n_spk = None
        self.use_f0 = None

    def load_hubert(self, embedder_model: str, embedder_model_custom: str = None):
        """
        Load the HuBERT model for speaker embedding extraction.

        Args:
            embedder_model: Path to the pre-trained HuBERT model or model name.
            embedder_model_custom: Path to a custom HuBERT model.

        Raises:
            RuntimeError: If model loading fails.
        """
        try:
            self.hubert_model = load_embedding(embedder_model, embedder_model_custom)
            self.hubert_model = self.hubert_model.to(self.config.device).float()
            self.hubert_model.eval()
            logging.info(f"Loaded HuBERT model: {embedder_model}")
        except Exception as error:
            logging.error(f"Failed to load HuBERT model: {error}")
            raise RuntimeError(f"Failed to load HuBERT model: {error}")

    @staticmethod
    def remove_audio_noise(data: np.ndarray, sr: int, reduction_strength: float = 0.7) -> np.ndarray:
        """
        Remove noise from audio using noisereduce.

        Args:
            data: Audio data as a NumPy array.
            sr: Sample rate of the audio.
            reduction_strength: Strength of noise reduction (0 to 1).

        Returns:
            np.ndarray: Noise-reduced audio.

        Raises:
            RuntimeError: If noise reduction fails.
        """
        try:
            return nr.reduce_noise(y=data, sr=sr, prop_decrease=reduction_strength)
        except Exception as error:
            logging.error(f"Failed to remove audio noise: {error}")
            raise RuntimeError(f"Failed to remove audio noise: {error}")

    @staticmethod
    def convert_audio_format(input_path: str, output_path: str, output_format: str) -> str:
        """
        Convert audio file to a specified format.

        Args:
            input_path: Path to input audio file.
            output_path: Path to output audio file.
            output_format: Desired audio format (e.g., "WAV", "MP3").

        Returns:
            str: Path to converted audio file.

        Raises:
            FileNotFoundError: If input file is missing.
            RuntimeError: If conversion fails.
        """
        try:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input audio file not found: {input_path}")
            if output_format.upper() != "WAV":
                logging.info(f"Converting audio to {output_format}...")
                audio, sample_rate = librosa.load(input_path, sr=None)
                common_sample_rates = [8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000]
                target_sr = min(common_sample_rates, key=lambda x: abs(x - sample_rate))
                audio = librosa.resample(audio, orig_sr=sample_rate, target_sr=target_sr, res_type="soxr_vhq")
                sf.write(output_path, audio, target_sr, format=output_format.lower())
            else:
                output_path = input_path
            return output_path
        except Exception as error:
            logging.error(f"Failed to convert audio format: {error}")
            raise RuntimeError(f"Failed to convert audio format: {error}")

    def convert_audio(
        self,
        audio_input_path: str,
        audio_output_path: str,
        model_path: str,
        index_path: str,
        pitch: int = 0,
        f0_file: str = None,
        f0_method: str = "rmvpe",
        index_rate: float = 0.75,
        volume_envelope: float = 1,
        protect: float = 0.5,
        hop_length: int = 128,
        split_audio: bool = False,
        f0_autotune: bool = False,
        f0_autotune_strength: float = 1,
        embedder_model: str = "contentvec",
        embedder_model_custom: str = None,
        clean_audio: bool = False,
        clean_strength: float = 0.5,
        export_format: str = "WAV",
        post_process: bool = False,
        resample_sr: int = 0,
        sid: int = 0,
        **kwargs,
    ):
        """
        Perform voice conversion on an audio file.

        Args:
            audio_input_path: Path to input audio file.
            audio_output_path: Path to output audio file.
            model_path: Path to voice conversion model (.pth file).
            index_path: Path to FAISS index file.
            pitch: Pitch adjustment in semitones.
            f0_file: Path to F0 contour file.
            f0_method: F0 estimation method (e.g., "rmvpe").
            index_rate: Blending rate for speaker embeddings (0 to 1).
            volume_envelope: RMS blending rate (0 to 1).
            protect: Pitch protection level (0 to 0.5).
            hop_length: Hop length for F0 estimation.
            split_audio: Whether to split audio into chunks.
            f0_autotune: Whether to apply autotune to F0.
            f0_autotune_strength: Autotune strength (0 to 1).
            embedder_model: HuBERT model name or path.
            embedder_model_custom: Path to custom HuBERT model.
            clean_audio: Whether to apply noise reduction.
            clean_strength: Noise reduction strength (0 to 1).
            export_format: Output audio format (e.g., "WAV", "MP3").
            post_process: Whether to apply post-processing effects (unused).
            resample_sr: Resample output to this rate (0 to disable).
            sid: Speaker ID.
            **kwargs: Additional arguments for audio loading.

        Raises:
            FileNotFoundError: If input or model files are missing.
            RuntimeError: If conversion fails.
        """
        if not model_path or not os.path.exists(model_path):
            logging.error("No valid model path provided. Aborting conversion.")
            raise FileNotFoundError("No valid model path provided.")

        if not os.path.exists(audio_input_path):
            logging.error(f"Input audio file not found: {audio_input_path}")
            raise FileNotFoundError(f"Input audio file not found: {audio_input_path}")

        self.get_vc(model_path, sid)

        try:
            start_time = time.time()
            logging.info(f"Converting audio '{audio_input_path}'...")

            audio = load_audio_infer(audio_input_path, 16000, **kwargs)
            audio_max = np.abs(audio).max() / 0.95
            if audio_max > 1:
                audio /= audio_max

            if not self.hubert_model or embedder_model != self.last_embedder_model:
                self.load_hubert(embedder_model, embedder_model_custom)
                self.last_embedder_model = embedder_model

            file_index = (
                index_path.strip().strip('"').strip("\n").replace("trained", "added")
                if index_path and os.path.exists(index_path)
                else ""
            )

            if resample_sr >= 16000 and self.tgt_sr != resample_sr:
                self.tgt_sr = resample_sr

            if split_audio:
                chunks, intervals = process_audio(audio, 16000)
                logging.info(f"Audio split into {len(chunks)} chunks for processing.")
            else:
                chunks = [audio]

            converted_chunks = []
            for i, chunk in enumerate(chunks):
                audio_opt = self.vc.pipeline(
                    model=self.hubert_model,
                    net_g=self.net_g,
                    sid=sid,
                    audio=chunk,
                    input_audio_path=audio_input_path,
                    pitch=pitch,
                    f0_method=f0_method,
                    file_index=file_index,
                    index_rate=index_rate,
                    pitch_guidance=self.use_f0,
                    tgt_sr=self.tgt_sr,
                    resample_sr=resample_sr,
                    volume_envelope=volume_envelope,
                    version=self.version,
                    protect=protect,
                    hop_length=hop_length,
                    f0_autotune=f0_autotune,
                    f0_autotune_strength=f0_autotune_strength,
                    f0_file=f0_file,
                )
                converted_chunks.append(audio_opt)
                if split_audio:
                    logging.info(f"Converted audio chunk {i + 1}/{len(chunks)}")

            if split_audio:
                audio_opt = merge_audio(chunks, converted_chunks, intervals, 16000, self.tgt_sr)
            else:
                audio_opt = converted_chunks[0]

            if clean_audio:
                audio_opt = self.remove_audio_noise(audio_opt, self.tgt_sr, clean_strength)

            # Ensure output directory exists
            os.makedirs(os.path.dirname(audio_output_path) or ".", exist_ok=True)
            sf.write(audio_output_path, audio_opt, self.tgt_sr, format="WAV")
            output_path_format = audio_output_path.replace(".wav", f".{export_format.lower()}")
            audio_output_path = self.convert_audio_format(audio_output_path, output_path_format, export_format)

            elapsed_time = time.time() - start_time
            logging.info(f"Conversion completed at '{audio_output_path}' in {elapsed_time:.2f} seconds.")
        except Exception as error:
            logging.error(f"Error during audio conversion: {error}")
            logging.debug(traceback.format_exc())
            raise

    def convert_audio_batch(
        self,
        audio_input_paths: str,
        audio_output_path: str,
        **kwargs,
    ):
        """
        Perform voice conversion on a batch of audio files.

        Args:
            audio_input_paths: Directory containing input audio files.
            audio_output_path: Directory for output audio files.
            **kwargs: Additional arguments for convert_audio.

        Raises:
            FileNotFoundError: If input directory is invalid.
            RuntimeError: If batch conversion fails.
        """
        try:
            if not os.path.isdir(audio_input_paths):
                raise FileNotFoundError(f"Input directory not found: {audio_input_paths}")

            start_time = time.time()
            logging.info(f"Converting audio batch from '{audio_input_paths}'...")

            audio_extensions = (
                ".wav", ".mp3", ".flac", ".ogg", ".opus", ".m4a", ".mp4",
                ".aac", ".alac", ".wma", ".aiff", ".webm", ".ac3"
            )
            audio_files = [
                f for f in os.listdir(audio_input_paths)
                if f.lower().endswith(audio_extensions)
            ]
            logging.info(f"Detected {len(audio_files)} audio files for inference.")

            os.makedirs(audio_output_path, exist_ok=True)
            for audio_file in audio_files:
                input_path = os.path.join(audio_input_paths, audio_file)
                output_file = os.path.splitext(audio_file)[0] + "_output.wav"
                output_path = os.path.join(audio_output_path, output_file)
                if os.path.exists(output_path):
                    logging.info(f"Skipping existing output file: {output_path}")
                    continue
                self.convert_audio(
                    audio_input_path=input_path,
                    audio_output_path=output_path,
                    **kwargs,
                )

            elapsed_time = time.time() - start_time
            logging.info(f"Batch conversion completed in {elapsed_time:.2f} seconds.")
        except Exception as error:
            logging.error(f"Error during batch audio conversion: {error}")
            logging.debug(traceback.format_exc())
            raise

    def get_vc(self, weight_root: str, sid: int):
        """
        Load or update the voice conversion model and pipeline.

        Args:
            source: Path to model weights (.pth file).
            sid: Speaker ID.

        Raises:
            FileNotFoundError: If model file is missing.
            RuntimeError: If model loading fails.
        """
        try:
            if not weight_root or not os.path.exists(weight_root):
                self.cleanup_model()
                raise FileNotFoundError(f"Model weight file not found: {weight_root}")

            if not hasattr(self, 'loaded_model_path') or self.loaded_model_path != weight_root:
                self.load_model(weight_root)
                if self.cpt is not None:
                    self.setup_network()
                    self.setup_vc()
                self.loaded_model_path = weight_root

        except Exception as error:
            logging.error(f"Error loading voice conversion model: {error}")
            raise RuntimeError(f"Error loading voice conversion model: {error}")

    def cleanup_model(self):
        """
        Clean up model resources.

        Raises:
            RuntimeError: If cleanup fails.
        """
        try:
            if self.hubert_model is not None:
                del self.net_guy, self.hubert_model, self.volume_converter, self.vc, self.tgt_sr
                self.hubert_model = self.net_guy = self.volume_converter = self.vc = self.tgt_sr = None
            del self.net_guy, self.cpt
            self.cpt = None
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception as error:
            logging.error(f"Error cleaning up model resources: {error}")
            raise RuntimeError(f"Error cleaning up model resources: {error}")

    def load_model(self, weight_root: str):
        """
        Load model weights from a checkpoint file.

        Args:
            source: Path to model weights (.pth file).

        Raises:
            FileNotFoundError: If weight file is missing.
        """
        try:
            if not os.path.isfile(weight_root):
                raise FileNotFoundError(f"Model weight file not found: {weight_root}")
            self.cpt = torch.load(weight_root, map_location="cpu", weights_only=True)
            logging.info(f"Loaded model weights from {weight_root}")
        except Exception as error:
            logging.error(f"Failed to load model weights: {error}")
            raise

    def setup_network(self):
        """
        Set up the network configuration from the checkpoint.

        Raises:
            RuntimeError: If network setup fails.
        """
        try:
            if self.cpt is None:
                raise RuntimeError("No checkpoint loaded")
            self.tgt_sr = self.cpt["config"][-1]
            self.cpt["config"][-3] = self.cpt["weight"]["emb_g.weight"].shape[0]
            self.use_f0 = self.cpt.get("f0", 1)
            self.version = self.cpt.get("version", "v1")
            self.text_enc_hidden_dim = 768 if self.version == "v2" else 256
            self.vocoder = self.cpt.get("vocoder", "HiFi-GAN")
            self.net_guy = Synthesizer(
                *self.cpt["config"],
                use_f0=self.use_f0,
                text_enc_hidden_dim=self.text_enc_hidden_dim,
                vocoder=self.vocoder,
            )
            del self.net_guy.enc_q
            self.net_guy.load_state_dict(self.cpt["weight"], strict=False)
            self.net_guy = self.net_guy.to(self.config.device).float()
            self.net_guy.eval()
            logging.info(f"Set up network with version {self.version}, target SR {self.tgt_sr}")
        except Exception as error:
            logging.error(f"Failed to set up network: {error}")
            raise

    def setup_vc(self):
        """
        Set up the voice conversion pipeline instance.

        Raises:
            RuntimeError: If pipeline setup fails.
        """
        try:
            if self.cpt is None:
                raise RuntimeError("No checkpoint loaded")
            self.volume_converter = Pipeline(self.tgt_sr, self.config)
            self.n_spk = self.cpt["config"][-3]
            logging.info("Voice conversion pipeline set up")
        except Exception as error:
            logging.error(f"Failed to set up voice conversion pipeline: {error}")
            raise

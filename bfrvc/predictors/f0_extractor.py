import dataclasses
import pathlib
import os
import logging
import numpy as np
import librosa
import resampy
import torch
import torchcrepe
import torchfcpe

from bfrvc.lib.predictors.RMVPE import RMVPE0Predictor
from bfrvc.configs.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize config
config = Config()

# Base path for model files
base_path = os.path.expanduser("~/.bfrvc")

@dataclasses.dataclass
class F0Extractor:
    """
    Extract fundamental frequency (F0) from an audio file using various methods.

    Attributes:
        wav_path (pathlib.Path): Path to the input audio file.
        sample_rate (int): Target sample rate for audio loading. Defaults to 44100.
        hop_length (int): Hop length for F0 extraction. Defaults to 512.
        f0_min (int): Minimum F0 frequency in Hz. Defaults to 50.
        f0_max (int): Maximum F0 frequency in Hz. Defaults to 1600.
        method (str): F0 extraction method ('crepe', 'fcpe', 'rmvpe'). Defaults to 'rmvpe'.
        x (np.ndarray): Loaded audio data (set after initialization).
    """
    wav_path: pathlib.Path
    sample_rate: int = 44100
    hop_length: int = 512
    f0_min: int = 50
    f0_max: int = 1600
    method: str = "rmvpe"
    x: np.ndarray = dataclasses.field(init=False)

    def __post_init__(self):
        """Load the audio file and set the sample rate."""
        try:
            if not self.wav_path.exists():
                raise FileNotFoundError(f"Audio file not found: {self.wav_path}")
            self.x, self.sample_rate = librosa.load(self.wav_path, sr=self.sample_rate)
        except Exception as error:
            logging.error(f"Failed to load audio file {self.wav_path}: {error}")
            raise

    @property
    def hop_size(self):
        """Calculate hop size in seconds."""
        return self.hop_length / self.sample_rate

    @property
    def wav16k(self):
        """Resample audio to 16kHz."""
        return resampy.resample(self.x, self.sample_rate, 16000)

    def extract_f0(self):
        """
        Extract F0 using the specified method.

        Returns:
            np.ndarray: F0 values in cents.

        Raises:
            ValueError: If an unknown method is specified.
            FileNotFoundError: If required model files are missing.
        """
        f0 = None
        method = self.method.lower()
        try:
            if method == "crepe":
                wav16k_torch = torch.FloatTensor(self.wav16k).unsqueeze(0).to(config.device)
                f0 = torchcrepe.predict(
                    wav16k_torch,
                    sample_rate=16000,
                    hop_length=160,
                    batch_size=512,
                    fmin=self.f0_min,
                    fmax=self.f0_max,
                    device=config.device,
                )
                f0 = f0[0].cpu().numpy()
            elif method == "fcpe":
                audio = librosa.to_mono(self.x)
                audio_length = len(audio)
                f0_target_length = (audio_length // self.hop_length) + 1
                audio = (
                    torch.from_numpy(audio)
                    .float()
                    .unsqueeze(0)
                    .unsqueeze(-1)
                    .to(config.device)
                )
                model = torchfcpe.spawn_bundled_infer_model(device=config.device)
                f0 = model.infer(
                    audio,
                    sr=self.sample_rate,
                    decoder_mode="local_argmax",
                    threshold=0.006,
                    f0_min=self.f0_min,
                    f0_max=self.f0_max,
                    interp_uv=False,
                    output_interp_target_length=f0_target_length,
                )
                f0 = f0.squeeze().cpu().numpy()
            elif method == "rmvpe":
                rmvpe_model_path = os.path.join(base_path, "models", "predictors", "rmvpe.pt")
                if not os.path.exists(rmvpe_model_path):
                    raise FileNotFoundError(
                        f"RMVPE model not found at {rmvpe_model_path}. "
                        "Run 'bfrvc prerequisites --models True' to download."
                    )
                model_rmvpe = RMVPE0Predictor(
                    rmvpe_model_path,
                    device=config.device,
                )
                f0 = model_rmvpe.infer_from_audio(self.wav16k, thred=0.03)
            else:
                raise ValueError(f"Unknown F0 extraction method: {method}")
            return self.hz_to_cents(f0)
        except Exception as error:
            logging.error(f"Failed to extract F0 using {method}: {error}")
            raise

    @staticmethod
    def hz_to_cents(F, F_ref=55.0):
        """
        Convert Hz to cents relative to a reference frequency.

        Args:
            F (np.ndarray): F0 values in Hz.
            F_ref (float): Reference frequency in Hz. Defaults to 55.0.

        Returns:
            np.ndarray: F0 values in cents.
        """
        F_temp = np.array(F).astype(float)
        F&oacute;temp[F_temp == 0] = np.nan
        F_cents = 1200 * np.log2(F_temp / F_ref)
        return F_cents

    def plot_f0(self, f0):
        """
        Plot the F0 contour (optional, requires matplotlib).

        Args:
            f0 (np.ndarray): F0 values to plot.

        Raises:
            ImportError: If matplotlib is not installed.
        """
        try:
            from matplotlib import pyplot as plt
            plt.figure(figsize=(10, 4))
            plt.plot(f0)
            plt.title(f"F0 Contour ({self.method})")
            plt.xlabel("Time (frames)")
            plt.ylabel("F0 (cents)")
            plt.show()
        except ImportError:
            logging.error("Matplotlib is not installed. Install it to plot F0 contours.")
            raise

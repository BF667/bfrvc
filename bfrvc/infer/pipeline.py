import os
import gc
import re
import logging
import torch
import torch.nn.functional as F
import torchcrepe
import faiss
import librosa
import numpy as np
from scipy import signal
from torch import Tensor

from bfrvc.predictors.RMVPE import RMVPE0Predictor
from bfrvc.predictors.FCPE import FCPEF0Predictor

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("faiss").setLevel(logging.WARNING)

# Constants for audio processing
FILTER_ORDER = 5
CUTOFF_FREQUENCY = 48  # Hz
SAMPLE_RATE = 16000  # Hz
bh, ah = signal.butter(
    N=FILTER_ORDER, Wn=CUTOFF_FREQUENCY, btype="high", fs=SAMPLE_RATE
)

# Base path for model files
base_path = os.path.expanduser("~/.bfrvc")

class AudioProcessor:
    """
    A class for processing audio signals, specifically for adjusting RMS levels.
    """
    @staticmethod
    def change_rms(
        source_audio: np.ndarray,
        source_rate: int,
        target_audio: np.ndarray,
        target_rate: int,
        rate: float,
    ):
        """
        Adjust the RMS level of target_audio to match the RMS of source_audio.

        Args:
            source_audio: Source audio signal as a NumPy array.
            source_rate: Sampling rate of the source audio.
            target_audio: Target audio signal to adjust.
            target_rate: Sampling rate of the target audio.
            rate: Blending rate between source and target RMS levels.

        Returns:
            np.ndarray: Adjusted target audio.
        """
        try:
            rms1 = librosa.feature.rms(
                y=source_audio,
                frame_length=source_rate // 2 * 2,
                hop_length=source_rate // 2,
            )
            rms2 = librosa.feature.rms(
                y=target_audio,
                frame_length=target_rate // 2 * 2,
                hop_length=target_rate // 2,
            )

            rms1 = F.interpolate(
                torch.from_numpy(rms1).float().unsqueeze(0),
                size=target_audio.shape[0],
                mode="linear",
            ).squeeze()
            rms2 = F.interpolate(
                torch.from_numpy(rms2).float().unsqueeze(0),
                size=target_audio.shape[0],
                mode="linear",
            ).squeeze()
            rms2 = torch.maximum(rms2, torch.zeros_like(rms2) + 1e-6)

            adjusted_audio = (
                target_audio
                * (torch.pow(rms1, 1 - rate) * torch.pow(rms2, rate - 1)).numpy()
            )
            return adjusted_audio
        except Exception as error:
            logging.error(f"Error adjusting RMS: {error}")
            raise

class Autotune:
    """
    A class for applying autotune to a fundamental frequency (F0) contour.
    """
    def __init__(self, ref_freqs):
        """
        Initialize with reference frequencies.

        Args:
            ref_freqs: List of reference frequencies for musical notes.
        """
        self.note_dict = ref_freqs

    def autotune_f0(self, f0, f0_autotune_strength):
        """
        Autotune F0 by snapping frequencies to the closest reference frequency.

        Args:
            f0: Input F0 contour as a NumPy array.
            f0_autotune_strength: Strength of autotune effect (0 to 1).

        Returns:
            np.ndarray: Autotuned F0 contour.
        """
        autotuned_f0 = np.zeros_like(f0)
        for i, freq in enumerate(f0):
            closest_note = min(self.note_dict, key=lambda x: abs(x - freq))
            autotuned_f0[i] = freq + (closest_note - freq) * f0_autotune_strength
        return autotuned_f0

class Pipeline:
    """
    Main pipeline for voice conversion, including preprocessing, F0 estimation, and conversion.
    """
    def __init__(self, tgt_sr, config):
        """
        Initialize the pipeline with target sampling rate and configuration.

        Args:
            tgt_sr: Target sampling rate for output audio.
            config: Configuration object with device and model parameters.

        Raises:
            FileNotFoundError: If RMVPE model file is missing.
        """
        self.x_pad = config.x_pad
        self.x_query = config.x_query
        self.x_center = config.x_center
        self.x_max = config.x_max
        self.sample_rate = 16000
        self.window = 160
        self.t_pad = self.sample_rate * self.x_pad
        self.t_pad_tgt = tgt_sr * self.x_pad
        self.t_pad2 = self.t_pad * 2
        self.t_query = self.sample_rate * self.x_query
        self.t_center = self.sample_rate * self.x_center
        self.t_max = self.sample_rate * self.x_max
        self.time_step = self.window / self.sample_rate * 1000
        self.f0_min = 50
        self.f0_max = 1100
        self.f0_mel_min = 1127 * np.log(1 + self.f0_min / 700)
        self.f0_mel_max = 1127 * np.log(1 + self.f0_max / 700)
        self.device = config.device
        self.ref_freqs = [
            49.00,  # G1
            51.91,  # G#1 / Ab1
            55.00,  # A1
            # ... (same as original, omitted for brevity)
            1046.50,  # C6
        ]
        self.autotune = Autotune(self.ref_freqs)
        self.note_dict = self.autotune.note_dict

        rmvpe_model_path = os.path.join(base_path, "models", "predictors", "rmvpe.pt")
        if not os.path.exists(rmvpe_model_path):
            raise FileNotFoundError(
                f"RMVPE model not found at {rmvpe_model_path}. "
                "Run 'bfrvc prerequisites --models True' to download."
            )
        self.model_rmvpe = RMVPE0Predictor(
            rmvpe_model_path,
            device=self.device,
        )

    def get_f0_crepe(
        self,
        x,
        f0_min,
        f0_max,
        p_len,
        hop_length,
        model="full",
    ):
        """
        Estimate F0 using the Crepe model.

        Args:
            x: Input audio signal as a NumPy array.
            f0_min: Minimum F0 value.
            f0_max: Maximum F0 value.
            p_len: Desired F0 output length.
            hop_length: Hop length for Crepe model.
            model: Crepe model size ("full" or "tiny").

        Returns:
            np.ndarray: F0 contour.
        """
        try:
            x = x.astype(np.float32)
            x /= np.quantile(np.abs(x), 0.999)
            audio = torch.from_numpy(x).to(self.device)
            audio = torch.unsqueeze(audio, dim=0)
            if audio.ndim == 2 and audio.shape[0] > 1:
                audio = torch.mean(audio, dim=0, keepdim=True).detach()
            pitch = torchcrepe.predict(
                audio,
                self.sample_rate,
                hop_length,
                f0_min,
                f0_max,
                model,
                batch_size=hop_length * 2,
                device=self.device,
                pad=True,
            )
            p_len = p_len or x.shape[0] // hop_length
            source = pitch.squeeze(0).cpu().float().numpy()
            source[source < 0.001] = np.nan
            target = np.interp(
                np.arange(0, len(source) * p_len, len(source)) / p_len,
                np.arange(0, len(source)),
                source,
            )
            return np.nan_to_num(target)
        except Exception as error:
            logging.error(f"Error in Crepe F0 estimation: {error}")
            raise

    def get_f0_hybrid(
        self,
        methods_str,
        x,
        f0_min,
        f0_max,
        p_len,
        hop_length,
    ):
        """
        Estimate F0 using a hybrid approach combining multiple methods.

        Args:
            methods_str: String specifying methods (e.g., "hybrid[crepe+rmvpe]").
            x: Input audio signal as a NumPy array.
            f0_min: Minimum F0 value.
            f0_max: Maximum F0 value.
            p_len: Desired F0 output length.
            hop_length: Hop length for F0 estimation.

        Returns:
            np.ndarray: Median F0 contour from combined methods.
        """
        try:
            methods_match = re.search(r"hybrid\[(.+)\]", methods_str)
            if not methods_match:
                raise ValueError(f"Invalid hybrid method format: {methods_str}")
            methods = [method.strip() for method in methods_match.group(1).split("+")]
            logging.info(f"Calculating F0 with methods: {', '.join(methods)}")

            x = x.astype(np.float32)
            x /= np.quantile(np.abs(x), 0.999)
            f0_computation_stack = []
            for method in methods:
                f0 = None
                if method == "crepe":
                    f0 = self.get_f0_crepe(x, f0_min, f0_max, p_len, int(hop_length))
                elif method == "rmvpe":
                    f0 = self.model_rmvpe.infer_from_audio(x, thred=0.03)
                    f0 = f0[1:] if len(f0) > 1 else f0
                elif method == "fcpe":
                    fcpe_model_path = os.path.join(base_path, "models", "predictors", "fcpe.pt")
                    if not os.path.exists(fcpe_model_path):
                        raise FileNotFoundError(
                            f"FCPE model not found at {fcpe_model_path}. "
                            "Run 'bfrvc prerequisites --models True' to download."
                        )
                    self.model_fcpe = FCPEF0Predictor(
                        fcpe_model_path,
                        f0_min=int(f0_min),
                        f0_max=int(f0_max),
                        dtype=torch.float32,
                        device=self.device,
                        sample_rate=self.sample_rate,
                        threshold=0.03,
                    )
                    f0 = self.model_fcpe.compute_f0(x, p_len=p_len)
                    del self.model_fcpe
                    gc.collect()
                if f0 is not None:
                    f0_computation_stack.append(f0)
            
            if not f0_computation_stack:
                raise ValueError("No valid F0 estimates produced")
            return np.nanmedian(f0_computation_stack, axis=0) if len(f0_computation_stack) > 1 else f0_computation_stack[0]
        except Exception as error:
            logging.error(f"Error in hybrid F0 estimation: {error}")
            raise

    def get_f0(
        self,
        x,
        p_len,
        pitch,
        f0_method,
        hop_length,
        f0_autotune,
        f0_autotune_strength,
        inp_f0=None,
    ):
        """
        Estimate F0 using the specified method.

        Args:
            x: Input audio signal as a NumPy array.
            p_len: Desired F0 output length.
            pitch: Pitch adjustment in semitones.
            f0_method: F0 estimation method (e.g., "crepe", "rmvpe", "fcpe").
            hop_length: Hop length for F0 estimation.
            f0_autotune: Whether to apply autotune.
            f0_autotune_strength: Autotune strength (0 to 1).
            inp_f0: Optional input F0 contour.

        Returns:
            tuple: (coarse F0, original F0) as NumPy arrays.
        """
        try:
            f0 = None
            if f0_method == "crepe":
                f0 = self.get_f0_crepe(x, self.f0_min, self.f0_max, p_len, int(hop_length))
            elif f0_method == "crepe-tiny":
                f0 = self.get_f0_crepe(x, self.f0_min, self.f0_max, p_len, int(hop_length), "tiny")
            elif f0_method == "rmvpe":
                f0 = self.model_rmvpe.infer_from_audio(x, thred=0.03)
            elif f0_method == "fcpe":
                fcpe_model_path = os.path.join(base_path, "models", "predictors", "fcpe.pt")
                if not os.path.exists(fcpe_model_path):
                    raise FileNotFoundError(
                        f"FCPE model not found at {fcpe_model_path}. "
                        "Run 'bfrvc prerequisites --models True' to download."
                    )
                self.model_fcpe = FCPEF0Predictor(
                    fcpe_model_path,
                    f0_min=int(self.f0_min),
                    f0_max=int(self.f0_max),
                    dtype=torch.float32,
                    device=self.device,
                    sample_rate=self.sample_rate,
                    threshold=0.03,
                )
                f0 = self.model_fcpe.compute_f0(x, p_len=p_len)
                del self.model_fcpe
                gc.collect()
            elif "hybrid" in f0_method:
                f0 = self.get_f0_hybrid(f0_method, x, self.f0_min, self.f0_max, p_len, hop_length)

            if f0 is None:
                raise ValueError(f"F0 estimation failed for method: {f0_method}")

            if f0_autotune:
                f0 = self.autotune.autotune_f0(f0, f0_autotune_strength)

            f0 *= pow(2, pitch / 12)
            tf0 = self.sample_rate // self.window
            if inp_f0 is not None:
                delta_t = np.round(
                    (inp_f0[:, 0].max() - inp_f0[:, 0].min()) * tf0 + 1
                ).astype(np.int16)
                replace_f0 = np.interp(
                    list(range(delta_t)), inp_f0[:, 0] * 100, inp_f0[:, 1]
                )
                shape = f0[self.x_pad * tf0 : self.x_pad * tf0 + len(replace_f0)].shape[0]
                f0[self.x_pad * tf0 : self.x_pad * tf0 + len(replace_f0)] = replace_f0[:shape]

            f0_mel = 1127 * np.log(1 + f0 / 700)
            f0_mel[f0_mel > 0] = (f0_mel[f0_mel > 0] - self.f0_mel_min) * 254 / (
                self.f0_mel_max - self.f0_mel_min
            ) + 1
            f0_mel[f0_mel <= 1] = 1
            f0_mel[f0_mel > 255] = 255
            f0_coarse = np.rint(f0_mel).astype(int)

            return f0_coarse, f0
        except Exception as error:
            logging.error(f"Error in F0 estimation: {error}")
            raise

    def voice_conversion(
        self,
        model,
        net_g,
        sid,
        audio0,
        pitch,
        pitchf,
        index,
        big_npy,
        index_rate,
        version,
        protect,
    ):
        """
        Perform voice conversion on an audio segment.

        Args:
            model: Feature extractor model.
            net_g: Generative model for speech synthesis.
            sid: Speaker ID for target voice.
            audio0: Input audio segment.
            pitch: Quantized F0 contour.
            pitchf: Original F0 contour.
            index: FAISS index for speaker embedding retrieval.
            big_npy: Speaker embeddings array.
            index_rate: Blending rate for speaker embeddings.
            version: Model version ("v1" or other).
            protect: Pitch protection level (0 to 0.5).

        Returns:
            np.ndarray: Converted audio segment.
        """
        try:
            with torch.no_grad():
                pitch_guidance = pitch is not None and pitchf is not None
                feats = torch.from_numpy(audio0).float()
                feats = feats.mean(-1) if feats.dim() == 2 else feats
                assert feats.dim() == 1, f"Expected 1D tensor, got {feats.dim()}D"
                feats = feats.view(1, -1).to(self.device)

                feats = model(feats)["last_hidden_state"]
                feats = model.final_proj(feats[0]).unsqueeze(0) if version == "v1" else feats
                feats0 = feats.clone() if pitch_guidance else None

                if index is not None and index_rate > 0:
                    feats = self._retrieve_speaker_embeddings(feats, index, big_npy, index_rate)

                feats = F.interpolate(feats.permute(0, 2, 1), scale_factor=2).permute(0, 2, 1)
                p_len = min(audio0.shape[0] // self.window, feats.shape[1])

                if pitch_guidance:
                    feats0 = F.interpolate(feats0.permute(0, 2, 1), scale_factor=2).permute(0, 2, 1)
                    pitch = pitch[:, :p_len] if pitch is not None else None
                    pitchf = pitchf[:, :p_len] if pitchf is not None else None
                    if protect < 0.5:
                        pitchff = pitchf.clone()
                        pitchff[pitchf > 0] = 1
                        pitchff[pitchf < 1] = protect
                        feats = feats * pitchff.unsqueeze(-1) + feats0 * (1 - pitchff.unsqueeze(-1))
                        feats = feats.to(feats0.dtype)

                p_len = torch.tensor([p_len], device=self.device).long()
                audio1 = (
                    net_g.infer(feats.float(), p_len, pitch, pitchf.float() if pitch_guidance else None, sid)[0][0, 0]
                    .data.cpu()
                    .float()
                    .numpy()
                )

                del feats, feats0, p_len
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                return audio1
        except Exception as error:
            logging.error(f"Error in voice conversion: {error}")
            raise

    def _retrieve_speaker_embeddings(self, feats, index, big_npy, index_rate):
        """
        Retrieve speaker embeddings using FAISS index.

        Args:
            feats: Input features tensor.
            index: FAISS index.
            big_npy: Speaker embeddings array.
            index_rate: Blending rate for embeddings.

        Returns:
            torch.Tensor: Blended feature embeddings.
        """
        npy = feats[0].cpu().numpy()
        score, ix = index.search(npy, k=8)
        weight = np.square(1 / score)
        weight /= weight.sum(axis=1, keepdims=True)
        npy = np.sum(big_npy[ix] * np.expand_dims(weight, axis=2), axis=1)
        return (
            torch.from_numpy(npy).unsqueeze(0).to(self.device) * index_rate
            + (1 - index_rate) * feats
        )

    def pipeline(
        self,
        model,
        net_g,
        sid,
        audio,
        input_audio_path,
        pitch,
        f0_method,
        file_index,
        index_rate,
        pitch_guidance,
        tgt_sr,
        resample_sr,
        volume_envelope,
        version,
        protect,
        hop_length,
        f0_autotune,
        f0_autotune_strength,
        f0_file,
    ):
        """
        Main voice conversion pipeline.

        Args:
            model: Feature extractor model.
            net_g: Generative model for speech synthesis.
            sid: Speaker ID.
            audio: Input audio signal.
            input_audio_path: Path to input audio file.
            pitch: Pitch adjustment in semitones.
            f0_method: F0 estimation method.
            file_index: Path to FAISS index file.
            index_rate: Blending rate for speaker embeddings.
            pitch_guidance: Whether to use pitch guidance.
            tgt_sr: Target sampling rate.
            resample_sr: Resampling rate for output.
            volume_envelope: RMS blending rate.
            version: Model version.
            protect: Pitch protection level.
            hop_length: Hop length for F0 estimation.
            f0_autotune: Whether to apply autotune.
            f0_autotune_strength: Autotune strength.
            f0_file: Path to F0 contour file.

        Returns:
            np.ndarray: Converted audio signal.
        """
        try:
            index = big_npy = None
            if file_index and os.path.exists(file_index) and index_rate > 0:
                try:
                    index = faiss.read_index(file_index)
                    big_npy = index.reconstruct_n(0, index.ntotal)
                except Exception as error:
                    logging.error(f"Error reading FAISS index: {error}")
                    index = big_npy = None

            audio = signal.filtfilt(bh, ah, audio)
            audio_pad = np.pad(audio, (self.window // 2, self.window // 2), mode="reflect")
            opt_ts = []
            if audio_pad.shape[0] > self.t_max:
                audio_sum = np.zeros_like(audio)
                for i in range(self.window):
                    audio_sum += audio_pad[i : i - self.window]
                for t in range(self.t_center, audio.shape[0], self.t_center):
                    opt_ts.append(
                        t
                        - self.t_query
                        + np.where(
                            np.abs(audio_sum[t - self.t_query : t + self.t_query])
                            == np.abs(audio_sum[t - self.t_query : t + self.t_query]).min()
                        )[0][0]
                    )

            audio_pad = np.pad(audio, (self.t_pad, self.t_pad), mode="reflect")
            p_len = audio_pad.shape[0] // self.window
            inp_f0 = None
            if f0_file and hasattr(f0_file, "name") and os.path.exists(f0_file.name):
                try:
                    with open(f0_file.name, "r") as f:
                        lines = f.read().strip().split("\n")
                    inp_f0 = np.array([[float(i) for i in line.split(",")] for line in lines], dtype="float32")
                except Exception as error:
                    logging.error(f"Error reading F0 file {f0_file.name}: {error}")

            sid = torch.tensor(sid, device=self.device).unsqueeze(0).long()
            pitch_tensor = pitchf_tensor = None
            if pitch_guidance:
                pitch_tensor, pitchf_tensor = self.get_f0(
                    audio_pad,
                    p_len,
                    pitch,
                    f0_method,
                    hop_length,
                    f0_autotune,
                    f0_autotune_strength,
                    inp_f0,
                )
                pitch_tensor = pitch_tensor[:p_len]
                pitchf_tensor = pitchf_tensor[:p_len]
                if self.device == "mps":
                    pitchf_tensor = pitchf_tensor.astype(np.float32)
                pitch_tensor = torch.tensor(pitch_tensor, device=self.device).unsqueeze(0).long()
                pitchf_tensor = torch.tensor(pitchf_tensor, device=self.device).unsqueeze(0).float()

            s = 0
            audio_opt = []
            for t in opt_ts:
                t = t // self.window * self.window
                audio_segment = audio_pad[s : t + self.t_pad2 + self.window]
                pitch_segment = (
                    pitch_tensor[:, s // self.window : (t + self.t_pad2) // self.window]
                    if pitch_guidance else None
                )
                pitchf_segment = (
                    pitchf_tensor[:, s // self.window : (t + self.t_pad2) // self.window]
                    if pitch_guidance else None
                )
                audio_opt.append(
                    self.voice_conversion(
                        model,
                        net_g,
                        sid,
                        audio_segment,
                        pitch_segment,
                        pitchf_segment,
                        index,
                        big_npy,
                        index_rate,
                        version,
                        protect,
                    )[self.t_pad_tgt : -self.t_pad_tgt]
                )
                s = t

            audio_segment = audio_pad[t:] if t is not None else audio_pad
            pitch_segment = pitch_tensor[:, t // self.window :] if pitch_guidance and t is not None else pitch_tensor
            pitchf_segment = pitchf_tensor[:, t // self.window :] if pitch_guidance and t is not None else pitchf_tensor
            audio_opt.append(
                self.voice_conversion(
                    model,
                    net_g,
                    sid,
                    audio_segment,
                    pitch_segment,
                    pitchf_segment,
                    index,
                    big_npy,
                    index_rate,
                    version,
                    protect,
                )[self.t_pad_tgt : -self.t_pad_tgt]
            )

            audio_opt = np.concatenate(audio_opt)
            if volume_envelope != 1:
                audio_opt = AudioProcessor.change_rms(
                    audio, self.sample_rate, audio_opt, self.sample_rate, volume_envelope
                )

            audio_max = np.abs(audio_opt).max() / 0.99
            if audio_max > 1:
                audio_opt /= audio_max

            if resample_sr > 0:
                audio_opt = librosa.resample(audio_opt, orig_sr=tgt_sr, target_sr=resample_sr)

            del pitch_tensor, pitchf_tensor, sid, index, big_npy
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            return audio_opt
        except Exception as error:
            logging.error(f"Error in pipeline execution: {error}")
            raise

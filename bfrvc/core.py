import os
import sys
import json
import argparse
import subprocess
from functools import lru_cache
from distutils.util import strtobool
import pkg_resources

# Define and ensure configuration directory exists
CONFIG_DIR = os.path.expanduser("~/.bfrvc")
os.makedirs(CONFIG_DIR, exist_ok=True)

# Package imports
from bfrvc.unit.tools.config_dw import model_need
    
python = sys.executable



@lru_cache(maxsize=None)
def import_voice_converter():
    try:
        from bfrvc.infer.infer import VoiceConverter
        return VoiceConverter()
    except ImportError as e:
        print(f"Error importing VoiceConverter: {e}")
        sys.exit(1)

@lru_cache(maxsize=1)
def get_config():
    try:
        from bfrvc.configs.config import Config
        return Config()
    except ImportError as e:
        print(f"Error importing Config: {e}")
        sys.exit(1)

# Infer
def run_infer_script(
    pitch: int,
    index_rate: float,
    volume_envelope: int,
    protect: float,
    hop_length: int,
    f0_method: str,
    input_path: str,
    output_path: str,
    pth_path: str,
    index_path: str,
    split_audio: bool,
    f0_autotune: bool,
    f0_autotune_strength: float,
    clean_audio: bool,
    clean_strength: float,
    export_format: str,
    f0_file: str,
    embedder_model: str,
    embedder_model_custom: str = None,
    sid: int = 0,
):
    # Validate input/output paths
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input audio file not found: {input_path}")
    if not os.path.exists(pth_path):
        raise FileNotFoundError(f"Model file not found: {pth_path}")
    if not os.path.exists(index_path):
        raise FileNotFoundError(f"Index file not found: {index_path}")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    kwargs = {
        "audio_input_path": input_path,
        "audio_output_path": output_path,
        "model_path": pth_path,
        "index_path": index_path,
        "pitch": pitch,
        "index_rate": index_rate,
        "volume_envelope": volume_envelope,
        "protect": protect,
        "hop_length": hop_length,
        "f0_method": f0_method,
        "split_audio": split_audio,
        "f0_autotune": f0_autotune,
        "f0_autotune_strength": f0_autotune_strength,
        "clean_audio": clean_audio,
        "clean_strength": clean_strength,
        "export_format": export_format,
        "f0_file": f0_file,
        "embedder_model": embedder_model,
        "embedder_model_custom": embedder_model_custom,
        "sid": sid,
    }
    infer_pipeline = import_voice_converter()
    infer_pipeline.convert_audio(**kwargs)
    return f"File {input_path} inferred successfully.", output_path.replace(
        ".wav", f".{export_format.lower()}"
    )

# Batch infer
def run_batch_infer_script(
    pitch: int,
    index_rate: float,
    volume_envelope: int,
    protect: float,
    hop_length: int,
    f0_method: str,
    input_folder: str,
    output_folder: str,
    pth_path: str,
    index_path: str,
    split_audio: bool,
    f0_autotune: bool,
    f0_autotune_strength: float,
    clean_audio: bool,
    clean_strength: float,
    export_format: str,
    f0_file: str,
    embedder_model: str,
    embedder_model_custom: str = None,
    sid: int = 0,
):
    # Validate input/output folders
    if not os.path.isdir(input_folder):
        raise NotADirectoryError(f"Input folder not found: {input_folder}")
    if not os.path.exists(pth_path):
        raise FileNotFoundError(f"Model file not found: {pth_path}")
    if not os.path.exists(index_path):
        raise FileNotFoundError(f"Index file not found: {index_path}")
    os.makedirs(output_folder, exist_ok=True)

    kwargs = {
        "audio_input_paths": input_folder,
        "audio_output_path": output_folder,
        "model_path": pth_path,
        "index_path": index_path,
        "pitch": pitch,
        "index_rate": index_rate,
        "volume_envelope": volume_envelope,
        "protect": protect,
        "hop_length": hop_length,
        "f0_method": f0_method,
        "split_audio": split_audio,
        "f0_autotune": f0_autotune,
        "f0_autotune_strength": f0_autotune_strength,
        "clean_audio": clean_audio,
        "clean_strength": clean_strength,
        "export_format": export_format,
        "f0_file": f0_file,
        "embedder_model": embedder_model,
        "embedder_model_custom": embedder_model_custom,
        "sid": sid,
    }
    infer_pipeline = import_voice_converter()
    infer_pipeline.convert_audio_batch(**kwargs)
    return f"Files from {input_folder} inferred successfully."



# Prerequisites
def run_prerequisites_script(
    models: bool,
    exe: bool,
):
    model_need(models, exe)
    return "Prerequisites installed successfully."

# Parse arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="BF RVC Fork 😺")
    subparsers = parser.add_subparsers(title="parser", dest="command", help="Choose a mode")

    # Parser for 'infer' mode
    infer_parser = subparsers.add_parser("infer", help="Run inference")
    pitch_description = "Set the pitch of the audio. Higher values result in a higher pitch."
    infer_parser.add_argument("--pitch", type=int, help=pitch_description, choices=range(-24, 25), default=0)
    index_rate_description = "Control the influence of the index file on the output."
    infer_parser.add_argument("--index_rate", type=float, help=index_rate_description, choices=[i / 100.0 for i in range(0, 101)], default=0.3)
    volume_envelope_description = "Control the blending of the output's volume envelope."
    infer_parser.add_argument("--volume_envelope", type=float, help=volume_envelope_description, choices=[i / 100.0 for i in range(0, 101)], default=1)
    protect_description = "Protect consonants and breathing sounds from artifacts."
    infer_parser.add_argument("--protect", type=float, help=protect_description, choices=[i / 1000.0 for i in range(0, 501)], default=0.33)
    hop_length_description = "Determines the time it takes for the system to react to a pitch change (Crepe only)."
    infer_parser.add_argument("--hop_length", type=int, help=hop_length_description, choices=range(1, 513), default=128)
    f0_method_description = "Choose the pitch extraction algorithm for the conversion."
    infer_parser.add_argument("--f0_method", type=str, help=f0_method_description, choices=[
        "crepe", "crepe-tiny", "rmvpe", "fcpe", "hybrid[crepe+rmvpe]", "hybrid[crepe+fcpe]",
        "hybrid[rmvpe+fcpe]", "hybrid[crepe+rmvpe+fcpe]"], default="rmvpe")
    infer_parser.add_argument("--input_path", type=str, help="Full path to the input audio file.", required=True)
    infer_parser.add_argument("--output_path", type=str, help="Full path to the output audio file.", required=True)
    pth_path_description = "Full path to the RVC model file (.pth)."
    infer_parser.add_argument("--pth_path", type=str, help=pth_path_description, required=True)
    index_path_description = "Full path to the index file (.index)."
    infer_parser.add_argument("--index_path", type=str, help=index_path_description, required=True)
    split_audio_description = "Split the audio into smaller segments before inference."
    infer_parser.add_argument("--split_audio", type=lambda x: bool(strtobool(x.lower())), choices=[True, False], help=split_audio_description, default=False)
    f0_autotune_description = "Apply a light autotune to the inferred audio."
    infer_parser.add_argument("--f0_autotune", type=lambda x: bool(strtobool(x.lower())), choices=[True, False], help=f0_autotune_description, default=False)
    f0_autotune_strength_description = "Set the autotune strength."
    infer_parser.add_argument("--f0_autotune_strength", type=float, help=f0_autotune_strength_description, choices=[i / 10 for i in range(11)], default=1.0)
    clean_audio_description = "Clean the output audio using noise reduction algorithms."
    infer_parser.add_argument("--clean_audio", type=lambda x: bool(strtobool(x.lower())), choices=[True, False], help=clean_audio_description, default=False)
    clean_strength_description = "Adjust the intensity of the audio cleaning process."
    infer_parser.add_argument("--clean_strength", type=float, help=clean_strength_description, choices=[i / 10 for i in range(11)], default=0.7)
    export_format_description = "Select the desired output audio format."
    infer_parser.add_argument("--export_format", type=str, help=export_format_description, choices=["WAV", "MP3", "FLAC", "OGG", "M4A"], default="WAV")
    embedder_model_description = "Choose the model used for generating speaker embeddings."
    infer_parser.add_argument("--embedder_model", type=str, help=embedder_model_description, choices=[
        "contentvec", "chinese-hubert-base", "japanese-hubert-base", "korean-hubert-base", "custom"], default="contentvec")
    embedder_model_custom_description = "Path to a custom model for speaker embedding."
    infer_parser.add_argument("--embedder_model_custom", type=str, help=embedder_model_custom_description, default=None)
    f0_file_description = "Full path to an external F0 file (.f0)."
    infer_parser.add_argument("--f0_file", type=str, help=f0_file_description, default=None)
    sid_description = "Speaker ID for multi-speaker models."
    infer_parser.add_argument("--sid", type=int, help=sid_description, default=0)

    # Parser for 'batch_infer' mode
    batch_infer_parser = subparsers.add_parser("batch_infer", help="Run batch inference")
    batch_infer_parser.add_argument("--pitch", type=int, help=pitch_description, choices=range(-24, 25), default=0)
    batch_infer_parser.add_argument("--index_rate", type=float, help=index_rate_description, choices=[i / 100.0 for i in range(0, 101)], default=0.3)
    batch_infer_parser.add_argument("--volume_envelope", type=float, help=volume_envelope_description, choices=[i / 100.0 for i in range(0, 101)], default=1)
    batch_infer_parser.add_argument("--protect", type=float, help=protect_description, choices=[i / 1000.0 for i in range(0, 501)], default=0.33)
    batch_infer_parser.add_argument("--hop_length", type=int, help=hop_length_description, choices=range(1, 513), default=128)
    batch_infer_parser.add_argument("--f0_method", type=str, help=f0_method_description, choices=[
        "crepe", "crepe-tiny", "rmvpe", "fcpe", "hybrid[crepe+rmvpe]", "hybrid[crepe+fcpe]",
        "hybrid[rmvpe+fcpe]", "hybrid[crepe+rmvpe+fcpe]"], default="rmvpe")
    batch_infer_parser.add_argument("--input_folder", type=str, help="Path to the folder containing input audio files.", required=True)
    batch_infer_parser.add_argument("--output_folder", type=str, help="Path to the folder for saving output audio files.", required=True)
    batch_infer_parser.add_argument("--pth_path", type=str, help=pth_path_description, required=True)# Parser for 'tts' mode
    
    # Parser for 'prerequisites' mode
    prerequisites_parser = subparsers.add_parser("prerequisites", help="Install prerequisites for RVC.")
    prerequisites_parser.add_argument("--models", type=lambda x: bool(strtobool(x.lower())), choices=[True, False], default=True, help="Download additional models.")
    prerequisites_parser.add_argument("--exe", type=lambda x: bool(strtobool(x.lower())), choices=[True, False], default=True, help="Download required executables.")

    return parser.parse_args()

def main():
    if len(sys.argv) == 1:
        print("⚠️: Please run the script with -h for more information.")
        return

    args = parse_arguments()

    try:
        if args.command == "infer":
            result, output_path = run_infer_script(
                pitch=args.pitch,
                index_rate=args.index_rate,
                volume_envelope=args.volume_envelope,
                protect=args.protect,
                hop_length=args.hop_length,
                f0_method=args.f0_method,
                input_path=args.input_path,
                output_path=args.output_path,
                pth_path=args.pth_path,
                index_path=args.index_path,
                split_audio=args.split_audio,
                f0_autotune=args.f0_autotune,
                f0_autotune_strength=args.f0_autotune_strength,
                clean_audio=args.clean_audio,
                clean_strength=args.clean_strength,
                export_format=args.export_format,
                f0_file=args.f0_file,
                embedder_model=args.embedder_model,
                embedder_model_custom=args.embedder_model_custom,
                sid=args.sid,
            )
            print(result)
        elif args.command == "batch_infer":
            result = run_batch_infer_script(
                pitch=args.pitch,
                index_rate=args.index_rate,
                volume_envelope=args.volume_envelope,
                protect=args.protect,
                hop_length=args.hop_length,
                f0_method=args.f0_method,
                input_folder=args.input_folder,
                output_folder=args.output_folder,
                pth_path=args.pth_path,
                index_path=args.index_path,
                split_audio=args.split_audio,
                f0_autotune=args.f0_autotune,
                f0_autotune_strength=args.f0_autotune_strength,
                clean_audio=args.clean_audio,
                clean_strength=args.clean_strength,
                export_format=args.export_format,
                f0_file=args.f0_file,
                embedder_model=args.embedder_model,
                embedder_model_custom=args.embedder_model_custom,
                sid=args.sid,
            )
            print(result)
        
        
        elif args.command == "prerequisites":
            result = run_prerequisites_script(
                models=args.models,
                exe=args.exe,
            )
            print(result)
    except Exception as error:
        print(f"An error occurred during execution: {error}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

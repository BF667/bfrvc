import os
import sys
import json
import argparse
import subprocess
from functools import lru_cache
from distutils.util import strtobool
import pkg_resources

# Package imports
from bfrvc.lib.tools.prerequisites_download import prequisites_download_pipeline
from bfrvc.lib.tools.model_download import model_download_pipeline

python = sys.executable

# Load tts_voices.json from package resources
@lru_cache(maxsize=1)
def load_voices_data():
    try:
        resource_path = pkg_resources.resource_filename('bfrvc.lib.tools', 'tts_voices.json')
        with open(resource_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        raise FileNotFoundError(f"Could not load tts_voices.json: {e}")

voices_data = load_voices_data()
locales = list({voice["ShortName"] for voice in voices_data})

@lru_cache(maxsize=None)
def import_voice_converter():
    from bfrvc.infer.infer import VoiceConverter
    return VoiceConverter()

@lru_cache(maxsize=1)
def get_config():
    from bfrvc.configs.config import Config
    return Config()

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
    formant_shifting: bool = False,
    formant_qfrency: float = 1.0,
    formant_timbre: float = 1.0,
):
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
        "formant_shifting": formant_shifting,
        "formant_qfrency": formant_qfrency,
        "formant_timbre": formant_timbre,
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
    formant_shifting: bool = False,
    formant_qfrency: float = 1.0,
    formant_timbre: float = 1.0,
):
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
        "formant_shifting": formant_shifting,
        "formant_qfrency": formant_qfrency,
        "formant_timbre": formant_timbre,
    }
    infer_pipeline = import_voice_converter()
    infer_pipeline.convert_audio_batch(**kwargs)
    return f"Files from {input_folder} inferred successfully."

# TTS
def run_tts_script(
    tts_file: str,
    tts_text: str,
    tts_voice: str,
    tts_rate: int,
    pitch: int,
    index_rate: float,
    volume_envelope: int,
    protect: float,
    hop_length: int,
    f0_method: str,
    output_tts_path: str,
    output_rvc_path: str,
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
    tts_script_path = pkg_resources.resource_filename('bfrvc.lib.tools', 'tts.py')

    if os.path.exists(output_tts_path):
        os.remove(output_tts_path)

    command_tts = [
        str(python),
        tts_script_path,
        tts_file,
        tts_text,
        tts_voice,
        str(tts_rate),
        output_tts_path,
    ]
    subprocess.run(command_tts, check=True)
    infer_pipeline = import_voice_converter()
    infer_pipeline.convert_audio(
        pitch=pitch,
        index_rate=index_rate,
        volume_envelope=volume_envelope,
        protect=protect,
        hop_length=hop_length,
        f0_method=f0_method,
        audio_input_path=output_tts_path,
        audio_output_path=output_rvc_path,
        model_path=pth_path,
        index_path=index_path,
        split_audio=split_audio,
        f0_autotune=f0_autotune,
        f0_autotune_strength=f0_autotune_strength,
        clean_audio=clean_audio,
        clean_strength=clean_strength,
        export_format=export_format,
        f0_file=f0_file,
        embedder_model=embedder_model,
        embedder_model_custom=embedder_model_custom,
        sid=sid,
    )
    return f"Text {tts_text} synthesized successfully.", output_rvc_path.replace(
        ".wav", f".{export_format.lower()}"
    )

# Download
def run_download_script(model_link: str):
    model_download_pipeline(model_link)
    return "Model downloaded successfully."

# Prerequisites
def run_prerequisites_script(
    pretraineds_hifigan: bool,
    models: bool,
    exe: bool,
):
    prequisites_download_pipeline(pretraineds_hifigan, models, exe)
    return "Prerequisites installed successfully."

# Parse arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="BFRVC: Voice Conversion and TTS Tool")
    subparsers = parser.add_subparsers(title="subcommands", dest="mode", help="Choose a mode")

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
    infer_parser.add_argument("--split_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=split_audio_description, default=False)
    f0_autotune_description = "Apply a light autotune to the inferred audio."
    infer_parser.add_argument("--f0_autotune", type=lambda x: bool(strtobool(x)), choices=[True, False], help=f0_autotune_description, default=False)
    f0_autotune_strength_description = "Set the autotune strength."
    infer_parser.add_argument("--f0_autotune_strength", type=float, help=f0_autotune_strength_description, choices=[i / 10 for i in range(11)], default=1.0)
    clean_audio_description = "Clean the output audio using noise reduction algorithms."
    infer_parser.add_argument("--clean_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=clean_audio_description, default=False)
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
    formant_shifting_description = "Apply formant shifting to the input audio."
    infer_parser.add_argument("--formant_shifting", type=lambda x: bool(strtobool(x)), choices=[True, False], help=formant_shifting_description, default=False)
    formant_qfrency_description = "Control the frequency of the formant shifting effect."
    infer_parser.add_argument("--formant_qfrency", type=float, help=formant_qfrency_description, default=1.0)
    formant_timbre_description = "Control the timbre of the formant shifting effect."
    infer_parser.add_argument("--formant_timbre", type=float, help=formant_timbre_description, default=1.0)
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
    batch_infer_parser.add_argument("--pth_path", type=str, help=pth_path_description, required=True)
    batch_infer_parser.add_argument("--index_path", type=str, help=index_path_description, required=True)
    batch_infer_parser.add_argument("--split_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=split_audio_description, default=False)
    batch_infer_parser.add_argument("--f0_autotune", type=lambda x: bool(strtobool(x)), choices=[True, False], help=f0_autotune_description, default=False)
    batch_infer_parser.add_argument("--f0_autotune_strength", type=float, help=f0_autotune_strength_description, choices=[i / 10 for i in range(11)], default=1.0)
    batch_infer_parser.add_argument("--clean_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=clean_audio_description, default=False)
    batch_infer_parser.add_argument("--clean_strength", type=float, help=clean_strength_description, choices=[i / 10 for i in range(11)], default=0.7)
    batch_infer_parser.add_argument("--export_format", type=str, help=export_format_description, choices=["WAV", "MP3", "FLAC", "OGG", "M4A"], default="WAV")
    batch_infer_parser.add_argument("--embedder_model", type=str, help=embedder_model_description, choices=[
        "contentvec", "chinese-hubert-base", "japanese-hubert-base", "korean-hubert-base", "custom"], default="contentvec")
    batch_infer_parser.add_argument("--embedder_model_custom", type=str, help=embedder_model_custom_description, default=None)
    batch_infer_parser.add_argument("--f0_file", type=str, help=f0_file_description, default=None)
    batch_infer_parser.add_argument("--formant_shifting", type=lambda x: bool(strtobool(x)), choices=[True, False], help=formant_shifting_description, default=False)
    batch_infer_parser.add_argument("--formant_qfrency", type=float, help=formant_qfrency_description, default=1.0)
    batch_infer_parser.add_argument("--formant_timbre", type=float, help=formant_timbre_description, default=1.0)
    batch_infer_parser.add_argument("--sid", type=int, help=sid_description, default=0)

    # Parser for 'tts' mode
    tts_parser = subparsers.add_parser("tts", help="Run TTS inference")
    tts_parser.add_argument("--tts_file", type=str, help="File with a text to be synthesized", required=True)
    tts_parser.add_argument("--tts_text", type=str, help="Text to be synthesized", required=True)
    tts_parser.add_argument("--tts_voice", type=str, help="Voice to be used for TTS synthesis.", choices=locales, required=True)
    tts_parser.add_argument("--tts_rate", type=int, help="Control the speaking rate of the TTS.", choices=range(-100, 101), default=0)
    tts_parser.add_argument("--pitch", type=int, help=pitch_description, choices=range(-24, 25), default=0)
    tts_parser.add_argument("--index_rate", type=float, help=index_rate_description, choices=[i / 10 for i in range(11)], default=0.3)
    tts_parser.add_argument("--volume_envelope", type=float, help=volume_envelope_description, choices=[i / 10 for i in range(11)], default=1)
    tts_parser.add_argument("--protect", type=float, help=protect_description, choices=[i / 10 for i in range(6)], default=0.33)
    tts_parser.add_argument("--hop_length", type=int, help=hop_length_description, choices=range(1, 513), default=128)
    tts_parser.add_argument("--f0_method", type=str, help=f0_method_description, choices=[
        "crepe", "crepe-tiny", "rmvpe", "fcpe", "hybrid[crepe+rmvpe]", "hybrid[crepe+fcpe]",
        "hybrid[rmvpe+fcpe]", "hybrid[crepe+rmvpe+fcpe]"], default="rmvpe")
    tts_parser.add_argument("--output_tts_path", type=str, help="Full path to save the synthesized TTS audio.", required=True)
    tts_parser.add_argument("--output_rvc_path", type=str, help="Full path to save the voice-converted audio.", required=True)
    tts_parser.add_argument("--pth_path", type=str, help=pth_path_description, required=True)
    tts_parser.add_argument("--index_path", type=str, help=index_path_description, required=True)
    tts_parser.add_argument("--split_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=split_audio_description, default=False)
    tts_parser.add_argument("--f0_autotune", type=lambda x: bool(strtobool(x)), choices=[True, False], help=f0_autotune_description, default=False)
    tts_parser.add_argument("--f0_autotune_strength", type=float, help=f0_autotune_strength_description, choices=[i / 10 for i in range(11)], default=1.0)
    tts_parser.add_argument("--clean_audio", type=lambda x: bool(strtobool(x)), choices=[True, False], help=clean_audio_description, default=False)
    tts_parser.add_argument("--clean_strength", type=float, help=clean_strength_description, choices=[i / 10 for i in range(11)], default=0.7)
    tts_parser.add_argument("--export_format", type=str, help=export_format_description, choices=["WAV", "MP3", "FLAC", "OGG", "M4A"], default="WAV")
    tts_parser.add_argument("--embedder_model", type=str, help=embedder_model_description, choices=[
        "contentvec", "chinese-hubert-base", "japanese-hubert-base", "korean-hubert-base", "custom"], default="contentvec")
    tts_parser.add_argument("--embedder_model_custom", type=str, help=embedder_model_custom_description, default=None)
    tts_parser.add_argument("--f0_file", type=str, help=f0_file_description, default=None)

    # Parser for 'download' mode
    download_parser = subparsers.add_parser("download", help="Download a model from a provided link.")
    download_parser.add_argument("--model_link", type=str, help="Direct link to the model file.", required=True)

    # Parser for 'prerequisites' mode
    prerequisites_parser = subparsers.add_parser("prerequisites", help="Install prerequisites for RVC.")
    prerequisites_parser.add_argument("--pretraineds_hifigan", type=lambda x: bool(strtobool(x)), choices=[True, False], default=True, help="Download pretrained models for RVC v2.")
    prerequisites_parser.add_argument("--models", type=lambda x: bool(strtobool(x)), choices=[True, False], default=True, help="Download additional models.")
    prerequisites_parser.add_argument("--exe", type=lambda x: bool(strtobool(x)), choices=[True, False], default=True, help="Download required executables.")

    return parser.parse_args()

def main():
    if len(sys.argv) == 1:
        print("Please run the script with '-h' for more information.")
        sys.exit(1)

    args = parse_arguments()

    try:
        if args.mode == "infer":
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
                embedder_model=args.embedder_model,
                embedder_model_custom=args.embedder_model_custom,
                f0_file=args.f0_file,
                formant_shifting=args.formant_shifting,
                formant_qfrency=args.formant_qfrency,
                formant_timbre=args.formant_timbre,
                sid=args.sid,
            )
            print(result)
        elif args.mode == "batch_infer":
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
                embedder_model=args.embedder_model,
                embedder_model_custom=args.embedder_model_custom,
                f0_file=args.f0_file,
                formant_shifting=args.formant_shifting,
                formant_qfrency=args.formant_qfrency,
                formant_timbre=args.formant_timbre,
                sid=args.sid,
            )
            print(result)
        elif args.mode == "tts":
            result, output_path = run_tts_script(
                tts_file=args.tts_file,
                tts_text=args.tts_text,
                tts_voice=args.tts_voice,
                tts_rate=args.tts_rate,
                pitch=args.pitch,
                index_rate=args.index_rate,
                volume_envelope=args.volume_envelope,
                protect=args.protect,
                hop_length=args.hop_length,
                f0_method=args.f0_method,
                output_tts_path=args.output_tts_path,
                output_rvc_path=args.output_rvc_path,
                pth_path=args.pth_path,
                index_path=args.index_path,
                split_audio=args.split_audio,
                f0_autotune=args.f0_autotune,
                f0_autotune_strength=args.f0_autotune_strength,
                clean_audio=args.clean_audio,
                clean_strength=args.clean_strength,
                export_format=args.export_format,
                embedder_model=args.embedder_model,
                embedder_model_custom=args.embedder_model_custom,
                f0_file=args.f0_file,
            )
            print(result)
        elif args.mode == "download":
            result = run_download_script(model_link=args.model_link)
            print(result)
        elif args.mode == "prerequisites":
            result = run_prerequisites_script(
                pretraineds_hifigan=args.pretraineds_hifigan,
                models=args.models,
                exe=args.exe,
            )
            print(result)
    except Exception as error:
        print(f"An error occurred during execution: {error}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

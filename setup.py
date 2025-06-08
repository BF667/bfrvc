from setuptools import setup, find_packages

setup(
    name="bfrvc",
    version="0.3.2",
    description="A Retrieval-based Voice Conversion (RVC) fork",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="BF667",
    author_email="festyus09@gmail.com",
    license="MIT",
    keywords=["voice-conversion", "tts", "audio-processing", "rvc", "machine-learning"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Multimedia :: Sound/Audio :: Speech",
        "Topic :: Scientific/Engineering :: Artificial Intelligence"
    ],
    python_requires=">=3.8,<3.12",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.26.4",
        "requests>=2.31.0,<2.32.0",
        "tqdm>=4.66.1",
        "wget>=3.2",
        "ffmpeg-python>=0.2.0",
        "faiss-cpu>=1.7.3,<1.8.1",
        "librosa==0.10.0",
        "scipy>=1.11.1",
        "soundfile>=0.12.1",
        "noisereduce>=3.0.0",
        "pedalboard>=0.7.4",
        "stftpitchshift>=1.5.1",
        "soxr>=0.3.7",
        "omegaconf>=2.0.6",
        "numba>=0.60.0",
        "torch>=2.0.0,<2.8.0",
        "torchaudio>=2.0.0,<2.8.0",
        "torchvision>=0.17.0",
        "torchcrepe>=0.0.23",
        "torchfcpe",
        "einops",
        "transformers>=4.44.2",
        "matplotlib>=3.7.2",
        "tensorboard>=2.16.2",
        "certifi>=2023.7.22",
        "antlr4-python3-runtime>=4.8",
        "tensorboardX>=2.6.2",
        "edge-tts>=6.1.9",
        "pypresence>=4.3.0",
        "beautifulsoup4>=4.12.2"
    ],
    include_package_data=True,
    extras_require={
        "plotting": ["matplotlib>=3.7.2"]
    },
    project_urls={
        "Homepage": "https://github.com/BF667/bfrvc",
        "Repository": "https://github.com/BF667/bfrvc",
        "Issues": "https://github.com/BF667/bfrvc/issues"
    },
    entry_points={
        "console_scripts": [
            "bfrvc=bfrvc.core:main"
        ]
    }
)

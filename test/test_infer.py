import unittest
import numpy as np
import os
import tempfile
import soundfile as sf
from bfrvc.infer.infer import VoiceConverter

class TestVoiceConverter(unittest.TestCase):
    def setUp(self):
        self.converter = VoiceConverter()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.audio_data = np.random.randn(16000)
        self.audio_path = os.path.join(self.temp_dir.name, "test.wav")
        sf.write(self.audio_path, self.audio_data, 16000)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_remove_audio_noise(self):
        cleaned_audio = self.converter.remove_audio_noise(self.audio_data, 16000, 0.5)
        self.assertEqual(cleaned_audio.shape, self.audio_data.shape)
        self.assertTrue(np.all(np.isfinite(cleaned_audio)))

    def test_convert_audio_format(self):
        output_path = os.path.join(self.temp_dir.name, "test.mp3")
        result_path = self.converter.convert_audio_format(self.audio_path, output_path, "MP3")
        self.assertTrue(os.path.exists(result_path))
        self.assertEqual(result_path, output_path)

if __name__ == "__main__":
    unittest.main()

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestComputeFileMetadata(unittest.TestCase):
    def test_plain_text_file(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"hello world")
            path = Path(f.name)

        try:
            with (
                patch("magic.from_file", return_value="text/plain"),
                patch("pymediainfo.MediaInfo.parse") as mock_parse,
            ):
                mock_info = MagicMock()
                mock_info.image_tracks = []
                mock_info.video_tracks = []
                mock_info.general_tracks = []
                mock_parse.return_value = mock_info

                from wcpan.drive.feed._metadata import compute_file_metadata

                meta = compute_file_metadata(path)

            self.assertEqual(meta.mime_type, "text/plain")
            self.assertEqual(len(meta.hash), 32)  # MD5 hex
            self.assertEqual(meta.size, 11)
            self.assertFalse(meta.is_image)
            self.assertFalse(meta.is_video)
            self.assertEqual(meta.width, 0)
            self.assertEqual(meta.height, 0)
            self.assertEqual(meta.ms_duration, 0)
        finally:
            path.unlink(missing_ok=True)

    def test_image_file(self):
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(b"\xff\xd8\xff" + b"\x00" * 100)
            path = Path(f.name)

        try:
            with (
                patch("magic.from_file", return_value="image/jpeg"),
                patch("pymediainfo.MediaInfo.parse") as mock_parse,
            ):
                mock_track = MagicMock()
                mock_track.width = 1920
                mock_track.height = 1080
                mock_info = MagicMock()
                mock_info.image_tracks = [mock_track]
                mock_parse.return_value = mock_info

                from wcpan.drive.feed._metadata import compute_file_metadata

                meta = compute_file_metadata(path)

            self.assertEqual(meta.mime_type, "image/jpeg")
            self.assertTrue(meta.is_image)
            self.assertFalse(meta.is_video)
            self.assertEqual(meta.width, 1920)
            self.assertEqual(meta.height, 1080)
            self.assertEqual(meta.ms_duration, 0)
        finally:
            path.unlink(missing_ok=True)

    def test_video_file(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            f.write(b"\x00" * 200)
            path = Path(f.name)

        try:
            with (
                patch("magic.from_file", return_value="video/mp4"),
                patch("pymediainfo.MediaInfo.parse") as mock_parse,
            ):
                mock_video = MagicMock()
                mock_video.width = 1280
                mock_video.height = 720
                mock_general = MagicMock()
                mock_general.duration = 5000
                mock_info = MagicMock()
                mock_info.video_tracks = [mock_video]
                mock_info.general_tracks = [mock_general]
                mock_parse.return_value = mock_info

                from wcpan.drive.feed._metadata import compute_file_metadata

                meta = compute_file_metadata(path)

            self.assertEqual(meta.mime_type, "video/mp4")
            self.assertFalse(meta.is_image)
            self.assertTrue(meta.is_video)
            self.assertEqual(meta.width, 1280)
            self.assertEqual(meta.height, 720)
            self.assertEqual(meta.ms_duration, 5000)
        finally:
            path.unlink(missing_ok=True)

    def test_md5_hash_correctness(self):
        import hashlib

        with tempfile.NamedTemporaryFile(delete=False) as f:
            content = b"test content for hashing"
            f.write(content)
            path = Path(f.name)

        expected_hash = hashlib.md5(content).hexdigest()

        try:
            with (
                patch("magic.from_file", return_value="application/octet-stream"),
                patch("pymediainfo.MediaInfo.parse") as mock_parse,
            ):
                mock_info = MagicMock()
                mock_info.image_tracks = []
                mock_info.video_tracks = []
                mock_info.general_tracks = []
                mock_parse.return_value = mock_info

                from wcpan.drive.feed._metadata import compute_file_metadata

                meta = compute_file_metadata(path)

            self.assertEqual(meta.hash, expected_hash)
        finally:
            path.unlink(missing_ok=True)

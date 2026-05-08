from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import threading
import random
import time
from collections import deque
import queue

BASE_DIR = Path(__file__).resolve().parent
TRACKS_DIR = BASE_DIR / "tracks"
COMMERCIALS_DIR = BASE_DIR / "commercials"
MISC_DIR = BASE_DIR / "misc"
SILENCE_FILE = BASE_DIR / "silence.mp3"

CHUNK_SIZE = 4096
COMMERCIAL_CHANCE = 0.2
MISC_CHANCE = 0.1

# PCM timing (44.1kHz, stereo, 16-bit)
BYTES_PER_SECOND = 44100 * 2 * 2

listeners = []
listeners_lock = threading.Lock()

audio_queue = queue.Queue(maxsize=100)

history = deque(maxlen=4)


# ---------------------------
# Scheduling
# ---------------------------
def pick_next_track(tracks):
    available = [t for t in tracks if t not in history]
    if not available:
        available = tracks

    choice = random.choice(available)
    history.append(choice)
    return choice


last_was_misc = False


def pick_next_item(tracks, commercials, misc):
    global last_was_misc

    r = random.random()

    if misc and not last_was_misc and r < MISC_CHANCE:
        last_was_misc = True
        return random.choice(misc)

    last_was_misc = False

    if commercials and r < COMMERCIAL_CHANCE:
        return random.choice(commercials)

    return pick_next_track(tracks)


def scheduler():
    tracks = list(TRACKS_DIR.glob("*.mp3"))
    commercials = list(COMMERCIALS_DIR.glob("*.mp3"))
    misc = list(MISC_DIR.glob("*.mp3"))

    while True:
        item = pick_next_item(tracks, commercials, misc)

        if item in misc:
            yield item
            continue

        yield item
        yield SILENCE_FILE


# ---------------------------
# FFmpeg helpers
# ---------------------------
def start_decoder(file_path):
    return subprocess.Popen([
        "ffmpeg",
        "-i", str(file_path),
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-ac", "2",
        "-ar", "44100",
        "pipe:1"
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    bufsize=0)


def start_encoder():
    return subprocess.Popen([
        "ffmpeg",
        "-f", "s16le",
        "-ac", "2",
        "-ar", "44100",
        "-i", "pipe:0",
        "-vn",
        "-f", "mp3",
        "-b:a", "128k",
        "-flush_packets", "1",
        "-fflags", "nobuffer",
        "pipe:1"
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    bufsize=0)


# ---------------------------
# Audio Pipeline (REAL-TIME FIX)
# ---------------------------
def audio_pipeline(encoder):
    gen = scheduler()

    while True:
        file_path = next(gen)
        decoder = start_decoder(file_path)

        start_time = time.time()
        bytes_sent = 0

        while True:
            chunk = decoder.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            try:
                encoder.stdin.write(chunk)
            except BrokenPipeError:
                return

            bytes_sent += len(chunk)

            # real-time pacing
            expected_time = bytes_sent / BYTES_PER_SECOND
            actual_time = time.time() - start_time

            if expected_time > actual_time:
                time.sleep(expected_time - actual_time)

        decoder.wait()


# ---------------------------
# Broadcast (BUFFERED)
# ---------------------------
def broadcast_loop(encoder):
    while True:
        chunk = encoder.stdout.read(CHUNK_SIZE)
        if not chunk:
            continue

        try:
            audio_queue.put(chunk, timeout=1)
        except queue.Full:
            pass


def client_stream(wfile):
    while True:
        chunk = audio_queue.get()
        wfile.write(chunk)


# ---------------------------
# HTTP Server
# ---------------------------
class StreamHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/stream.mp3":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "audio/mpeg")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("icy-name", "Python Radio")
        self.send_header("icy-br", "128")
        self.end_headers()

        with listeners_lock:
            listeners.append(self.wfile)

        try:
            client_stream(self.wfile)
        finally:
            with listeners_lock:
                if self.wfile in listeners:
                    listeners.remove(self.wfile)


# ---------------------------
# Main
# ---------------------------
def run():
    encoder = start_encoder()

    threading.Thread(target=audio_pipeline, args=(encoder,), daemon=True).start()
    threading.Thread(target=broadcast_loop, args=(encoder,), daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", 8000), StreamHandler)
    print("Streaming at http://<ip>:8000/stream.mp3")
    server.serve_forever()


if __name__ == "__main__":
    run()

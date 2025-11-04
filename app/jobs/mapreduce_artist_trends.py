from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRTopArtistsByGen(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        if line.startswith("generation,"):
            return
        parts = line.rstrip("\n").split(",")
        if len(parts) < 3:
            return
        gen, artist, streams = parts[0], parts[1], parts[2]
        try:
            streams = int(streams)
        except Exception:
            return
        yield (gen or "unknown", artist or "Unknown Artist"), streams

    def reducer(self, key, values):
        gen, artist = key
        total = sum(values)
        yield None, {"generation": gen, "artist": artist, "total_streams": total}

if __name__ == "__main__":
    MRTopArtistsByGen.run()

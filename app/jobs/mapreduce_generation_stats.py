from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRGenStats(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        if line.startswith("generation,"):
            return
        parts = line.rstrip("\n").split(",")
        if len(parts) < 3:
            return
        gen, plat, streams = parts[0], parts[1], parts[2]
        try:
            streams = int(streams)
        except Exception:
            return
        yield (gen or "unknown", plat or "unknown"), streams

    def reducer(self, key, values):
        gen, plat = key
        total = sum(values)
        yield None, {"generation": gen, "platform": plat, "total_streams": total}

if __name__ == "__main__":
    MRGenStats.run()

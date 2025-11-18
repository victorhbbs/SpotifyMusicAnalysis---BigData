from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRTopArtistsByGen(MRJob):
    # define o protocolo de saída como JSON
    OUTPUT_PROTOCOL = JSONValueProtocol

    # mapeador
    def mapper(self, _, line):
        
        # ignora cabeçalho
        if line.startswith("generation,"):
            return
        parts = line.rstrip("\n").split(",")
        
        # verifica se há partes suficientes
        if len(parts) < 3:
            return
        
        # extrai geração, artista e streams
        gen, artist, streams = parts[0], parts[1], parts[2]
        
        # tenta converter streams para inteiro
        try:
            streams = int(streams)
        except Exception:
            return
        yield (gen or "unknown", artist or "Unknown Artist"), streams

    # redutor
    def reducer(self, key, values):
        # soma os valores de streams para a chave dada
        gen, artist = key
        total = sum(values)
        yield None, {"generation": gen, "artist": artist, "total_streams": total}

if __name__ == "__main__":
    MRTopArtistsByGen.run()

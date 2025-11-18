from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRGenStats(MRJob):
    # define o protocolo de saída como JSON
    OUTPUT_PROTOCOL = JSONValueProtocol

    # mapeador
    def mapper(self, _, line):

        # ignora o cabeçalho
        if line.startswith("generation,"):
            return
        
        # divide a linha em partes
        parts = line.rstrip("\n").split(",")
        
        # verifica se há partes suficientes
        if len(parts) < 3:
            return
        
        # extrai geração, plataforma e streams
        gen, plat, streams = parts[0], parts[1], parts[2]
        
        # tenta converter streams para inteiro
        try:
            streams = int(streams)
        except Exception:
            return
        
        # emite a chave (geração, plataforma) e o valor streams
        yield (gen or "unknown", plat or "unknown"), streams

    # redutor
    def reducer(self, key, values):
        
        # soma os valores de streams para a chave dada
        gen, plat = key
        total = sum(values)
        yield None, {"generation": gen, "platform": plat, "total_streams": total}

if __name__ == "__main__":
    MRGenStats.run()

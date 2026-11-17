"""
servidor.py — Servidor de conteúdos (é também um nó do overlay, D14, D17).

Uso:
    python3 servidor.py <nome> <ip_bootstrapper> <video> [<video> ...] [-v]

Exemplo:
    python3 servidor.py O1 10.0.4.20 movie.Mjpeg outro.Mjpeg

Cada vídeo é um fluxo, identificado por "<nome>/<video>" (D23).
  * A cada 2 s anuncia todos os seus fluxos num só anúncio (D22, D27, D28),
    com um identificador (hora de arranque, nº de sequência) (D29).
  * Para cada fluxo com Destinos, envia 20 pacotes de teste numerados por
    segundo (D42). Na Etapa 4 o conteúdo de teste é trocado pelos frames do
    vídeo. Envia sempre, em contínuo, enquanto houver Destinos (requisito da
    D36).

Comandos no terminal (D41): t, v, h.
"""

import os
import time

import protocolo as P
from no import No
from ott import argumentos_base, configurar_logs, correr
from rotas import Rota


class Servidor(No):

    TAMANHO_TESTE = 1000      # bytes de conteúdo de teste por pacote

    def __init__(self, nome, ip_bootstrapper, videos, **kw):
        super().__init__(nome, ip_bootstrapper, **kw)
        self.videos = [os.path.basename(v) for v in videos]
        self.arranque = int(time.time() * 1000)     # identifica esta execução (D29)
        self.seq_anuncio = 0
        for v in self.videos:
            fluxo = "%s/%s" % (nome, v)
            self.rotas[fluxo] = Rota(fluxo, nome, fonte=True)

    def arrancar(self):
        super().arrancar()
        self.log.info("servidor com os fluxos: %s", sorted(self.rotas_fonte()))
        self._thread(self._ciclo_anuncios, "anuncios")
        self._thread(self._ciclo_envio, "envio")

    def rotas_fonte(self):
        with self.lock:
            return [f for f, r in self.rotas.items() if r.fonte]

    # -- anúncios (D22, D27, D28, D29) ---------------------------------------
    def _ciclo_anuncios(self):
        while self.a_correr.is_set():
            self.seq_anuncio += 1
            msg = P.mensagem(P.ANUNCIO, servidor=self.nome, arranque=self.arranque,
                             seq=self.seq_anuncio, fluxos=self.videos,
                             custo=0.0, saltos=0, caminho=[self.nome])
            for nome in self.nomes_vizinhos():
                self.enviar_a(nome, msg)
            time.sleep(P.ANUNCIO_PERIODO)

    # -- dados de teste (D42) -------------------------------------------------
    def _ciclo_envio(self):
        intervalo = 1.0 / P.TAXA_DADOS
        seq = {f: 0 for f in self.rotas_fonte()}
        proximo = time.monotonic()
        while self.a_correr.is_set():
            proximo += intervalo
            for fluxo in seq:
                seq[fluxo] = (seq[fluxo] + 1) & 0xFFFF
                with self.lock:
                    ativo = self.rotas[fluxo].ativa()
                if not ativo:
                    continue
                conteudo = ("TESTE %s #%d " % (fluxo, seq[fluxo])).encode()
                conteudo = conteudo.ljust(self.TAMANHO_TESTE, b".")
                ts = int(time.monotonic() * 1000)
                self.reencaminhar(fluxo, None, P.rtp_codificar(seq[fluxo], ts, conteudo))
            time.sleep(max(0.0, proximo - time.monotonic()))

    def texto_ajuda(self):
        return super().texto_ajuda() + "\nServidor dos fluxos: " + ", ".join(sorted(self.rotas_fonte()))


def main():
    ap = argumentos_base("Servidor de conteúdos do overlay OTT")
    ap.add_argument("videos", nargs="+", help="vídeos que este servidor disponibiliza")
    args = ap.parse_args()
    configurar_logs(args.verboso)
    correr(Servidor(args.nome, args.bootstrapper, args.videos))


if __name__ == "__main__":
    main()

"""
cliente.py — Cliente de streaming (é também um nó do overlay, D14, D17).

Uso:
    python3 cliente.py <nome> <ip_bootstrapper> [--ver <fluxo>] [-v]

Exemplo:
    python3 cliente.py O3 10.0.4.20 --ver O1/movie.Mjpeg

O cliente recebe os anúncios como qualquer nó: a sua tabela de rotas é a
lista de fluxos disponíveis (D34). Só pede fluxos dessa lista.

Comandos no terminal:
    l               lista os fluxos disponíveis
    ver <fluxo>     começa a ver um fluxo (envia ATIVAR, D31)
    parar <fluxo>   deixa de ver (DESATIVAR / poda, D33)
    e               estatísticas dos fluxos que está a ver
    t, v, h         tabela de rotas, vizinhos, ajuda (D41)

Na Etapa 2 os dados são pacotes de teste (D42): o cliente conta recebidos,
perdidos e duplicados e mostra um resumo a cada 5 s. Na Etapa 4 passa a
mostrar o vídeo.
"""

import time

from no import No
from ott import argumentos_base, configurar_logs, correr


class Cliente(No):

    PERIODO_RESUMO = 5.0

    def __init__(self, nome, ip_bootstrapper, ver=None, **kw):
        super().__init__(nome, ip_bootstrapper, **kw)
        self.ver_ao_arrancar = ver
        self.a_ver = set()

    def arrancar(self):
        super().arrancar()
        self._thread(self._ciclo_resumo, "resumo")
        if self.ver_ao_arrancar:
            self._thread(self._esperar_e_ver, "ver", self.ver_ao_arrancar)

    def _esperar_e_ver(self, fluxo, limite=15.0):
        """--ver: espera que o fluxo apareça nos anúncios e pede-o."""
        fim = time.monotonic() + limite
        while self.a_correr.is_set() and time.monotonic() < fim:
            if fluxo in self.fluxos_disponiveis():
                self.ver(fluxo)
                return
            time.sleep(0.2)
        self.log.warning("fluxo %s não apareceu nos anúncios em %.0f s", fluxo, limite)

    # -- ver / parar ------------------------------------------------------------
    def ver(self, fluxo):
        if fluxo not in self.fluxos_disponiveis():
            # D34: só pede fluxos que conhece -> nenhuma mensagem na rede
            print("fluxo desconhecido: %s. Disponíveis: %s" %
                  (fluxo, ", ".join(self.fluxos_disponiveis()) or "nenhum"), flush=True)
            return
        ok, motivo = self.pedir_fluxo(fluxo)
        if ok:
            self.a_ver.add(fluxo)
            self.log.info("a ver %s", fluxo)
        else:
            print("não foi possível ver %s: %s" % (fluxo, motivo), flush=True)

    def parar(self, fluxo):
        self.a_ver.discard(fluxo)
        self.largar_fluxo(fluxo)
        self.log.info("deixou de ver %s", fluxo)

    def ao_erro_fluxo(self, fluxo, motivo):
        self.a_ver.discard(fluxo)
        self.log.warning("fluxo %s indisponível de momento (%s)", fluxo, motivo)

    # -- estatísticas -------------------------------------------------------------
    def _ciclo_resumo(self):
        while self.a_correr.is_set():
            time.sleep(self.PERIODO_RESUMO)
            for linha in self._linhas_estatisticas():
                self.log.info(linha)

    def _linhas_estatisticas(self):
        with self.lock:
            linhas = []
            for fluxo in sorted(self.a_ver):
                r = self.rotas.get(fluxo)
                if r is None or not r.sou_cliente:
                    continue
                linhas.append("%s via %s: %d recebidos, %d perdidos, %d duplicados "
                              "descartados, último nº %s" % (
                                  fluxo, r.origem or "—", r.recebidos, r.perdidos,
                                  r.duplicados, r.ultimo_seq))
        return linhas

    # -- comandos -----------------------------------------------------------------
    def comando(self, cmd, args):
        if cmd == "l":
            print("Fluxos disponíveis: %s" % (", ".join(self.fluxos_disponiveis()) or "nenhum"),
                  flush=True)
        elif cmd == "ver" and args:
            self.ver(args[0])
        elif cmd == "parar" and args:
            self.parar(args[0])
        elif cmd == "e":
            print("\n".join(self._linhas_estatisticas()) or "não está a ver nenhum fluxo",
                  flush=True)
        else:
            super().comando(cmd, args)

    def texto_ajuda(self):
        return ("Comandos: l = lista fluxos | ver <fluxo> | parar <fluxo> | "
                "e = estatísticas\n" + super().texto_ajuda())


def main():
    ap = argumentos_base("Cliente de streaming do overlay OTT")
    ap.add_argument("--ver", metavar="FLUXO", help="fluxo a ver logo ao arrancar")
    args = ap.parse_args()
    configurar_logs(args.verboso)
    correr(Cliente(args.nome, args.bootstrapper, ver=args.ver))


if __name__ == "__main__":
    main()

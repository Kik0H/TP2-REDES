"""
bootstrapper.py — Nó bem conhecido que ajuda a construir o overlay (D05, D06).

Uso:
    python3 bootstrapper.py <topologia.json> [ip_escuta]

Lê do ficheiro de configuração (D10) quem é vizinho de quem e, quando um nó
se regista, responde com os vizinhos configurados que JÁ estão ativos (D13).

Os nós repetem o REGISTER a cada 5 s (refresh, D18). Um nó que não renova
o registo há mais de 15 s é considerado inativo e esquecido.

Formato do ficheiro:
    {
      "O1": {"ip": "10.0.0.10", "vizinhos": ["O2"]},
      "O2": {"ip": "10.0.1.10", "vizinhos": ["O1", "O3"]},
      ...
    }
"""

import json
import logging
import socket
import sys
import threading
import time

import protocolo as P

log = logging.getLogger("BOOT")


class Bootstrapper:

    def __init__(self, topologia, ip_escuta=""):
        self.topologia = topologia          # nome -> {"ip", "vizinhos"} {O2: {"1123", "O1,O3"}}
        self.ip_escuta = ip_escuta        
        self.ativos = {}                    # nome -> hora do último REGISTER
        self.lock = threading.Lock()        # protege self.ativos (D08)

    # -- arranque ----------------------------------------------------------
    def arrancar(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.ip_escuta, P.PORTA_BOOTSTRAPPER))
        srv.listen()
        threading.Thread(target=self._ciclo_expirar, daemon=True).start()
        log.info("à escuta em %s:%d com %d nós na topologia",
                 self.ip_escuta or "*", P.PORTA_BOOTSTRAPPER, len(self.topologia))
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=self.atender, args=(sock, addr),
                             daemon=True).start()

    # -- um pedido por ligação --------------------------------------------
    def atender(self, sock, addr):
        with sock:
            try:
                msg = P.LeitorLinhas(sock).ler()
                if msg is None:
                    return
                tipo = msg["type"]
                if tipo == P.REGISTER:
                    sock.sendall(P.codificar(self.registar(msg.get("nome"), addr)))
                elif tipo == P.LEAVE:
                    self.sair(msg.get("nome"))
                else:
                    log.warning("mensagem inesperada de %s: %s", addr[0], msg)
            except (OSError, ValueError) as e:
                log.warning("erro a atender %s: %s", addr[0], e)

    def registar(self, nome, addr):
        if nome not in self.topologia:
            log.warning("REGISTER recusado: '%s' (de %s) não existe na topologia",
                        nome, addr[0])
            return P.mensagem(P.REGISTER_ERR,
                              motivo="nó '%s' não existe na topologia" % nome)
        with self.lock:
            self._expirar()
            novo = nome not in self.ativos
            self.ativos[nome] = time.monotonic()
            vizinhos = [
                {"nome": v, "ip": self.topologia[v]["ip"]}
                for v in self.topologia[nome]["vizinhos"]
                if v in self.ativos and v != nome
            ]
            ativos = sorted(self.ativos)
        texto = "REGISTER %s (de %s) -> vizinhos ativos: %s | ativos: %s"
        args = (nome, addr[0], [v["nome"] for v in vizinhos], ativos)
        if novo:
            log.info(texto, *args)
        else:
            log.debug("(refresh) " + texto, *args)   # refresh: só com -v
        return P.mensagem(P.REGISTER_OK, nome=nome,
                          ip=self.topologia[nome]["ip"], vizinhos=vizinhos)

    def sair(self, nome):
        with self.lock:
            self.ativos.pop(nome, None)
            ativos = sorted(self.ativos)
        log.info("LEAVE %s | ativos: %s", nome, ativos)


    # -- expiração de registos (D18) ----------------------------------------
    def _expirar(self):
        """Remove nós sem refresh há mais de REGISTO_TIMEOUT. Chamar com lock."""
        agora = time.monotonic()
        for nome, t in list(self.ativos.items()):
            if agora - t > P.REGISTO_TIMEOUT:
                del self.ativos[nome]
                log.info("registo de %s expirou (sem refresh há %.0fs) | ativos: %s",
                         nome, agora - t, sorted(self.ativos))

    def _ciclo_expirar(self):
        while True:
            time.sleep(P.REFRESH_PERIODO)
            with self.lock:
                self._expirar()


def carregar_topologia(caminho):
    with open(caminho, encoding="utf-8") as f:
        topo = json.load(f)
    # validação simples: todos os vizinhos têm de existir
    for nome, info in topo.items():
        for v in info["vizinhos"]:
            if v not in topo:
                raise ValueError("%s tem vizinho desconhecido: %s" % (nome, v))
    return topo


def main():
    args = [a for a in sys.argv[1:] if a not in ("-v", "--verboso")]
    verboso = len(args) != len(sys.argv) - 1
    if not args:
        print("Uso: python3 bootstrapper.py <topologia.json> [ip_escuta] [-v]")
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if verboso else logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s",
                        datefmt="%H:%M:%S")
    topo = carregar_topologia(args[0])
    ip = args[1] if len(args) > 1 else ""
    try:
        Bootstrapper(topo, ip).arrancar()
    except KeyboardInterrupt:
        log.info("a terminar")


if __name__ == "__main__":
    main()

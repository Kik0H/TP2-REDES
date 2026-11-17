"""
ott.py — Nó intermédio do overlay.

Uso:
    python3 ott.py <nome> <ip_bootstrapper> [-v]

Exemplo (no CORE, na máquina que corresponde a O2):
    python3 ott.py O2 10.0.4.20

Comandos no terminal (D41): t = tabela de rotas, v = vizinhos, h = ajuda.
"""

import argparse
import logging
import signal
import sys
import threading

from no import No


def configurar_logs(verboso=False):
    logging.basicConfig(level=logging.DEBUG if verboso else logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s",
                        datefmt="%H:%M:%S")


def argumentos_base(descricao):
    ap = argparse.ArgumentParser(description=descricao)
    ap.add_argument("nome", help="nome lógico do nó (ex.: O3)")
    ap.add_argument("bootstrapper", help="IP do bootstrapper")
    ap.add_argument("-v", "--verboso", action="store_true")
    return ap


def correr(no):
    """Arranca o nó e fica à espera de Ctrl+C / SIGTERM para sair limpo."""
    parar = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: parar.set())
    try:
        no.arrancar()
    except (OSError, RuntimeError) as e:
        logging.error("não foi possível arrancar: %s", e)
        sys.exit(1)
    try:
        while not parar.wait(1):
            pass
    except KeyboardInterrupt:
        pass
    no.sair()


def main():
    args = argumentos_base("Nó intermédio do overlay OTT").parse_args()
    configurar_logs(args.verboso)
    correr(No(args.nome, args.bootstrapper))


if __name__ == "__main__":
    main()

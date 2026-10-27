"""
protocolo.py — Definições comuns do protocolo de controlo do overlay.

Mensagens de controlo (D07): objetos JSON em UTF-8, uma mensagem por linha
(terminada em '\\n'), enviadas sobre TCP. O campo "type" identifica o tipo.
"""

import json

# ---------------------------------------------------------------------------
# Portas (D12)
# ---------------------------------------------------------------------------
PORTA_BOOTSTRAPPER = 5000   # TCP — bootstrapper
PORTA_CONTROLO = 6000       # TCP — controlo entre nós do overlay
PORTA_DADOS = 6001          # UDP — streaming (Etapa 4)

# ---------------------------------------------------------------------------
# Heartbeats (D15)
# ---------------------------------------------------------------------------
HEARTBEAT_PERIODO = 2.0     # segundos entre heartbeats
HEARTBEAT_TIMEOUT = 6.0     # segundos sem notícias -> vizinho morto

# ---------------------------------------------------------------------------
# Refresh no bootstrapper (D18)
# ---------------------------------------------------------------------------
REFRESH_PERIODO = 5.0       # cada nó renova o registo a cada 5 s
REGISTO_TIMEOUT = 15.0      # bootstrapper esquece nós sem renovação há 15 s

# ---------------------------------------------------------------------------
# Tipos de mensagem
# ---------------------------------------------------------------------------
# Nó <-> Bootstrapper
REGISTER = "REGISTER"           # nó -> boot  : {"nome"} — registo e refresh (D18)
REGISTER_OK = "REGISTER_OK"     # boot -> nó  : {"nome", "ip", "vizinhos": [{"nome","ip"}]}
REGISTER_ERR = "REGISTER_ERR"   # boot -> nó  : {"motivo"}
# Nó <-> Nó (e nó -> boot no caso de LEAVE)
HELLO = "HELLO"                 # {"nome"} — primeira mensagem numa ligação nova
HEARTBEAT = "HEARTBEAT"         # {"nome"}
LEAVE = "LEAVE"                 # {"nome"} — saída limpa (D11)


def mensagem(tipo, **campos):
    """Cria um dicionário de mensagem: mensagem(HELLO, nome="O3")."""
    msg = {"type": tipo}
    msg.update(campos)
    return msg


def codificar(msg):
    """dict -> bytes prontos a enviar (JSON + '\\n')."""
    return (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")


def descodificar(linha):
    """Linha recebida (str ou bytes) -> dict. Lança ValueError se inválida."""
    if isinstance(linha, bytes):
        linha = linha.decode("utf-8")
    msg = json.loads(linha)
    if not isinstance(msg, dict) or "type" not in msg:
        raise ValueError("mensagem sem campo 'type': %r" % linha)
    return msg


class LeitorLinhas:
    """
    Lê mensagens completas de um socket TCP.

    O TCP é um fluxo de bytes: um recv() pode trazer meia mensagem ou várias
    coladas. Esta classe acumula bytes e só devolve mensagens completas
    (terminadas em '\\n').
    """

    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def ler(self):
        """Devolve a próxima mensagem (dict), ou None se a ligação fechou."""
        while b"\n" not in self.buffer:
            dados = self.sock.recv(4096)
            if not dados:
                return None
            self.buffer += dados
        linha, self.buffer = self.buffer.split(b"\n", 1)
        return descodificar(linha)


def pedido_resposta(ip, porta, msg, timeout=5.0):
    """
    Abre uma ligação TCP, envia uma mensagem e espera por UMA resposta.
    Usado para falar com o bootstrapper.
    """
    import socket
    with socket.create_connection((ip, porta), timeout=timeout) as s:
        s.sendall(codificar(msg))
        return LeitorLinhas(s).ler()


def enviar_uma(ip, porta, msg, timeout=5.0):
    """Abre uma ligação TCP, envia uma mensagem e fecha (sem resposta)."""
    import socket
    with socket.create_connection((ip, porta), timeout=timeout) as s:
        s.sendall(codificar(msg))

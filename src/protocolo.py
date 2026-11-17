"""
protocolo.py — Definições comuns do protocolo do overlay.

Mensagens de controlo (D07): objetos JSON em UTF-8, uma mensagem por linha
(terminada em '\\n'), enviadas sobre TCP. O campo "type" identifica o tipo.

Pacotes de dados (D23, D46), enviados por UDP:
    [1 byte: tamanho do id do fluxo][id do fluxo]
    [1 byte: tamanho do nome do remetente][nome do remetente]
    [pacote RTP: cabeçalho de 12 bytes + conteúdo]
"""

import json
import struct

# ---------------------------------------------------------------------------
# Portas (D12)
# ---------------------------------------------------------------------------
PORTA_BOOTSTRAPPER = 5000   # TCP — bootstrapper
PORTA_CONTROLO = 6000       # TCP — controlo entre nós do overlay
PORTA_DADOS = 6001          # UDP — dados (D42; vídeo na Etapa 4)

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
# Rotas (Etapa 2)
# ---------------------------------------------------------------------------
ANUNCIO_PERIODO = 2.0       # D27: cada servidor anuncia a cada 2 s
ROTA_TIMEOUT = 6.0          # D30: candidata sem anúncio há 6 s -> apagada
TOLERANCIA_REL = 0.10       # D25: atrasos com < 10% de diferença empatam
TOLERANCIA_ABS_MS = 1.0     # D43: ... ou com < 1 ms de diferença
SEGUIDOS_PARA_TROCAR = 3    # D37: só troca se for melhor 3 anúncios seguidos
VIDEO_TIMEOUT = 1.0         # D36: ~1 s sem dados pela Origem -> falha
SUSPENSAO = 10.0            # D45: candidata suspensa 10 s após a D36
ALFA_ATRASO = 0.25          # D39: média móvel exponencial do atraso
ATRASO_DESCONHECIDO = 1.0   # ms, usado enquanto um link ainda não foi medido
TAXA_DADOS = 20             # D42: pacotes de teste por segundo, por fluxo

# ---------------------------------------------------------------------------
# Tipos de mensagem
# ---------------------------------------------------------------------------
# Nó <-> Bootstrapper
REGISTER = "REGISTER"           # nó -> boot  : {"nome"} — registo e refresh (D18)
REGISTER_OK = "REGISTER_OK"     # boot -> nó  : {"nome", "ip", "vizinhos": [{"nome","ip"}]}
REGISTER_ERR = "REGISTER_ERR"   # boot -> nó  : {"motivo"}
# Nó <-> Nó (e nó -> boot no caso de LEAVE)
HELLO = "HELLO"                 # {"nome", "ip"} — 1.ª mensagem numa ligação nova;
                                #   "ip" = IP configurado onde o nó está à escuta
HEARTBEAT = "HEARTBEAT"         # {"nome", "t", "eco", "espera"} — RTT (D24)
LEAVE = "LEAVE"                 # {"nome"} — saída limpa (D11)
# Rotas (Etapa 2)
ANUNCIO = "ANUNCIO"             # {"servidor", "arranque", "seq", "fluxos",
                                #  "custo", "saltos", "caminho"} (D22, D28, D29)
ATIVAR = "ATIVAR"               # {"fluxo"} (D31, D32)
DESATIVAR = "DESATIVAR"         # {"fluxo"} (D31, D33)
ATIVAR_ERRO = "ATIVAR_ERRO"     # {"fluxo", "motivo"} (D34, D44)


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


# ---------------------------------------------------------------------------
# Pacotes de dados (D23, D46) e RTP
# ---------------------------------------------------------------------------
RTP_PT_MJPEG = 26


def rtp_codificar(seq, timestamp, conteudo, pt=RTP_PT_MJPEG, ssrc=0):
    """Cabeçalho RTP (versão 2, sem padding/extensão/CSRC) + conteúdo."""
    cab = struct.pack("!BBHII", 0x80, pt & 0x7F, seq & 0xFFFF,
                      timestamp & 0xFFFFFFFF, ssrc & 0xFFFFFFFF)
    return cab + conteudo


def rtp_seq(rtp):
    return struct.unpack("!H", rtp[2:4])[0]


def rtp_timestamp(rtp):
    return struct.unpack("!I", rtp[4:8])[0]


def rtp_conteudo(rtp):
    return rtp[12:]


def codificar_dados(fluxo, remetente, rtp):
    f = fluxo.encode("utf-8")
    r = remetente.encode("utf-8")
    return bytes([len(f)]) + f + bytes([len(r)]) + r + rtp


def descodificar_dados(pacote):
    """bytes -> (fluxo, remetente, rtp). Lança ValueError se mal formado."""
    try:
        n = pacote[0]
        fluxo = pacote[1:1 + n].decode("utf-8")
        m = pacote[1 + n]
        remetente = pacote[2 + n:2 + n + m].decode("utf-8")
        rtp = pacote[2 + n + m:]
    except (IndexError, UnicodeDecodeError) as e:
        raise ValueError("pacote de dados mal formado: %s" % e)
    if len(rtp) < 12:
        raise ValueError("pacote de dados sem cabeçalho RTP")
    return fluxo, remetente, rtp


def seq_mais_recente(novo, ultimo):
    """True se o nº de sequência RTP `novo` vem depois de `ultimo` (16 bits, com volta)."""
    return 0 < ((novo - ultimo) & 0xFFFF) < 0x8000

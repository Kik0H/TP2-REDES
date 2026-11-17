"""
rotas.py — Tabela de rotas e regras de comparação (Etapa 2).

Estrutura (D40): uma Rota por fluxo, com as candidatas por vizinho lá dentro.
Regras de comparação entre candidatas (D24, D25, D26, D43):
    1. atraso acumulado — ganha o menor, mas diferenças < 10% ou < 1 ms
       contam como empate;
    2. nº de saltos — ganha o menor;
    3. desempate final — manter a Origem atual; sem Origem, nome menor.

Este módulo só tem dados e funções puras (sem rede nem threads); a lógica
de quando escolher/trocar está em no.py.
"""

import time

import protocolo as P


class Candidata:
    """Um caminho possível para um fluxo, por um vizinho que o anunciou (D22)."""

    def __init__(self, vizinho):
        self.vizinho = vizinho
        self.atraso = None          # ms acumulados até ao servidor (D24)
        self.saltos = None
        self.caminho = []
        self.ultimo_anuncio = 0.0   # time.monotonic() (D30)
        self.melhor_seguidos = 0    # anúncios seguidos em que foi melhor (D37)
        self.suspensa_ate = 0.0     # time.monotonic() (D45)

    def suspensa(self, agora=None):
        return (agora or time.monotonic()) < self.suspensa_ate


class Rota:
    """Entrada da tabela de rotas para um fluxo (D40)."""

    def __init__(self, fluxo, servidor, fonte=False):
        self.fluxo = fluxo              # "S1/movie.Mjpeg" (D23)
        self.servidor = servidor
        self.fonte = fonte              # True no servidor que gera o fluxo
        self.estado = "inativa"         # "ativa" | "inativa"
        self.origem = None              # vizinho de onde recebe (D35)
        self.destinos = set()           # vizinhos a quem reenvia (D31–D33)
        self.sou_cliente = False        # o próprio nó está a ver o fluxo
        self.candidatas = {}            # vizinho -> Candidata
        # troca sem corte em curso (D38)
        self.trocar_para = None
        self.trocar_desde = 0.0
        # plano de dados (D36, D38, D42)
        self.ultimo_seq = None
        self.ultimo_aceite = 0.0        # último pacote novo (qualquer remetente)
        self.ultimo_pacote = 0.0        # último pacote vindo da Origem (D36)
        self.sem_dados_avisado = False  # D47: já registou "sem dados, sem reserva"
        self.recebidos = 0
        self.perdidos = 0
        self.duplicados = 0

    def ativa(self):
        return self.estado == "ativa"

    def reset_dados(self):
        self.ultimo_seq = None
        self.ultimo_aceite = 0.0


# ---------------------------------------------------------------------------
# Comparação (D24, D25, D26, D43)
# ---------------------------------------------------------------------------
def atrasos_empatam(a, b):
    d = abs(a - b)
    return d < P.TOLERANCIA_ABS_MS or d < P.TOLERANCIA_REL * min(a, b)


def comparar(atraso1, saltos1, atraso2, saltos2):
    """-1 se a 1.ª é melhor, 1 se a 2.ª é melhor, 0 se empatam (sem nome)."""
    if not atrasos_empatam(atraso1, atraso2):
        return -1 if atraso1 < atraso2 else 1
    if saltos1 != saltos2:
        return -1 if saltos1 < saltos2 else 1
    return 0


def comparar_candidatas(c1, c2):
    return comparar(c1.atraso, c1.saltos, c2.atraso, c2.saltos)


def melhor_candidata(rota, excluir=(), agora=None):
    """
    Melhor candidata utilizável (não excluída nem suspensa), ou None.
    Sem Origem atual: em empate ganha o vizinho de nome menor (D26).
    """
    agora = agora or time.monotonic()
    melhor = None
    for nome in sorted(rota.candidatas):        # ordem de nome = desempate
        c = rota.candidatas[nome]
        if nome in excluir or c.suspensa(agora) or c.atraso is None:
            continue
        if melhor is None or comparar_candidatas(c, melhor) < 0:
            melhor = c
    return melhor.vizinho if melhor else None

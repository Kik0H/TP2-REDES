"""
no.py — Núcleo comum de um nó do overlay (D14, D17).

A classe No trata de tudo o que é comum a qualquer nó:
  * registo no bootstrapper e obtenção dos vizinhos (D05, D13);
  * ligações TCP persistentes com os vizinhos, iniciadas com HELLO;
  * heartbeats e deteção de vizinhos mortos (D11, D15);
  * refresh periódico no bootstrapper e religação a vizinhos perdidos (D18);
  * saída limpa com LEAVE (D11).

Os papéis (servidor, cliente) são subclasses que reescrevem os "hooks":
    ao_receber(viz, msg)          — chamada para cada mensagem recebida
    ao_vizinho_novo(viz)          — chamada quando um vizinho fica ligado
    ao_vizinho_perdido(viz, motivo)
Ao reescrever ao_receber, chamar super().ao_receber(viz, msg) para os tipos
que a subclasse não trata.

Concorrência (D08): uma thread para aceitar ligações, uma por vizinho, uma
para os heartbeats e uma para o refresh. self.vizinhos é partilhado -> acesso sempre com self.lock.
"""

import logging
import socket
import threading
import time

import protocolo as P


class Vizinho:
    """Uma ligação TCP ativa com um vizinho do overlay."""

    def __init__(self, nome, ip, sock, leitor, iniciador):
        self.nome = nome
        self.iniciador = iniciador            # nome do nó que abriu a ligação
        self.ip = ip
        self.sock = sock
        self.leitor = leitor
        self.ultimo_contacto = time.monotonic()
        self._lock_envio = threading.Lock()   # várias threads podem enviar

    def enviar(self, msg):
        with self._lock_envio:
            self.sock.sendall(P.codificar(msg))

    def fechar(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)   # acorda o recv() bloqueado
        except OSError:
            pass
        self.sock.close()

    def __repr__(self):
        return "%s(%s)" % (self.nome, self.ip)


class No:

    TENTATIVAS_LIGACAO = 3
    INTERVALO_TENTATIVAS = 0.5   # s

    def __init__(self, nome, ip_bootstrapper):
        self.nome = nome
        self.ip_bootstrapper = ip_bootstrapper
        self.ip = None                       # recebido do bootstrapper
        self.vizinhos = {}                   # nome -> Vizinho
        self.lock = threading.RLock()        # protege self.vizinhos
        self.a_correr = threading.Event()
        self._servidor = None
        self.log = logging.getLogger(nome)

    # =====================================================================
    # Arranque e saída
    # =====================================================================
    def _registar(self):
        """REGISTER no bootstrapper (registo inicial e refresh, D18)."""
        resp = P.pedido_resposta(self.ip_bootstrapper, P.PORTA_BOOTSTRAPPER,
                                 P.mensagem(P.REGISTER, nome=self.nome))
        if resp is None or resp["type"] != P.REGISTER_OK:
            motivo = resp.get("motivo") if resp else "sem resposta"
            raise RuntimeError("registo recusado pelo bootstrapper: %s" % motivo)
        return resp

    def arrancar(self):
        # 1. Registo no bootstrapper
        resp = self._registar()
        self.ip = resp["ip"]
        self.log.info("registado no bootstrapper com IP %s; vizinhos ativos: %s",
                      self.ip, [v["nome"] for v in resp["vizinhos"]])

        # 2. Começar a aceitar ligações de vizinhos que cheguem depois
        self.a_correr.set()
        self._servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._servidor.bind((self.ip, P.PORTA_CONTROLO))
        self._servidor.listen()
        threading.Thread(target=self._ciclo_aceitar, daemon=True,
                         name="aceitar").start()

        # 3. Ligar aos vizinhos que já estão ativos (D13)
        for v in resp["vizinhos"]:
            self.ligar(v["nome"], v["ip"])

        # 4. Heartbeats (D15) e refresh (D18)
        threading.Thread(target=self._ciclo_heartbeat, daemon=True,
                         name="heartbeat").start()
        threading.Thread(target=self._ciclo_refresh, daemon=True,
                         name="refresh").start()

    def sair(self):
        """Saída limpa: avisa vizinhos e bootstrapper com LEAVE (D11)."""
        if not self.a_correr.is_set():
            return
        self.a_correr.clear()
        self.log.info("a sair do overlay (LEAVE)")
        leave = P.mensagem(P.LEAVE, nome=self.nome)
        with self.lock:
            vizinhos = list(self.vizinhos.values())
            self.vizinhos.clear()
        for viz in vizinhos:
            try:
                viz.enviar(leave)
            except OSError:
                pass
            viz.fechar()
        try:
            P.enviar_uma(self.ip_bootstrapper, P.PORTA_BOOTSTRAPPER, leave)
        except OSError as e:
            self.log.warning("não foi possível avisar o bootstrapper: %s", e)
        if self._servidor:
            self._servidor.close()

    # =====================================================================
    # Estabelecimento de ligações
    # =====================================================================
    def ligar(self, nome, ip):
        """Liga-se ativamente a um vizinho e troca HELLO nos dois sentidos."""
        for tentativa in range(1, self.TENTATIVAS_LIGACAO + 1):
            try:
                sock = socket.create_connection((ip, P.PORTA_CONTROLO), timeout=3)
                break
            except OSError as e:
                self.log.warning("ligação a %s (%s) falhou [%d/%d]: %s",
                                 nome, ip, tentativa, self.TENTATIVAS_LIGACAO, e)
                time.sleep(self.INTERVALO_TENTATIVAS)
        else:
            return None
        try:
            leitor = P.LeitorLinhas(sock)
            sock.sendall(P.codificar(P.mensagem(P.HELLO, nome=self.nome)))
            resp = leitor.ler()                    # espera o HELLO de volta
            if resp is None or resp["type"] != P.HELLO:
                raise ValueError("resposta inesperada ao HELLO: %s" % resp)
            sock.settimeout(None)
        except (OSError, ValueError) as e:
            self.log.warning("HELLO com %s falhou: %s", nome, e)
            sock.close()
            return None
        self.log.info("HELLO enviado a %s, HELLO recebido de %s",
                      nome, resp.get("nome"))
        return self._adicionar_vizinho(resp.get("nome", nome), ip, sock, leitor,
                                       iniciador=self.nome)

    def _ciclo_aceitar(self):
        while self.a_correr.is_set():
            try:
                sock, addr = self._servidor.accept()
            except OSError:
                break                               # socket fechado em sair()
            threading.Thread(target=self._aceitar_vizinho, args=(sock, addr),
                             daemon=True).start()

    def _aceitar_vizinho(self, sock, addr):
        """Um vizinho ligou-se a nós: espera HELLO e responde com HELLO."""
        try:
            sock.settimeout(5)
            leitor = P.LeitorLinhas(sock)
            msg = leitor.ler()
            if msg is None or msg["type"] != P.HELLO or "nome" not in msg:
                raise ValueError("esperava HELLO, recebi %s" % msg)
            sock.sendall(P.codificar(P.mensagem(P.HELLO, nome=self.nome)))
            sock.settimeout(None)
        except (OSError, ValueError) as e:
            self.log.warning("ligação de %s rejeitada: %s", addr[0], e)
            sock.close()
            return
        self.log.info("HELLO recebido de %s (%s), HELLO enviado de volta",
                      msg["nome"], addr[0])
        self._adicionar_vizinho(msg["nome"], addr[0], sock, leitor,
                                iniciador=msg["nome"])

    def _adicionar_vizinho(self, nome, ip, sock, leitor, iniciador):
        viz = Vizinho(nome, ip, sock, leitor, iniciador)
        preferido = min(self.nome, nome)            # desempate por nome (D18)
        with self.lock:
            antigo = self.vizinhos.get(nome)
            # Duas ligações para o mesmo par (ex.: refresh em simultâneo com a
            # entrada do vizinho): os dois lados ficam com a que foi iniciada
            # pelo nó de nome menor, para escolherem a MESMA.
            manter_antigo = (antigo is not None
                             and antigo.iniciador == preferido
                             and iniciador != preferido)
            if not manter_antigo:
                self.vizinhos[nome] = viz
        if manter_antigo:
            self.log.info("ligação duplicada com %s descartada (fica a iniciada por %s)",
                          nome, preferido)
            viz.fechar()
            return antigo
        if antigo is not None:                      # ex.: vizinho reiniciou
            self.log.info("nova ligação de %s substitui a anterior", nome)
            antigo.fechar()
        threading.Thread(target=self._ciclo_leitura, args=(viz,), daemon=True,
                         name="viz-" + nome).start()
        self.log.info("vizinhos: %s", self.nomes_vizinhos())
        self.ao_vizinho_novo(viz)
        return viz

    # =====================================================================
    # Receção de mensagens
    # =====================================================================
    def _ciclo_leitura(self, viz):
        motivo = "ligação fechada"
        try:
            while self.a_correr.is_set():
                msg = viz.leitor.ler()
                if msg is None:
                    break
                viz.ultimo_contacto = time.monotonic()
                self.ao_receber(viz, msg)
        except (OSError, ValueError) as e:
            motivo = "erro na ligação: %s" % e
        self._perder_vizinho(viz, motivo)

    def _perder_vizinho(self, viz, motivo):
        with self.lock:
            removido = self.vizinhos.get(viz.nome) is viz
            if removido:
                del self.vizinhos[viz.nome]
        viz.fechar()
        if removido and self.a_correr.is_set():
            self.log.info("vizinho %s perdido (%s); vizinhos: %s",
                          viz.nome, motivo, self.nomes_vizinhos())
            self.ao_vizinho_perdido(viz, motivo)

    # =====================================================================
    # Heartbeats (D11, D15)
    # =====================================================================
    def _ciclo_heartbeat(self):
        hb = P.mensagem(P.HEARTBEAT, nome=self.nome)
        while self.a_correr.is_set():
            time.sleep(P.HEARTBEAT_PERIODO)
            agora = time.monotonic()
            with self.lock:
                vizinhos = list(self.vizinhos.values())
            for viz in vizinhos:
                if agora - viz.ultimo_contacto > P.HEARTBEAT_TIMEOUT:
                    self._perder_vizinho(viz, "sem heartbeat há mais de %.0fs"
                                         % P.HEARTBEAT_TIMEOUT)
                    continue
                try:
                    viz.enviar(hb)
                except OSError as e:
                    self._perder_vizinho(viz, "falha a enviar heartbeat: %s" % e)

    # =====================================================================
    # Refresh no bootstrapper e religação (D18)
    # =====================================================================
    def _ciclo_refresh(self):
        while self.a_correr.is_set():
            time.sleep(P.REFRESH_PERIODO)
            if not self.a_correr.is_set():
                break
            try:
                resp = self._registar()
            except (OSError, ValueError, RuntimeError) as e:
                self.log.warning("refresh no bootstrapper falhou: %s", e)
                continue
            for v in resp["vizinhos"]:
                with self.lock:
                    ja_ligado = v["nome"] in self.vizinhos
                # só o nó de nome menor inicia a religação (evita duplicados)
                if not ja_ligado and self.nome < v["nome"]:
                    self.log.info("refresh: %s está ativo mas não ligado -> a religar",
                                  v["nome"])
                    self.ligar(v["nome"], v["ip"])

    # =====================================================================
    # Utilitários
    # =====================================================================
    def nomes_vizinhos(self):
        with self.lock:
            return sorted(self.vizinhos)

    def enviar_a(self, nome, msg):
        """Envia uma mensagem de controlo a um vizinho. Devolve True se enviou."""
        with self.lock:
            viz = self.vizinhos.get(nome)
        if viz is None:
            return False
        try:
            viz.enviar(msg)
            return True
        except OSError as e:
            self._perder_vizinho(viz, "falha a enviar: %s" % e)
            return False

    # =====================================================================
    # Hooks — reescrever nas subclasses (Servidor, Cliente)
    # =====================================================================
    def ao_receber(self, viz, msg):
        tipo = msg["type"]
        if tipo == P.HEARTBEAT:
            pass                                    # ultimo_contacto já atualizado
        elif tipo == P.LEAVE:
            self._perder_vizinho(viz, "LEAVE")
        elif tipo == P.HELLO:
            self.log.debug("HELLO repetido de %s", viz.nome)
        else:
            self.log.warning("tipo de mensagem desconhecido de %s: %s",
                             viz.nome, tipo)

    def ao_vizinho_novo(self, viz):
        pass

    def ao_vizinho_perdido(self, viz, motivo):
        pass

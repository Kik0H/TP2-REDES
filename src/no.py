"""
no.py — Núcleo comum de um nó do overlay (D14, D17).

Etapa 1 — vizinhança:
  * registo no bootstrapper e obtenção dos vizinhos (D05, D13);
  * ligações TCP persistentes com os vizinhos, iniciadas com HELLO;
  * heartbeats e deteção de vizinhos mortos (D11, D15);
  * refresh periódico no bootstrapper e religação a vizinhos perdidos (D18, D21);
  * saída limpa com LEAVE (D11).

Etapa 2 — rotas e dados:
  * atraso de cada link medido pelos heartbeats, suavizado (D24, D39);
  * receção e reencaminhamento dos anúncios dos servidores (D22, D28, D29);
  * tabela de rotas com candidatas por vizinho e expiração (D30, D40);
  * ativação / desativação / poda / erros (D31–D34, D44);
  * reencaminhamento dos pacotes de dados pelas rotas (D23, D42, D46);
  * falhas: reserva local, deteção por falta de dados, suspensão (D35, D36, D45);
  * troca para rotas melhores com histerese e sem corte (D37, D38);
  * comandos no terminal: t (tabela), v (vizinhos), h (ajuda) (D41).

Os papéis (servidor, cliente) são subclasses que reescrevem os "hooks":
    ao_receber(viz, msg), ao_vizinho_novo(viz), ao_vizinho_perdido(viz, motivo),
    ao_receber_dados(rota, seq, rtp), ao_erro_fluxo(fluxo, motivo), comando(cmd, args)
Ao reescrever, chamar o método da classe base para o que não tratam.

Concorrência (D08): threads para aceitar ligações, uma por vizinho, heartbeats,
refresh, dados (UDP), manutenção (expirações e D36) e comandos. Todo o estado
partilhado (vizinhos, rotas, anúncios) é protegido por self.lock. Para nunca
bloquear com o lock na mão, as mensagens a enviar são juntadas numa lista
("ações") e enviadas depois de o largar.
"""

import logging
import socket
import sys
import threading
import time

import protocolo as P
from rotas import Candidata, Rota, comparar, comparar_candidatas, melhor_candidata


def agora_ms():
    return time.monotonic() * 1000.0


class Vizinho:
    """Uma ligação TCP ativa com um vizinho do overlay."""

    def __init__(self, nome, ip, sock, leitor, iniciador):
        self.nome = nome
        self.iniciador = iniciador            # nome do nó que abriu a ligação
        self.ip = ip
        self.sock = sock
        self.leitor = leitor
        self.ultimo_contacto = time.monotonic()
        self.atraso = None                    # ms, suavizado (D24, D39)
        self.hb_t_peer = None                 # último "t" recebido do vizinho
        self.hb_recebido = None               # quando o recebemos (ms)
        self._lock_envio = threading.Lock()   # várias threads podem enviar

    def enviar(self, msg):
        with self._lock_envio:
            self.sock.sendall(P.codificar(msg))

    def registar_rtt(self, rtt_ms):
        """Nova medição de RTT -> atualiza o atraso do link (≈ RTT/2, D39)."""
        medicao = max(rtt_ms, 0.0) / 2.0
        if self.atraso is None:
            self.atraso = medicao
        else:
            self.atraso = (1 - P.ALFA_ATRASO) * self.atraso + P.ALFA_ATRASO * medicao

    def atraso_ou_omissao(self):
        return self.atraso if self.atraso is not None else P.ATRASO_DESCONHECIDO

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
    PERIODO_MANUTENCAO = 0.2     # s

    def __init__(self, nome, ip_bootstrapper, comandos=True):
        self.nome = nome
        self.ip_bootstrapper = ip_bootstrapper
        self.ip = None                       # recebido do bootstrapper
        self.vizinhos = {}                   # nome -> Vizinho
        self.rotas = {}                      # fluxo -> Rota (D40)
        self.anuncios = {}                   # servidor -> {"id", "atraso", "saltos"} (D29)
        self.lock = threading.RLock()        # protege vizinhos, rotas, anuncios
        self.a_correr = threading.Event()
        self.com_comandos = comandos
        self._servidor = None
        self._udp = None
        self._a_religar = set()              # vizinhos com religação em curso
        self.falha_dados_simulada = False    # comando "x": testar a D36/D45
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

        # 2. Sockets: controlo (TCP) e dados (UDP, D12)
        self.a_correr.set()
        self._servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._servidor.bind((self.ip, P.PORTA_CONTROLO))
        self._servidor.listen()
        self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp.bind((self.ip, P.PORTA_DADOS))
        self._thread(self._ciclo_aceitar, "aceitar")
        self._thread(self._ciclo_dados, "dados")

        # 3. Ligar aos vizinhos que já estão ativos (D13)
        for v in resp["vizinhos"]:
            self.ligar(v["nome"], v["ip"])

        # 4. Heartbeats (D15), refresh (D18), manutenção (D30, D36) e comandos (D41)
        self._thread(self._ciclo_heartbeat, "heartbeat")
        self._thread(self._ciclo_refresh, "refresh")
        self._thread(self._ciclo_manutencao, "manutencao")
        if self.com_comandos:
            self._thread(self._ciclo_comandos, "comandos")

    def _thread(self, alvo, nome, *args):
        t = threading.Thread(target=alvo, args=args, daemon=True, name=nome)
        t.start()
        return t

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
        for s in (self._servidor, self._udp):
            if s:
                s.close()

    # =====================================================================
    # Estabelecimento de ligações
    # =====================================================================
    def ligar(self, nome, ip, tentativas=None):
        """Liga-se ativamente a um vizinho e troca HELLO nos dois sentidos."""
        tentativas = tentativas or self.TENTATIVAS_LIGACAO
        for tentativa in range(1, tentativas + 1):
            try:
                sock = socket.create_connection((ip, P.PORTA_CONTROLO), timeout=3)
                break
            except OSError as e:
                self.log.warning("ligação a %s (%s) falhou [%d/%d]: %s",
                                 nome, ip, tentativa, tentativas, e)
                if tentativa < tentativas:
                    time.sleep(self.INTERVALO_TENTATIVAS)
        else:
            return None
        try:
            leitor = P.LeitorLinhas(sock)
            t0 = agora_ms()
            sock.sendall(P.codificar(P.mensagem(P.HELLO, nome=self.nome, ip=self.ip)))
            resp = leitor.ler()                    # espera o HELLO de volta
            rtt = agora_ms() - t0                  # 1.ª estimativa do RTT (D24)
            if resp is None or resp["type"] != P.HELLO:
                raise ValueError("resposta inesperada ao HELLO: %s" % resp)
            sock.settimeout(None)
        except (OSError, ValueError) as e:
            self.log.warning("HELLO com %s falhou: %s", nome, e)
            sock.close()
            return None
        self.log.info("HELLO enviado a %s, HELLO recebido de %s",
                      nome, resp.get("nome"))
        return self._adicionar_vizinho(resp.get("nome", nome), resp.get("ip", ip),
                                       sock, leitor, iniciador=self.nome, rtt=rtt)

    def _ciclo_aceitar(self):
        while self.a_correr.is_set():
            try:
                sock, addr = self._servidor.accept()
            except OSError:
                break                               # socket fechado em sair()
            self._thread(self._aceitar_vizinho, "aceitar-viz", sock, addr)

    def _aceitar_vizinho(self, sock, addr):
        """Um vizinho ligou-se a nós: espera HELLO e responde com HELLO."""
        try:
            sock.settimeout(5)
            leitor = P.LeitorLinhas(sock)
            msg = leitor.ler()
            if msg is None or msg["type"] != P.HELLO or "nome" not in msg:
                raise ValueError("esperava HELLO, recebi %s" % msg)
            sock.sendall(P.codificar(P.mensagem(P.HELLO, nome=self.nome, ip=self.ip)))
            sock.settimeout(None)
        except (OSError, ValueError) as e:
            self.log.warning("ligação de %s rejeitada: %s", addr[0], e)
            sock.close()
            return
        self.log.info("HELLO recebido de %s (%s), HELLO enviado de volta",
                      msg["nome"], addr[0])
        # o IP de origem da ligação pode não ser o IP configurado do vizinho
        # (várias interfaces, D09): usa-se o IP que ele próprio indica no HELLO,
        # que é onde ele está à escuta (TCP 6000 e UDP 6001)
        self._adicionar_vizinho(msg["nome"], msg.get("ip", addr[0]), sock, leitor,
                                iniciador=msg["nome"])

    def _adicionar_vizinho(self, nome, ip, sock, leitor, iniciador, rtt=None):
        viz = Vizinho(nome, ip, sock, leitor, iniciador)
        if rtt is not None:
            viz.registar_rtt(rtt)
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
        self._thread(self._ciclo_leitura, "viz-" + nome, viz)
        self.log.info("vizinhos: %s", self.nomes_vizinhos())
        self._enviar_heartbeat(viz)                 # mede já o RTT (D24)
        self.ao_vizinho_novo(viz)
        return viz

    # =====================================================================
    # Receção de mensagens de controlo
    # =====================================================================
    def _ciclo_leitura(self, viz):
        motivo = "ligação fechada"
        try:
            while self.a_correr.is_set():
                msg = viz.leitor.ler()
                if msg is None:
                    break
                viz.ultimo_contacto = time.monotonic()
                try:
                    self.ao_receber(viz, msg)
                except (KeyError, TypeError, ValueError) as e:
                    self.log.warning("mensagem inválida de %s (%s): %s", viz.nome, e, msg)
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
    # Heartbeats com medição do RTT (D11, D15, D24, D39)
    # =====================================================================
    def _enviar_heartbeat(self, viz):
        """
        HEARTBEAT {t, eco, espera}: "t" é a nossa hora; "eco" devolve o último
        "t" recebido do vizinho e "espera" diz há quanto tempo o recebemos.
        Quem recebe o eco calcula RTT = agora - eco - espera, usando só o seu
        próprio relógio (não precisa de relógios sincronizados, D24).
        """
        agora = agora_ms()
        msg = P.mensagem(P.HEARTBEAT, nome=self.nome, t=agora)
        if viz.hb_t_peer is not None:
            msg["eco"] = viz.hb_t_peer
            msg["espera"] = agora - viz.hb_recebido
        try:
            viz.enviar(msg)
        except OSError as e:
            self._perder_vizinho(viz, "falha a enviar heartbeat: %s" % e)

    def _tratar_heartbeat(self, viz, msg):
        agora = agora_ms()
        viz.hb_t_peer = msg.get("t")
        viz.hb_recebido = agora
        if msg.get("eco") is not None:
            viz.registar_rtt(agora - msg["eco"] - msg.get("espera", 0.0))
        elif viz.hb_t_peer is not None:
            # o vizinho ainda não tem eco nosso: responde já, para ele medir
            self._enviar_heartbeat(viz)

    def _ciclo_heartbeat(self):
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
                self._enviar_heartbeat(viz)

    # =====================================================================
    # Refresh no bootstrapper e religação (D18, D21)
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
                    self._religar_em_fundo(v["nome"], v["ip"])

    def _religar_em_fundo(self, nome, ip):
        """
        Religação numa thread própria, para o refresh nunca ficar bloqueado à
        espera de timeouts (D21). Uma só tentativa: se falhar, o refresh
        seguinte volta a tentar. Nunca há duas religações ao mesmo vizinho.
        """
        with self.lock:
            if nome in self._a_religar:
                return
            self._a_religar.add(nome)
        self.log.info("refresh: %s está ativo mas não ligado -> a religar", nome)

        def tarefa():
            try:
                self.ligar(nome, ip, tentativas=1)
            finally:
                with self.lock:
                    self._a_religar.discard(nome)

        self._thread(tarefa, "religar-" + nome)

    # =====================================================================
    # Anúncios (D22, D28, D29, D30, D37)
    # =====================================================================
    def _tratar_anuncio(self, viz, msg):
        caminho = msg["caminho"]
        if self.nome in caminho:
            return                                  # volta a mim: ciclo (D29)
        servidor = msg["servidor"]
        ident = (msg["arranque"], msg["seq"])
        atraso = msg["custo"] + viz.atraso_ou_omissao()
        saltos = msg["saltos"] + 1
        agora = time.monotonic()
        acoes = []
        with self.lock:
            estado = self.anuncios.get(servidor)
            if estado is not None and ident < estado["id"]:
                return                              # anúncio antigo
            # 1. Registar a candidata em cada fluxo do servidor (D28, D40)
            for nome_fluxo in msg["fluxos"]:
                fluxo = "%s/%s" % (servidor, nome_fluxo)
                rota = self.rotas.get(fluxo)
                if rota is None:
                    rota = self.rotas[fluxo] = Rota(fluxo, servidor)
                    self.log.info("novo fluxo conhecido: %s (via %s, %.1f ms, %d saltos)",
                                  fluxo, viz.nome, atraso, saltos)
                if rota.fonte:
                    continue
                cand = rota.candidatas.get(viz.nome)
                if cand is None:
                    cand = rota.candidatas[viz.nome] = Candidata(viz.nome)
                cand.atraso, cand.saltos = atraso, saltos
                cand.caminho = caminho
                cand.ultimo_anuncio = agora
                self._avaliar_troca(rota, cand, agora, acoes)
            # 2. Reencaminhar? 1.ª cópia deste nº, ou cópia com custo melhor (D29)
            reenviar = (estado is None or ident > estado["id"] or
                        comparar(atraso, saltos, estado["atraso"], estado["saltos"]) < 0)
            if reenviar:
                self.anuncios[servidor] = {"id": ident, "atraso": atraso, "saltos": saltos}
                novo = dict(msg, custo=atraso, saltos=saltos, caminho=caminho + [self.nome])
                for nome in self.vizinhos:
                    if nome != viz.nome and nome not in caminho:
                        acoes.append((nome, novo))
        self._executar(acoes)

    def _avaliar_troca(self, rota, cand, agora, acoes):
        """
        Chamada com o lock, depois de atualizar `cand`:
          * rota ativa à espera de Origem (D35, sem reserva) -> ativa já;
          * D37: conta anúncios seguidos em que `cand` é melhor do que a Origem
            e, ao 3.º, inicia a troca sem corte (D38).
        """
        if not rota.ativa() or rota.fonte:
            return
        if rota.origem is None:
            nova = melhor_candidata(rota, excluir=rota.destinos, agora=agora)
            if nova is not None:
                self._definir_origem(rota, nova, agora, acoes,
                                     "caminho novo após falha sem reserva")
            return
        if cand.vizinho == rota.origem or rota.trocar_para is not None:
            cand.melhor_seguidos = 0
            return
        atual = rota.candidatas.get(rota.origem)
        if (atual is None or cand.vizinho in rota.destinos or cand.suspensa(agora)
                or comparar_candidatas(cand, atual) >= 0):
            cand.melhor_seguidos = 0
            return
        cand.melhor_seguidos += 1
        if cand.melhor_seguidos >= P.SEGUIDOS_PARA_TROCAR:
            cand.melhor_seguidos = 0
            rota.trocar_para = cand.vizinho
            rota.trocar_desde = agora
            self.log.info("%s: %s é melhor há %d anúncios (%.1f ms/%d saltos contra "
                          "%.1f ms/%d) -> troca sem corte iniciada (D37, D38)",
                          rota.fluxo, cand.vizinho, P.SEGUIDOS_PARA_TROCAR,
                          cand.atraso, cand.saltos, atual.atraso, atual.saltos)
            acoes.append((cand.vizinho, P.mensagem(P.ATIVAR, fluxo=rota.fluxo)))

    # =====================================================================
    # Ativação, desativação, poda e erros (D31–D35, D44)
    # =====================================================================
    def pedir_fluxo(self, fluxo, pedinte=None):
        """
        Pedido de ativação: de um vizinho (`pedinte`) ou do próprio nó
        (pedinte=None, quando é cliente). Devolve (ok, motivo).
        """
        agora = time.monotonic()
        acoes = []
        with self.lock:
            rota = self.rotas.get(fluxo)
            if rota is None:
                ok, motivo = False, "fluxo desconhecido"
            elif pedinte is not None and pedinte in (rota.origem, rota.trocar_para):
                # quem me envia o fluxo não o pode receber de mim (seria um ciclo)
                ok, motivo = False, "pedido em ciclo"
            elif rota.fonte or rota.ativa():
                # já recebe (ou gera) o fluxo: só acrescenta e não propaga (D32)
                self._juntar_pedinte(rota, pedinte)
                if rota.fonte and not rota.ativa():
                    rota.estado = "ativa"
                ok, motivo = True, None
            else:
                excluir = set(rota.destinos)
                if pedinte:
                    excluir.add(pedinte)
                origem = melhor_candidata(rota, excluir=excluir, agora=agora)
                if origem is None:
                    ok, motivo = False, "sem rota para o fluxo"
                else:
                    self._juntar_pedinte(rota, pedinte)
                    rota.estado = "ativa"
                    rota.reset_dados()
                    self._definir_origem(rota, origem, agora, acoes, "pedido de %s"
                                         % (pedinte or "cliente local"))
                    ok, motivo = True, None
            if not ok and pedinte:
                acoes.append((pedinte, P.mensagem(P.ATIVAR_ERRO, fluxo=fluxo,
                                                  motivo=motivo)))
        if pedinte:
            self.log.info("ATIVAR %s de %s -> %s", fluxo, pedinte,
                          "ok" if ok else "ATIVAR_ERRO (%s)" % motivo)
        self._executar(acoes)
        return ok, motivo

    def _juntar_pedinte(self, rota, pedinte):
        if pedinte:
            rota.destinos.add(pedinte)
        else:
            rota.sou_cliente = True

    def _definir_origem(self, rota, origem, agora, acoes, motivo):
        """Com o lock: passa a receber `rota` por `origem` (envia ATIVAR)."""
        antiga = rota.origem
        rota.origem = origem
        rota.trocar_para = None
        rota.ultimo_pacote = agora                  # dá 1 s até ao 1.º pacote (D36)
        for c in rota.candidatas.values():
            c.melhor_seguidos = 0
        acoes.append((origem, P.mensagem(P.ATIVAR, fluxo=rota.fluxo)))
        self.log.info("%s: Origem %s -> %s (%s); Destinos %s%s", rota.fluxo,
                      antiga or "—", origem, motivo, sorted(rota.destinos) or "—",
                      " + cliente local" if rota.sou_cliente else "")

    def largar_fluxo(self, fluxo, pedinte=None):
        """DESATIVAR de um vizinho, ou o cliente local deixou de ver."""
        acoes = []
        with self.lock:
            rota = self.rotas.get(fluxo)
            if rota is None:
                return
            if pedinte:
                rota.destinos.discard(pedinte)
            else:
                rota.sou_cliente = False
            self._podar(rota, acoes)
        self._executar(acoes)

    def _podar(self, rota, acoes):
        """Com o lock: sem Destinos e sem cliente local -> desativa já (D33)."""
        if not rota.ativa() or rota.destinos or rota.sou_cliente:
            return
        rota.estado = "inativa"
        if rota.fonte:
            self.log.info("%s: sem Destinos -> deixa de enviar", rota.fluxo)
            return
        for v in {rota.origem, rota.trocar_para} - {None}:
            acoes.append((v, P.mensagem(P.DESATIVAR, fluxo=rota.fluxo)))
        self.log.info("%s: sem Destinos -> DESATIVAR a %s (poda)", rota.fluxo,
                      rota.origem or "—")
        rota.origem = None
        rota.trocar_para = None
        rota.reset_dados()

    def _tratar_ativar_erro(self, viz, msg):
        """D44: a minha Origem não tem rota -> tenta reservas; sem reserva, passa o erro."""
        fluxo = msg["fluxo"]
        acoes = []
        erro_local = False
        with self.lock:
            rota = self.rotas.get(fluxo)
            if rota is None or rota.fonte:
                return
            rota.candidatas.pop(viz.nome, None)
            if rota.trocar_para == viz.nome:        # troca D38 falhou: mantém a atual
                rota.trocar_para = None
                self.log.info("%s: troca para %s cancelada (ATIVAR_ERRO)", fluxo, viz.nome)
            elif rota.origem == viz.nome:
                if not self._falha_origem(rota, "ATIVAR_ERRO de %s" % viz.nome,
                                          acoes, suspender=False):
                    # sem reserva: passa o erro a quem pediu (D34)
                    for d in rota.destinos:
                        acoes.append((d, P.mensagem(P.ATIVAR_ERRO, fluxo=fluxo,
                                                    motivo=msg.get("motivo"))))
                    rota.destinos.clear()
                    erro_local = rota.sou_cliente
                    rota.sou_cliente = False
                    rota.estado = "inativa"
                    rota.origem = None
        self._executar(acoes)
        if erro_local:
            self.ao_erro_fluxo(fluxo, msg.get("motivo"))

    def _falha_origem(self, rota, motivo, acoes, suspender):
        """
        Com o lock. A Origem de `rota` falhou: passa à melhor reserva (D35).
        Com suspender=True (falta de dados, D36), a candidata antiga fica
        suspensa 10 s e recebe DESATIVAR (D45). Devolve True se há reserva.
        """
        agora = time.monotonic()
        antiga = rota.origem
        if suspender and antiga in rota.candidatas:
            rota.candidatas[antiga].suspensa_ate = agora + P.SUSPENSAO
            acoes.append((antiga, P.mensagem(P.DESATIVAR, fluxo=rota.fluxo)))
        if rota.trocar_para is not None:
            # se havia uma troca em curso, a nova Origem passa a ser essa
            nova = rota.trocar_para
        else:
            nova = melhor_candidata(rota, excluir=rota.destinos | {antiga}, agora=agora)
        # (o último nº de sequência mantém-se: evita reenviar repetidos se a
        #  nova Origem vier "atrasada" em relação à antiga)
        if nova is not None:
            self.log.info("%s: Origem %s falhou (%s) -> reserva %s (D35)",
                          rota.fluxo, antiga, motivo, nova)
            if nova == rota.trocar_para:
                rota.origem, rota.trocar_para = nova, None
                rota.ultimo_pacote = agora
            else:
                self._definir_origem(rota, nova, agora, acoes, "reserva")
            return True
        self.log.warning("%s: Origem %s falhou (%s) e não há reserva -> à espera "
                         "de um anúncio (D35)", rota.fluxo, antiga, motivo)
        rota.origem = None
        rota.trocar_para = None
        return False

    # =====================================================================
    # Plano de dados (D23, D36, D38, D42, D46)
    # =====================================================================
    def _ciclo_dados(self):
        while self.a_correr.is_set():
            try:
                pacote, _ = self._udp.recvfrom(65535)
            except OSError:
                break
            try:
                fluxo, remetente, rtp = P.descodificar_dados(pacote)
            except ValueError as e:
                self.log.debug("pacote de dados descartado: %s", e)
                continue
            self.reencaminhar(fluxo, remetente, rtp)

    def reencaminhar(self, fluxo, remetente, rtp):
        """
        Trata um pacote de dados: aceita-o só se vier da Origem (ou da nova
        Origem numa troca, D38), descarta repetidos pelo nº de sequência e
        envia uma cópia a cada Destino. remetente=None: gerado por este nó.
        """
        if self.falha_dados_simulada and remetente is not None:
            return                                  # comando "x": nó "avariado"
        seq = P.rtp_seq(rtp)
        agora = time.monotonic()
        acoes = []
        with self.lock:
            rota = self.rotas.get(fluxo)
            if rota is None or not rota.ativa():
                return
            if remetente is not None:
                if remetente == rota.origem:
                    rota.ultimo_pacote = agora
                    rota.sem_dados_avisado = False
                elif remetente == rota.trocar_para:
                    # 1.º pacote pela nova Origem: termina a troca (D38)
                    antiga = rota.origem
                    rota.origem, rota.trocar_para = remetente, None
                    rota.ultimo_pacote = agora
                    if antiga:
                        acoes.append((antiga, P.mensagem(P.DESATIVAR, fluxo=fluxo)))
                    self.log.info("%s: troca sem corte concluída: Origem %s -> %s (D38)",
                                  fluxo, antiga, remetente)
                else:
                    return                          # remetente que já não é Origem
            novo = (rota.ultimo_seq is None
                    or P.seq_mais_recente(seq, rota.ultimo_seq)
                    or agora - rota.ultimo_aceite > P.VIDEO_TIMEOUT)
            if not novo:
                rota.duplicados += 1
                destinos = []
                local = False
            else:
                if rota.ultimo_seq is not None:
                    salto = (seq - rota.ultimo_seq) & 0xFFFF
                    if 1 < salto < 0x8000:
                        rota.perdidos += salto - 1
                rota.ultimo_seq = seq
                rota.ultimo_aceite = agora
                rota.recebidos += 1
                destinos = [self.vizinhos[d].ip for d in rota.destinos if d in self.vizinhos]
                local = rota.sou_cliente
        self._executar(acoes)
        if destinos:
            pacote = P.codificar_dados(fluxo, self.nome, rtp)
            for ip in destinos:
                try:
                    self._udp.sendto(pacote, (ip, P.PORTA_DADOS))
                except OSError as e:
                    self.log.debug("envio de dados para %s falhou: %s", ip, e)
        if local:
            self.ao_receber_dados(rota, seq, rtp)

    # =====================================================================
    # Manutenção: expiração (D30), falta de dados (D36), trocas pendentes (D38)
    # =====================================================================
    def _ciclo_manutencao(self):
        while self.a_correr.is_set():
            time.sleep(self.PERIODO_MANUTENCAO)
            agora = time.monotonic()
            acoes = []
            erros_locais = []
            with self.lock:
                for fluxo, rota in list(self.rotas.items()):
                    if rota.fonte:
                        continue
                    # D30: candidatas sem anúncio há mais de 6 s
                    for nome, c in list(rota.candidatas.items()):
                        if agora - c.ultimo_anuncio > P.ROTA_TIMEOUT:
                            del rota.candidatas[nome]
                            self.log.info("%s: rota por %s expirou (sem anúncio há %.0fs)",
                                          fluxo, nome, agora - c.ultimo_anuncio)
                            if rota.trocar_para == nome:
                                rota.trocar_para = None
                            if rota.ativa() and rota.origem == nome:
                                self._falha_origem(rota, "rota expirou", acoes,
                                                   suspender=False)
                    if not rota.candidatas:
                        # já ninguém anuncia o fluxo (ex.: o servidor saiu)
                        del self.rotas[fluxo]
                        self.log.info("fluxo %s deixou de estar disponível", fluxo)
                        if rota.ativa():
                            for d in rota.destinos:
                                acoes.append((d, P.mensagem(
                                    P.ATIVAR_ERRO, fluxo=fluxo,
                                    motivo="fluxo deixou de ser anunciado")))
                            if rota.sou_cliente:
                                erros_locais.append(fluxo)
                        continue
                    if not rota.ativa():
                        continue
                    # D36: ~1 s sem dados pela Origem. Só muda quem tem reserva
                    # (D47): sem reserva mantém a Origem, para não haver cascata.
                    if (rota.origem is not None
                            and agora - rota.ultimo_pacote > P.VIDEO_TIMEOUT):
                        reserva = melhor_candidata(
                            rota, excluir=rota.destinos | {rota.origem}, agora=agora)
                        if reserva is not None or rota.trocar_para is not None:
                            self._falha_origem(rota, "%.1f s sem dados" %
                                               (agora - rota.ultimo_pacote), acoes,
                                               suspender=True)
                        elif not rota.sem_dados_avisado:
                            rota.sem_dados_avisado = True
                            self.log.info("%s: %.1f s sem dados de %s, mas sem reserva "
                                          "-> mantém a Origem (D47)", fluxo,
                                          agora - rota.ultimo_pacote, rota.origem)
                    # D38: troca iniciada mas a nova Origem não enviou nada
                    if (rota.trocar_para is not None
                            and agora - rota.trocar_desde > P.VIDEO_TIMEOUT):
                        falhada = rota.trocar_para
                        rota.trocar_para = None
                        c = rota.candidatas.get(falhada)
                        if c:
                            c.suspensa_ate = agora + P.SUSPENSAO
                        acoes.append((falhada, P.mensagem(P.DESATIVAR, fluxo=fluxo)))
                        self.log.info("%s: troca para %s cancelada (sem dados em %.0f s)",
                                      fluxo, falhada, P.VIDEO_TIMEOUT)
            self._executar(acoes)
            for fluxo in erros_locais:
                self.ao_erro_fluxo(fluxo, "fluxo deixou de ser anunciado")

    # =====================================================================
    # Comandos no terminal (D41)
    # =====================================================================
    def _ciclo_comandos(self):
        while self.a_correr.is_set():
            linha = sys.stdin.readline()
            if not linha:
                return                              # sem terminal (ex.: testes)
            partes = linha.split()
            if partes:
                try:
                    self.comando(partes[0].lower(), partes[1:])
                except Exception as e:              # um comando nunca mata o nó
                    print("erro no comando: %s" % e)

    def comando(self, cmd, args):
        if cmd == "t":
            print(self.texto_tabela(), flush=True)
        elif cmd == "v":
            print(self.texto_vizinhos(), flush=True)
        elif cmd == "x":
            self.falha_dados_simulada = not self.falha_dados_simulada
            self.log.warning("falha simulada nos dados %s (o controlo continua normal)",
                             "LIGADA: não reencaminha dados" if self.falha_dados_simulada
                             else "DESLIGADA")
        elif cmd in ("h", "?", "ajuda"):
            print(self.texto_ajuda(), flush=True)
        else:
            print("comando desconhecido: %s (h = ajuda)" % cmd, flush=True)

    def texto_ajuda(self):
        return ("Comandos: t = tabela de rotas | v = vizinhos e atrasos | "
                "x = simular falha só nos dados (liga/desliga) | h = ajuda")

    def texto_vizinhos(self):
        with self.lock:
            linhas = ["[%s] Vizinhos:" % self.nome]
            for nome in sorted(self.vizinhos):
                v = self.vizinhos[nome]
                atraso = "%.2f ms" % v.atraso if v.atraso is not None else "por medir"
                linhas.append("  %-8s %-15s atraso do link %s" % (nome, v.ip, atraso))
            if len(linhas) == 1:
                linhas.append("  (nenhum)")
        return "\n".join(linhas)

    def texto_tabela(self):
        agora = time.monotonic()
        with self.lock:
            linhas = ["[%s] Tabela de rotas:" % self.nome]
            if not self.rotas:
                linhas.append("  (vazia)")
            for fluxo in sorted(self.rotas):
                r = self.rotas[fluxo]
                if r.fonte:
                    linhas.append("  %-22s %-7s FONTE   Destinos: %s" %
                                  (fluxo, r.estado, ",".join(sorted(r.destinos)) or "—"))
                    continue
                extra = ""
                if r.trocar_para:
                    extra = "  (a trocar para %s)" % r.trocar_para
                linhas.append("  %-22s %-7s Origem: %-6s Destinos: %s%s%s" % (
                    fluxo, r.estado, r.origem or "—",
                    ",".join(sorted(r.destinos)) or "—",
                    " + cliente local" if r.sou_cliente else "", extra))
                for nome in sorted(r.candidatas):
                    c = r.candidatas[nome]
                    marca = "*" if nome == r.origem else " "
                    susp = "  SUSPENSA %.0fs" % (c.suspensa_ate - agora) if c.suspensa(agora) else ""
                    linhas.append("     %s via %-6s %7.2f ms %2d saltos  caminho %-24s há %.1fs%s" % (
                        marca, nome, c.atraso, c.saltos, ">".join(c.caminho),
                        agora - c.ultimo_anuncio, susp))
                if r.ativa():
                    linhas.append("       dados: %d recebidos, %d perdidos, %d duplicados descartados"
                                  % (r.recebidos, r.perdidos, r.duplicados))
        return "\n".join(linhas)

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

    def _executar(self, acoes):
        """Envia as mensagens juntadas enquanto se tinha o lock."""
        for nome, msg in acoes:
            self.enviar_a(nome, msg)

    def fluxos_disponiveis(self):
        """Fluxos com pelo menos uma rota conhecida (lista para o cliente, D34)."""
        with self.lock:
            return sorted(f for f, r in self.rotas.items() if r.candidatas or r.fonte)

    # =====================================================================
    # Hooks — reescrever nas subclasses (Servidor, Cliente)
    # =====================================================================
    def ao_receber(self, viz, msg):
        tipo = msg["type"]
        if tipo == P.HEARTBEAT:
            self._tratar_heartbeat(viz, msg)
        elif tipo == P.ANUNCIO:
            self._tratar_anuncio(viz, msg)
        elif tipo == P.ATIVAR:
            self.pedir_fluxo(msg["fluxo"], pedinte=viz.nome)
        elif tipo == P.DESATIVAR:
            self.log.info("DESATIVAR %s de %s", msg["fluxo"], viz.nome)
            self.largar_fluxo(msg["fluxo"], pedinte=viz.nome)
        elif tipo == P.ATIVAR_ERRO:
            self.log.info("ATIVAR_ERRO %s de %s: %s", msg["fluxo"], viz.nome,
                          msg.get("motivo"))
            self._tratar_ativar_erro(viz, msg)
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
        """Retira o vizinho das rotas: candidatas, Destinos e Origem (D35)."""
        acoes = []
        with self.lock:
            for rota in self.rotas.values():
                rota.candidatas.pop(viz.nome, None)
                if rota.trocar_para == viz.nome:
                    rota.trocar_para = None
                if viz.nome in rota.destinos:
                    rota.destinos.discard(viz.nome)
                    self._podar(rota, acoes)
                if rota.ativa() and not rota.fonte and rota.origem == viz.nome:
                    self._falha_origem(rota, "vizinho perdido: %s" % motivo, acoes,
                                       suspender=False)
        self._executar(acoes)

    def ao_receber_dados(self, rota, seq, rtp):
        """Chamada (sem o lock) para cada pacote novo de um fluxo que este nó vê."""
        pass

    def ao_erro_fluxo(self, fluxo, motivo):
        """O fluxo que este nó (cliente) pediu ficou indisponível (D34, D44)."""
        self.log.warning("fluxo %s indisponível: %s", fluxo, motivo)

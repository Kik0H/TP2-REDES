# Registo de Decisões — TP2 ESR (Serviço OTT para entrega de multimédia)

Registo das decisões tomadas pelo grupo ao longo do trabalho, com a respetiva
justificação e alternativas consideradas. Serve de base às secções
"Arquitetura", "Especificação dos protocolos" e "Implementação" do relatório.

Formato de cada entrada: **contexto → decisão → porquê → alternativas rejeitadas**.

---

## Etapa 0 — Preparação

### D01 · Linguagem de programação: Python
- **Data:** 2026-10-27
- **Decisão:** Implementar o projeto em Python 3.
- **Porquê:** Considerada a opção mais fácil para o grupo; o código de exemplo
  fornecido pelos docentes (livro Kurose, adaptado) existe em Python e pode ser
  reaproveitado na Etapa 4 (streaming).
- **Alternativas:** Java (o exemplo também existe em Java).

### D02 · Transporte: TCP para controlo do overlay, UDP para o streaming
- **Data:** 2026-10-27
- **Decisão:**
  - **TCP** para as mensagens de controlo entre nós do overlay (vizinhança,
    anúncios de rotas, ativação, heartbeats, etc.).
  - **UDP** para os pacotes de vídeo (RTP).
- **Porquê:**
  - As mensagens de controlo não se podem perder (um anúncio ou ativação de rota
    perdido deixa a árvore de distribuição inconsistente). O TCP garante entrega
    e ordem sem termos de implementar ACKs/retransmissões.
  - No streaming em tempo real um frame atrasado já não tem utilidade; com UDP
    um pacote perdido é simplesmente ignorado e o vídeo continua. Com TCP, uma
    perda bloquearia os frames seguintes até à retransmissão (bloqueio à cabeça
    da fila), causando congelamentos.
  - É a mesma divisão usada no exemplo fornecido (RTSP sobre TCP + RTP sobre UDP).
- **Alternativas rejeitadas:**
  - Tudo em TCP — congelamentos no vídeo em caso de perda.
  - Tudo em UDP — obrigaria a implementar fiabilidade própria para o controlo.
- **Implicações a ter em conta (por decidir):**
  - TCP é um fluxo de bytes → é preciso definir o enquadramento das mensagens.
  - TCP não deteta rapidamente um vizinho que "morreu" sem fechar a ligação →
    serão necessários heartbeats ao nível da aplicação.
  - Os pacotes UDP têm de identificar o fluxo (servidor + stream) para os nós
    intermédios saberem reencaminhar.
- **Ideia para os testes:** comparar o comportamento do vídeo com perda
  configurada nos links do CORE (ex.: 5%) para justificar a escolha.

### D03 · Buffer no cliente (intenção, Etapa 4)
- **Data:** 2026-10-27
- **Decisão (provisória):** O cliente terá um pequeno buffer na aplicação
  (jitter buffer) que ordena os frames pelo número de sequência, descarta
  frames atrasados e não pede retransmissões.
- **Porquê:** Absorver variações de atraso sem precisar de confirmações.
- **Estado:** a detalhar na Etapa 4.

### D04 · Repositório: o código de exemplo não é versionado
- **Data:** 2026-10-27
- **Decisão:** A pasta `ProgEx/` (exemplo dos docentes) fica no `.gitignore`;
  o enunciado é versionado.

## Etapa 1 — Construção da topologia overlay

### D05 · Construção do overlay: estratégia com bootstrapper
- **Data:** 2026-10-27
- **Decisão:** Cada nó arranca com `ott <bootstrapper>`, regista-se no
  bootstrapper e recebe como resposta a lista dos seus vizinhos. O bootstrapper
  obtém a informação "quem liga a quem" de um ficheiro de configuração
  construído manualmente.
- **Porquê:** Corresponde ao requisito do enunciado ("ao iniciar, o nó tem de
  conhecer pelo menos um outro nó, que o ajuda a obter a sua lista de X
  vizinhos") e facilita a entrada/saída dinâmica de nós, ao contrário da
  abordagem manual em que a vizinhança é fixa.
- **Alternativas rejeitadas:** Estratégia manual (`ott viz1 viz2 viz3`) —
  mais simples, mas a tabela de vizinhos não se adapta durante a execução.
- **Por decidir:** estratégia de abandono (o nó avisa? registo periódico?),
  formato do ficheiro de configuração.

### D06 · Bootstrapper como processo dedicado
- **Data:** 2026-10-27
- **Decisão:** O bootstrapper é um programa à parte, num endereço bem
  conhecido, independente dos servidores de streaming.
- **Porquê:** Como o trabalho exige pelo menos 2 servidores, evita eleger um
  deles como "especial"; separa a função de controlo da topologia da função de
  servir conteúdos.
- **Alternativas rejeitadas:** Usar um dos servidores de streaming como
  bootstrapper (sugestão do enunciado) — menos processos, mas mistura funções.

### D07 · Formato das mensagens de controlo: JSON, uma mensagem por linha
- **Data:** 2026-10-27
- **Decisão:** Cada mensagem de controlo é um objeto JSON codificado em UTF-8
  e terminado por `\n` (ex.: `{"type": "HELLO", ...}\n`). O campo `type`
  identifica o tipo de mensagem.
- **Porquê:** Resolve o enquadramento sobre TCP (que é um fluxo de bytes) de
  forma simples; é legível, fácil de depurar e de documentar; novos tipos de
  mensagem acrescentam-se sem mudar a estrutura.
- **Alternativas rejeitadas:**
  - Prefixo de tamanho + JSON — mais robusto, mas mais trabalhoso.
  - Binário (struct) — compacto, mas difícil de depurar e de estender.

### D08 · Concorrência dentro de cada nó: threads
- **Data:** 2026-10-27
- **Decisão:** Cada nó usa threads: uma para aceitar novas ligações, uma por
  cada ligação de vizinho (e, mais tarde, para o socket UDP e para tarefas
  periódicas como heartbeats). As estruturas partilhadas (tabela de vizinhos,
  tabela de rotas) são protegidas com locks (`threading.Lock`).
- **Porquê:** `recv()`/`accept()` bloqueiam; com uma thread por socket cada uma
  pode bloquear sem parar o resto do nó. O código de cada thread lê-se como
  código sequencial, é o modelo já usado no exemplo fornecido (facilita
  reaproveitar o streaming) e, com poucos vizinhos por nó (3–5), o número de
  threads é pequeno.
- **Custo assumido:** acesso concorrente às tabelas → obrigatório usar locks.
- **Alternativas rejeitadas:**
  - `selectors` — um só ciclo de eventos sem locks, mas nada pode bloquear e
    buffers/temporizadores têm de ser geridos à mão.
  - `asyncio` — sem locks e código limpo, mas obriga a tornar todo o programa
    assíncrono e a adaptar o código de streaming do exemplo.

### D09 · Identificação dos nós por nome lógico
- **Data:** 2026-10-27
- **Decisão:** Cada nó é identificado por um nome lógico (ex.: `O3`), passado
  no arranque: `ott O3 <bootstrapper>`. O nó indica o seu nome ao registar-se;
  o ficheiro de configuração do bootstrapper associa cada nome ao seu IP e aos
  seus vizinhos.
- **Porquê:** No CORE uma máquina pode ter várias interfaces/IPs, e o IP de
  origem de uma ligação depende da interface por onde sai — pode não coincidir
  com o IP configurado. Um nome lógico elimina essa ambiguidade e torna os
  logs mais legíveis.
- **Alternativas rejeitadas:** Identificar pelo IP de origem da ligação —
  mais simples, mas frágil com múltiplas interfaces.

### D10 · Ficheiro de configuração do bootstrapper em JSON
- **Data:** 2026-10-27
- **Decisão:** A topologia do overlay (nome → IP + lista de vizinhos) é
  descrita num ficheiro JSON, ex.:
  `{"O3": {"ip": "10.0.1.2", "vizinhos": ["O1", "O5"]}}`.
- **Porquê:** Lido com a biblioteca standard (`json`), sem dependências;
  coerente com o formato das mensagens (D07).
- **Alternativas rejeitadas:**
  - Texto simples — rápido de escrever, mas exige parser próprio.
  - YAML — requer PyYAML, que pode não estar disponível nas máquinas do CORE.

### D11 · Deteção de saída de nós: mensagem LEAVE + heartbeats
- **Data:** 2026-10-27
- **Decisão:**
  - **Saída limpa** (ex.: Ctrl+C): o nó envia `LEAVE` aos vizinhos e ao
    bootstrapper antes de terminar.
  - **Saída abrupta** (processo morto, link em baixo): os vizinhos trocam
    heartbeats periódicos e declaram o vizinho morto após um timeout.
- **Porquê:** Cobre os dois casos. O LEAVE permite reagir de imediato numa
  saída normal; os heartbeats são necessários porque o TCP, por si só, não
  deteta rapidamente um par que desapareceu sem fechar a ligação (D02).
- **Alternativas rejeitadas:**
  - Só heartbeats — uma saída limpa demoraria tanto a detetar como uma falha.
  - Registo periódico no bootstrapper — o bootstrapper saberia quem está vivo,
    mas os vizinhos continuariam a precisar de detetar falhas entre si.
- **Por decidir:** período e timeout dos heartbeats.

### D12 · Portas fixas
- **Data:** 2026-10-27
- **Decisão:** Todos os nós usam **TCP 6000** (controlo) e **UDP 6001**
  (dados); o bootstrapper escuta em **TCP 5000**.
- **Porquê:** Portas iguais em todos os nós simplificam a configuração (basta
  saber o IP do vizinho); números distintos para TCP e UDP facilitam a leitura
  do código e das capturas no Wireshark; não colidem com a porta do exemplo
  (25000).
- **Alternativas rejeitadas:** mesmo número para TCP e UDP (possível, mas
  confuso); portas configuráveis (flexível, mas desnecessário por agora).

### D13 · Ligação entre vizinhos: só a vizinhos já ativos, iniciada pelo nó mais recente
- **Data:** 2026-10-27
- **Decisão:** Ao registar-se, o nó recebe do bootstrapper apenas os vizinhos
  configurados **que já estão registados/ativos**, e liga-se a eles. Os
  vizinhos que arrancarem mais tarde é que se ligam a este nó.
- **Porquê:** Quem inicia a ligação é sempre o nó que chega por último, logo
  nunca há duas ligações para o mesmo par (sem necessidade de desempate) e
  ninguém tenta ligar-se a um nó que ainda não está a correr.
- **Alternativas rejeitadas:** Devolver todos os vizinhos configurados e
  tentar periodicamente os que faltam — possíveis ligações duplicadas quando
  dois nós tentam em simultâneo, exigindo uma regra de desempate.

### D14 · Papéis: núcleo comum de nó overlay + programas por papel
- **Data:** 2026-10-27
- **Decisão:** Um módulo com a lógica comum de nó do overlay (vizinhos,
  heartbeats, rotas, reencaminhamento), sobre o qual assentam os programas de
  cada papel: nó intermédio (`ott`), servidor de streaming e cliente.
- **Porquê:** O enunciado define que servidores e clientes também são nós do
  overlay; partilhando o núcleo, a lógica do overlay existe num só sítio.
- **Alternativas rejeitadas:**
  - Um único programa com flags (`--servidor`, `--cliente`) — mistura
    responsabilidades num ficheiro grande.
  - Programas independentes — duplicaria a lógica do overlay.
- **Por decidir:** como os papéis reutilizam o núcleo (herança vs. composição).

### D15 · Heartbeats: 2 s de período, 6 s de timeout
- **Data:** 2026-10-27
- **Decisão:** Cada nó envia `HEARTBEAT` a cada vizinho a cada 2 s; um vizinho
  sem mensagens há 6 s (3 heartbeats falhados) é declarado morto.
- **Porquê:** Deteção de falhas em ~6 s com pouco tráfego de controlo; tolera
  a perda/atraso pontual de um heartbeat sem falsos positivos.
- **Alternativas rejeitadas:** 1 s/3 s (o dobro do tráfego, mais falsos
  positivos em links com atraso/perdas); 5 s/15 s (falhas detetadas tarde, o
  vídeo ficaria parado até 15 s).

### D16 · Organização do repositório: `src/` com módulos
- **Data:** 2026-10-27
- **Decisão:** Código em `src/` dividido em módulos (ex.: protocolo de
  mensagens, núcleo do nó, bootstrapper, programas por papel); topologias de
  teste (JSON do bootstrapper e `.imn` do CORE) em `topologias/`;
  documentação em `docs/`.
- **Porquê:** Separação de responsabilidades; escala à medida que o projeto
  cresce.

### D17 · Reutilização do núcleo por herança
- **Data:** 2026-10-27
- **Decisão:** O núcleo é uma classe `No` (módulo `no.py`); os papéis são
  subclasses importadas desse módulo — `class Servidor(No)`,
  `class Cliente(No)` — que reescrevem apenas os métodos cujo comportamento
  muda (ex.: tratamento de mensagens). O nó intermédio usa `No` diretamente.
- **Porquê:** Traduz diretamente o enunciado ("o servidor S1 é também o nó
  O1"); modelo familiar ao grupo (vindo de Java: `extends`, `super`,
  `@Override`); com três papéis o acoplamento extra não pesa.
- **Alternativas rejeitadas:** Composição (o papel *tem* um `No` e recebe
  eventos por callbacks) — mais desacoplado, mas mais código de ligação.
- **Nota:** escolha feita pelo Claude a pedido do grupo ("usa a melhor").

### D18 · Refresh periódico no bootstrapper + expiração de registos
- **Data:** 2026-10-27
- **Problema (detetado nos testes):** um nó bloqueado durante 9 s foi
  declarado morto pelo vizinho; quando recuperou, nenhum dos lados voltou a
  ligar-se e o overlay ficou partido. Além disso, um nó morto sem `LEAVE`
  ficava "ativo" para sempre no bootstrapper.
- **Decisão:**
  - **Refresh:** a cada **5 s**, cada nó volta a contactar o bootstrapper,
    renovando o seu registo e obtendo a lista atual dos seus vizinhos ativos;
    liga-se aos que lhe faltam. Para evitar ligações duplicadas, numa
    religação **só o nó com o nome "menor" inicia a ligação**.
  - **Expiração:** o bootstrapper guarda a hora do último contacto de cada nó
    e considera inativo quem não renova há mais de **15 s** (3 refreshes
    falhados).
- **Porquê:** recupera de qualquer falha temporária (nó bloqueado, link
  momentaneamente em baixo) sem intervenção manual, e mantém a visão global
  do bootstrapper correta. O refresh já é o sinal de vida, logo a expiração
  não acrescenta tráfego.
- **Diferença para os heartbeats (D11/D15):** os heartbeats são entre
  vizinhos (visão local, deteção rápida de falhas, 2 s/6 s); o refresh é de
  cada nó para o bootstrapper (visão global, religação, 5 s/15 s). O
  bootstrapper nunca inicia contactos — só responde.
- **Alternativas rejeitadas:**
  - Religar diretamente ao IP do vizinho perdido — os dois lados podem
    tentar em simultâneo e desiste ao fim de N tentativas.
  - Não religar — o overlay fica partido após falhas temporárias.
  - Vizinhos reportarem mortes ao bootstrapper — um link em baixo entre dois
    nós poderia marcar como morto um nó que está vivo.

---

## Detalhes de implementação da Etapa 1 (a rever pelo grupo)

Pequenas escolhas feitas ao escrever o código, dentro das decisões acima.
Confirmar ou alterar:

- Nomes das mensagens: `REGISTER`, `REGISTER_OK`, `REGISTER_ERR`, `HELLO`,
  `HEARTBEAT`, `LEAVE` (ver [PROTOCOLO.md](PROTOCOLO.md)).
- O bootstrapper recusa nomes que não existem na topologia (`REGISTER_ERR`).
- Pedidos ao bootstrapper usam ligações TCP curtas (pedido/resposta).
- O `HELLO` é trocado nos dois sentidos (teste full-duplex pedido na Etapa 1).
- Cada nó escuta no IP que lhe está atribuído no ficheiro de configuração
  (permite testar vários nós numa só máquina com 127.0.0.x).
- Ligação a um vizinho: 3 tentativas com 0,5 s de intervalo.
- Qualquer mensagem recebida conta como sinal de vida, não só `HEARTBEAT`.
- Se um vizinho volta a ligar-se com o mesmo nome, a ligação nova substitui
  a antiga.
- `SIGTERM` (ex.: quando o CORE pára o nó) é tratado como saída limpa.
- O refresh (D18) reutiliza a mensagem `REGISTER` em vez de criar uma nova:
  o mesmo pedido serve para entrar e para renovar, e se o bootstrapper
  reiniciar os nós voltam a registar-se sozinhos.
- Se apesar do desempate surgirem duas ligações para o mesmo par, os dois
  lados ficam com a ligação iniciada pelo nó de nome menor.
- O bootstrapper só regista no log os refreshes com `-v` (para não encher
  o ecrã de 5 em 5 s).

## Testes realizados (Etapa 1, local com 127.0.0.x — cenário 1)

| Teste | Resultado |
|---|---|
| Entrada de 4 nós por ordem O1→O4 | Cada nó liga-se apenas aos vizinhos já ativos; HELLO nos dois sentidos ✔ |
| Nó com nome inexistente (O9) | `REGISTER_ERR`, o nó termina com mensagem de erro ✔ |
| Saída limpa (SIGTERM em O3) | O2 remove O3 de imediato (`LEAVE`); bootstrapper atualizado ✔ |
| Morte abrupta (kill -9 em O4) | O2 deteta de imediato (ligação fechada); bootstrapper expira O4 ao fim de ~15–20 s ✔ |
| Nó bloqueado 9 s (SIGSTOP em O1) | O2 declara O1 morto após 6 s sem heartbeat; ao recuperar, O1 religa-se em ~1 s pelo refresh ✔ |
| Reentrada de O3 | Volta a ligar-se a O2 normalmente ✔ |

## Questões em aberto

- [ ] Identificação dos fluxos nos pacotes UDP

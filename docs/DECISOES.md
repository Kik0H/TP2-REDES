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

## Etapa 2 — Construção das rotas para os fluxos

### D22 · Estratégia de construção das rotas: anúncios periódicos dos servidores (Estratégia 1 do enunciado)
- **Data:** 2026-11-10
- **Decisão:** Cada servidor inunda periodicamente o overlay com anúncios dos
  seus fluxos. Cada nó reencaminha o anúncio aos vizinhos (inundação
  controlada, atualizando a métrica) e guarda as rotas na sua tabela, em
  estado **inativo**. Quando um cliente quer um fluxo, envia um pedido de
  ativação pelo caminho inverso (seguindo o campo Origem); cada nó no caminho
  acrescenta quem pediu aos seus Destinos e ativa a rota.
- **Porquê:**
  - Rotas alternativas "de graça": cada nó ouve o anúncio de vários vizinhos
    e pode guardá-los como reservas, permitindo reagir de imediato a falhas.
  - Métricas de rede (ex.: atraso) fáceis de medir: o anúncio pode levar um
    timestamp e cada nó mede à chegada.
  - Suporta naturalmente vários servidores e fluxos (cada servidor anuncia
    os seus).
  - Decisões locais em cada nó, sem entidade central; usa só as ligações entre
    vizinhos da Etapa 1.
  - É a estratégia descrita com mais pormenor no enunciado.
- **Custo assumido:** tráfego de controlo periódico (anúncios pequenos).
- **Alternativas rejeitadas:**
  - Estratégia 2a (cálculo central da rota) — exige que uma entidade conheça
    o overlay e as métricas em tempo real; o bootstrapper (D06) só conhece a
    topologia estática; reação a falhas lenta e ponto central de falha.
  - Estratégia 2b (pedido do cliente por inundação) — não guarda rotas
    alternativas e obriga a nova inundação a cada falha.
- **Por decidir:** métrica, período dos anúncios, controlo da inundação,
  formato das mensagens, comportamento em falhas (ver questões em aberto).

### D23 · Identificação dos fluxos: nome textual `servidor/vídeo`
- **Data:** 2026-11-10
- **Decisão:** Cada fluxo é identificado por `<servidor>/<vídeo>` (ex.:
  `S1/movie.Mjpeg`), em todas as mensagens de controlo e nos pacotes de
  vídeo. Nos pacotes UDP de vídeo o id vai num pequeno cabeçalho próprio
  antes do pacote RTP: 1 byte com o tamanho do id + o id em UTF-8.
- **Porquê:** Único sem coordenação entre servidores (os nomes dos nós já são
  únicos, D09); legível nos logs e no Wireshark; não altera o código RTP do
  exemplo (o pacote RTP vai intacto lá dentro). Custo ~15 bytes por pacote,
  desprezável face a frames de até ~14 KB.
- **Alternativas rejeitadas:**
  - Número no campo SSRC do RTP — sem bytes extra, mas ilegível e obriga a
    alterar o RTP do exemplo.
  - Número global definido por configuração — exige que todos os servidores
    partilhem o mesmo ficheiro.

### D24 · Métrica: atraso por link (RTT medido pelos heartbeats) + nº de saltos
- **Data:** 2026-11-10
- **Decisão:** O custo de uma rota é o **atraso acumulado** ao longo do
  caminho. Cada nó mede o atraso de cada link para os vizinhos através dos
  heartbeats (que passam a levar um timestamp devolvido pelo vizinho → RTT;
  atraso do link ≈ RTT/2, suavizado). Cada anúncio vai somando o atraso de
  cada link que atravessa. O **nº de saltos** é o critério secundário.
- **Porquê:** Cumpre o requisito de usar métricas da rede e reage a links
  lentos ou congestionados. Não depende de relógios sincronizados (o RTT é
  medido com o relógio do próprio nó), pelo que funcionaria numa rede real e
  não só no CORE. Aproveita os heartbeats já existentes (D15).
- **Custo assumido:** as medições oscilam → é preciso suavização (média
  móvel) e histerese na troca de rotas (decisão em aberto).
- **Alternativas rejeitadas:**
  - Só nº de saltos — simples e estável, mas ignora a qualidade dos links.
  - Atraso fim-a-fim com timestamp do servidor — simples no CORE (relógio
    partilhado), mas numa rede real exigiria relógios sincronizados (NTP).
- **Por decidir:** fórmula de suavização. (Tolerância de empate: D25.)

### D25 · Tolerância de empate no atraso: 10% (relativa)
- **Data:** 2026-11-10
- **Decisão:** Dois atrasos contam como **iguais** se a diferença for menor do
  que 10% do menor dos dois (ex.: 10 ms e 10,8 ms empatam; 10 ms e 12 ms
  não). Ordem de comparação entre duas rotas: (1) atraso, com esta
  tolerância; (2) nº de saltos; (3) regra de desempate final.
- **Porquê:** Atrasos medidos quase nunca são exatamente iguais (ruído de
  medição); sem tolerância, a rota trocaria por diferenças de décimas de
  milissegundo e o critério dos saltos nunca seria usado. Uma tolerância
  relativa adapta-se tanto a links rápidos como lentos.
- **Alternativas rejeitadas:** tolerância absoluta de 1 ms (demasiado grande
  para links muito rápidos, irrelevante para links lentos); sem tolerância
  (trocas por ruído).
- **Nota:** valor de 10% discutido com o professor, sugerido por ele e
  aprovado.

### D26 · Desempate final: manter a Origem atual, depois o nome do vizinho
- **Data:** 2026-11-10
- **Decisão:** Se, depois do atraso (com a tolerância da D25) e dos saltos,
  duas rotas continuarem empatadas: se uma delas é a **Origem atual**, fica
  essa; se não houver Origem atual (ex.: primeira ativação), ganha o
  **vizinho de nome alfabeticamente menor**.
- **Porquê:** Um empate nunca provoca uma troca de rota — só uma rota
  claramente melhor ou uma falha o fazem. Evita trocas inúteis (pedidos de
  ativação/desativação e possíveis cortes no vídeo) e o resultado é
  determinístico, logo reproduzível nos testes.
- **Exemplo:** com o C a receber pelo A (15,2 ms, 3 saltos) e um anúncio pelo
  B a 14,9 ms e 3 saltos (diferença de 2% < 10%), o C mantém-se no A.
- **Alternativas rejeitadas:**
  - Primeiro a chegar — não determinístico; a rota pode saltar a cada anúncio.
  - Só o nome menor — obrigaria a largar a Origem atual sem ganho (ex.: voltar
    ao A só porque "A" < "B").

### D27 · Período dos anúncios: 2 s
- **Data:** 2026-11-10
- **Decisão:** Cada servidor envia um anúncio a cada **2 s**.
- **Porquê:** Um nó ou cliente novo descobre os fluxos em ≤ 2 s e as mudanças
  de atraso refletem-se depressa; o tráfego é baixo (mensagens de ~200 bytes,
  face a ~280 KB/s de vídeo). Igual ao período dos heartbeats (D15). As
  falhas não dependem dos anúncios (são tratadas por heartbeats e reservas).
- **Alternativas rejeitadas:** 1 s (o dobro do tráfego sem ganho relevante);
  5 s (nós novos esperariam até 5 s e a adaptação a mudanças de atraso seria
  lenta).

### D28 · Um anúncio por servidor (lista todos os seus fluxos)
- **Data:** 2026-11-10
- **Decisão:** Cada anúncio identifica o servidor e lista todos os seus
  fluxos (ex.: `{servidor: S1, fluxos: [movie.Mjpeg, outro.Mjpeg], …}`). A
  tabela de rotas continua a ter uma entrada **por fluxo** (D23).
- **Porquê:** O caminho e o atraso até um servidor são os mesmos para todos
  os seus fluxos; agrupar reduz o nº de inundações (metade, com 2 vídeos por
  servidor).
- **Alternativas rejeitadas:** um anúncio por fluxo — mais inundações sem
  informação adicional.

### D29 · Controlo da inundação: nº de sequência + caminho, com reenvio quando melhora
- **Data:** 2026-11-10
- **Decisão:**
  - Cada anúncio leva um identificador `(arranque do servidor, nº de
    sequência)` e o **caminho** percorrido.
  - Um nó **descarta** o anúncio se o seu nome já estiver no caminho (volta a
    ele próprio → evita ciclos e reservas falsas).
  - Caso contrário, **regista** a entrada (fluxo pelo vizinho que enviou) e
    **reencaminha** se for a primeira cópia daquele número, **ou** se for uma
    cópia posterior do mesmo número com custo **melhor** (para além da
    tolerância da D25) do que o que já anunciou.
  - A hora de arranque no identificador evita que, quando um servidor
    reinicia e volta ao nº 1, os nós tomem os anúncios novos por antigos.
- **Porquê:** Um nº repetido não significa ciclo — significa que o anúncio
  chegou por outro caminho (ex.: num triângulo, o nó da direita recebe-o pelos
  dois extremos, e a cópia que chega primeiro pode não ser a de menor custo).
  Reencaminhar só a primeira cópia faria um nó anunciar aos vizinhos um custo
  pior do que o seu melhor. Esta opção garante: sem circulação infinita, sem
  reservas falsas e com os custos certos, com poucos reenvios extra.
- **Alternativas rejeitadas:**
  - Nº de sequência + caminho, só a 1.ª cópia — menos mensagens, mas os
    custos anunciados podem não ser os melhores.
  - Só caminho — nº de mensagens pode explodir em redes densas.
  - TTL — não evita duplicados nem reservas falsas.

### D30 · Expiração das rotas: 3 períodos sem anúncio (6 s)
- **Data:** 2026-11-10
- **Decisão:** Cada entrada da tabela ("fluxo F pelo vizinho V") guarda a hora
  do último anúncio recebido por V para esse servidor. Se passarem **6 s**
  (3 períodos) sem novo anúncio por V, a entrada é apagada. Se a ligação ao V
  cair (`LEAVE`, fecho do TCP ou heartbeats), todas as entradas por V são
  apagadas **de imediato**, sem esperar.
- **Porquê:** Apaga em ~6 s as rotas para servidores que desapareceram ou por
  vizinhos que perderam o caminho, tolerando a perda pontual de 1–2 anúncios.
  Mesma lógica dos heartbeats (3 falhados = morto).
- **Alternativas rejeitadas:** 5 períodos (10 s — mais lento a esquecer rotas
  mortas); só ao perder o vizinho (rotas "fantasma" para servidores que já
  não existem).
- **Por decidir:** o que acontece quando a entrada que expira é a Origem ativa
  (bloco das falhas).

## Cenários de teste

### D19 · Cenário 1: nó intermédio do lado dos clientes
- **Data:** 2026-11-03
- **Topologia underlay (CORE):** routers n4 e n3 ligados entre si; PCs n1
  (10.0.3.20) e n2 (10.0.4.20) no n4; PCs n5 (10.0.0.20), n6 (10.0.1.20) e
  n7 (novo, 10.0.5.20) no n3.
- **Mapeamento overlay → underlay:**

  | Nó overlay | Papel | PC | IP |
  |---|---|---|---|
  | O1 | Servidor S1 | n1 | 10.0.3.20 |
  | O2 | Nó intermédio | n7 → **router n3** (D20) | 10.0.5.20 → **10.0.2.1** |
  | O3 | Cliente C1 | n5 | 10.0.0.20 |
  | O4 | Cliente C2 | n6 | 10.0.1.20 |
  | — | Bootstrapper | n2 | 10.0.4.20 |

- **Ligações overlay:** O1–O2, O2–O3, O2–O4 (como na figura do enunciado).
- **Porquê o intermédio no n3:** a replicação tem de acontecer *depois* do
  troço partilhado pelos clientes. Com o intermédio no n3, o link n4–n3 leva
  uma só cópia do stream. Com o intermédio do lado do servidor (ex.: no n2),
  o O2 replicaria antes do troço comum e o link n4–n3 levaria duas cópias —
  tanto como sem overlay.
- **Bootstrapper no n2:** escolha do Claude (o grupo não tinha preferência);
  o n2 ficou livre e assim cumpre-se a D06 (processo dedicado).
- **Ideia para os testes (Etapa 4):** comparar com o intermédio do lado do
  servidor e medir no Wireshark o tráfego no link n4–n3.

### D20 · Cenário 1: nó intermédio no router n3 (substitui o PC n7 da D19)
- **Data:** 2026-11-03
- **Decisão:** O nó intermédio O2 corre no **router n3**, com o IP
  **10.0.2.1** (interface do link n4–n3). O PC n7 deixa de ser necessário.
- **Porquê:** Os docentes permitem correr o overlay em routers para
  simplificar. Com o intermédio no próprio router onde os caminhos para os
  clientes se separam, desaparece o desvio n3 → n7 → n3 da D19: o link n4–n3
  continua a levar uma só cópia e a duplicação acontece no ponto ótimo.
- **IP escolhido:** o n3 tem um IP por interface; qualquer um funciona (o OSPF
  encaminha para todos). Escolheu-se o 10.0.2.1, do link n4–n3, por estar no
  backbone e não depender do link de nenhum cliente.
- **Alternativas rejeitadas:** manter o intermédio no PC n7 — mais
  "realista" (aplicação só em sistemas terminais, como aconselha o
  enunciado), mas com o desvio pelo link n3–n7.
- **Para o relatório:** referir que numa rede real os routers não executam
  aplicações genéricas, e que a alternativa realista é a da D19.

### D21 · Religação pelo refresh numa thread própria (correção de bug)
- **Data:** 2026-11-03
- **Problema (detetado no teste 5 no CORE, link n3–n6 com 100% de perda):**
  a religação a um vizinho inacessível era feita na própria thread de
  refresh. Cada tentativa esperava até 3 s pelo timeout (3 tentativas +
  pausas ≈ 10,5 s), pelo que o refresh do O2 só se repetia ao fim de
  ~15,5 s — acima do limite de 15 s do bootstrapper. Resultado: o O2, que
  estava vivo, **expirou no bootstrapper** (12:41:44) e voltou 1 s depois.
  Um nó que se registasse nesse intervalo não receberia o O2 como vizinho.
- **Decisão:** o refresh lança cada religação numa thread própria e nunca
  fica bloqueado. A religação pelo refresh faz **uma só tentativa** (o
  refresh seguinte volta a tentar) e nunca há duas religações em simultâneo
  para o mesmo vizinho.
- **Alternativas rejeitadas:**
  - Só reduzir tentativas/timeout — o refresh continuaria a bloquear com
    vários vizinhos inacessíveis.
  - Aumentar o timeout do bootstrapper — escondia o problema e os nós mortos
    seriam esquecidos mais tarde.
- **Verificação:** teste local com um vizinho que o bootstrapper dá como
  ativo mas cujas ligações dão timeout: durante 40 s, o refresh do O2
  manteve-se a cada 5 s e o O2 nunca expirou.

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
- Ligação a um vizinho: 3 tentativas com 0,5 s de intervalo no arranque;
  1 tentativa nas religações feitas pelo refresh (D21).
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

## Testes realizados (Etapa 1, CORE — cenário 1, D19/D20)

| Teste | Resultado |
|---|---|
| Arranque: bootstrapper (n2) + O1 (n1), O2 (router n3), O3 (n5), O4 (n6) | Bootstrapper registou os 4 nós; O2 com vizinhos O1, O3, O4; restantes com vizinho O2 ✔ (2026-11-03) |
| 1 · Saída limpa (Ctrl+C no n5/O3) | O2 remove O3 de imediato (`LEAVE`) ✔ |
| 2 · Reentrada do O3 | O3 volta a ligar-se ao O2 ✔ |
| 3 · Nó bloqueado (Ctrl+Z no n6/O4, ~10 s, depois `fg`) | O2 deteta a falha após 6 s sem heartbeat; após o `fg`, religação automática pelo refresh ✔ |
| 4 · Morte abrupta (`kill -9` no O4) | O2 deteta de imediato (ligação fechada); bootstrapper expira o O4 após ~15 s ✔ |
| 6 · Nome inválido (O9) | Registo recusado (`REGISTER_ERR`), o nó termina com erro ✔ |
| 5 · Link n3–n6 com 100% de perda e depois reposto | O4 expirou no bootstrapper após 15 s ✔; com o link reposto, O4 voltou a registar-se e O2 religou-se automaticamente ✔; **bug:** O2 também expirou no bootstrapper sem ter falhado → corrigido (D21), repetir o teste |
| 5 (repetido após D21) | O2 deteta a perda do O4 por falta de heartbeat (6 s) ✔; tentativas de religação com 1 tentativa e sem bloquear o refresh ✔; O4 expira no bootstrapper (~17 s) e o O2 deixa de tentar ✔; **O2 não expirou** ✔; com o link reposto, O4 regista-se (12:50:56) e o O2 religa-se no refresh seguinte (12:50:59) ✔ |

## Questões em aberto

- [ ] **Pergunta do professor (2026-11-03) — falha de um nó com tráfego (Etapa 2):**
      quem assume o fluxo, como é escolhido, e o que acontece quando o nó
      original volta (assume de imediato ou espera pelo fim do fluxo?).
      Opções discutidas, ainda sem decisão:
      - *Quem assume:* rotas de reserva locais (uma entrada por vizinho que
        anunciou o fluxo; anúncios com o caminho percorrido para evitar
        ciclos) / esperar pelo próximo anúncio / reparação central.
      - *Regresso:* histerese (só volta se for claramente melhor e estável) /
        imediato / só no fim do fluxo — qualquer um com *make-before-break*
        (ativar a rota nova antes de largar a antiga; o buffer descarta
        duplicados pelo nº de sequência).
      - *Deteção:* só heartbeats (até 6 s) / também pela ausência de frames
        (~1 s).
      - Nota: o Cenário 1 não tem caminhos alternativos; para demonstrar é
        preciso o Cenário 2 ou 3.

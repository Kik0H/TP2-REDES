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
- **Complemento (D43):** diferenças menores do que 1 ms também contam como
  empate.

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

### D31 · Ativação explícita: mensagens ATIVAR / DESATIVAR
- **Data:** 2026-11-17
- **Decisão:** O cliente envia `ATIVAR` uma vez; a rota mantém-se ativa até
  chegar um `DESATIVAR` ou até o vizinho de baixo se perder (`LEAVE`, fecho do
  TCP ou heartbeats — mecanismos da Etapa 1), caso em que é retirado dos
  Destinos.
- **Porquê:** As mensagens vão por TCP (D02), logo não se perdem; a deteção de
  vizinhos perdidos já existe e trata os clientes que morrem sem avisar. Só há
  tráfego de controlo quando alguém entra ou sai.
- **Alternativas rejeitadas:** renovação periódica (soft state) — robusta a
  mensagens perdidas, problema que não temos, à custa de tráfego constante e
  mais temporizadores.

### D32 · O pedido de ativação pára no primeiro nó já ativo (por fluxo)
- **Data:** 2026-11-17
- **Decisão:** Quando um nó recebe `ATIVAR` para um fluxo que **já está a
  receber**, acrescenta quem pediu aos Destinos desse fluxo e não propaga o
  pedido. Se ainda não o recebe, acrescenta-o aos Destinos e envia `ATIVAR` à
  sua Origem para esse fluxo. A regra aplica-se **por fluxo**: um nó ativo
  para `S1/movie.Mjpeg` mas não para `S2/outro` propaga normalmente um pedido
  do segundo.
- **Porquê:** Garante que cada link transporta no máximo uma cópia de cada
  fluxo (a replicação acontece no nó onde os caminhos se separam) com o
  mínimo de mensagens.
- **Alternativas rejeitadas:** propagar sempre até ao servidor — o servidor
  saberia de todos os clientes, mas com mais mensagens e o risco de
  duplicar envios se algum nó não verificasse que já está ativo.

### D33 · Poda imediata dos ramos sem Destinos
- **Data:** 2026-11-17
- **Decisão:** Quando um nó que não é cliente fica sem Destinos para um fluxo
  (por `DESATIVAR` de um vizinho ou por perda do vizinho), envia logo
  `DESATIVAR` à sua Origem desse fluxo e a rota volta a inativa. A poda sobe
  em cadeia enquanto os nós de cima também ficarem sem Destinos, e pára no
  primeiro nó que ainda tenha outros Destinos.
- **Porquê:** Liberta a largura de banda de imediato; o vídeo só circula onde
  há quem o veja. A reativação também é rápida (o pedido sobe só até ao
  primeiro nó ativo, D32), pelo que não compensa esperar.
- **Alternativas rejeitadas:**
  - Tempo de graça de ~5 s — evita reativações se um cliente sair e voltar
    logo, mas desperdiça até 5 s de vídeo por ramo e exige mais um
    temporizador.
  - Não podar — ramos inúteis a receber vídeo para sempre.

### D34 · Pedidos para fluxos sem rota: o cliente só pede o que conhece; erro explícito
- **Data:** 2026-11-17
- **Decisão:**
  - O cliente, sendo um nó do overlay, recebe os anúncios; a sua tabela de
    rotas é a **lista de vídeos disponíveis**, que mostra ao utilizador. Só
    envia `ATIVAR` para fluxos dessa lista (um fluxo desconhecido é recusado
    localmente, sem mensagens na rede).
  - Um nó que receba `ATIVAR` para um fluxo sem rota (caso raro: a rota
    expirou nele mas ainda não nos nós de baixo) responde `ATIVAR_ERRO`. Quem
    o recebe retira o pedinte dos Destinos desse fluxo e passa o erro para
    baixo, até ao cliente, que mostra "fluxo indisponível de momento".
- **Porquê:** O cliente nunca fica à espera de um vídeo que não vem; o caso
  normal não gera tráfego inútil; o utilizador vê os vídeos disponíveis,
  que aparecem e desaparecem sozinhos com os anúncios.
- **Alternativas rejeitadas:**
  - Pedido pendente até chegar um anúncio — mais estado e temporizadores; o
    cliente só saberia do problema no fim do tempo limite.
  - Ignorar — o cliente ficaria à espera para sempre.

### D35 · Falha da Origem ativa: passa à melhor reserva local; sem reserva, espera pelo anúncio
- **Data:** 2026-11-17 · *(resposta à pergunta do professor de 2026-11-03)*
- **Decisão:** Quando a Origem ativa de um fluxo cai (vizinho perdido) ou
  falha (D36), o nó **imediatamente abaixo** — o que recebia dela — apaga as
  entradas por essa Origem e escolhe a **melhor reserva** da sua tabela para
  esse fluxo (critérios D24–D26), enviando-lhe `ATIVAR`; se essa reserva não
  estiver ativa, propaga o pedido para cima (D32). Os nós mais abaixo não
  fazem nada (continuam ligados ao mesmo nó e não notam a troca). Se não houver
  reserva, a rota fica inativa (mantendo os Destinos) até um anúncio trazer um
  caminho novo, sendo então reativada.
- **Porquê:** As reservas já existem na tabela graças aos anúncios (D22, D30);
  a reparação é imediata, local e sem entidade central. Ninguém precisa de
  "nomear" um substituto.
- **Alternativas rejeitadas:**
  - Só esperar pelo próximo anúncio — mais lento (até mais 2 s) e desperdiça
    as reservas.
  - Reparação central — lenta e cria um ponto central de falha.

### D36 · Deteção da falha: mecanismos da Etapa 1 + ~1 s sem vídeo
- **Data:** 2026-11-17
- **Decisão:** Para além de `LEAVE`, fecho do TCP e heartbeats (até 6 s),
  qualquer nó com uma rota **ativa** vigia o fluxo: se não chegar nenhum
  pacote desse fluxo pela Origem durante **~1 s** (20 frames a 20 fps),
  considera a Origem falhada **para esse fluxo** e aplica a D35.
- **Porquê:** Reduz o vídeo parado de até 6 s para ~1 s quando um link cai, e
  apanha também vizinhos vivos que deixaram de reencaminhar (ex.: perderam a
  sua própria Origem).
- **Requisito para a Etapa 4:** o servidor tem de enviar continuamente (ex.:
  repetir o vídeo em ciclo); caso contrário, o fim do vídeo seria confundido
  com uma falha.
- **Alternativas rejeitadas:** só os mecanismos atuais — até 6 s de vídeo
  parado.

### D37 · Regresso do nó original: troca só se for >10% melhor durante 3 anúncios seguidos
- **Data:** 2026-11-17 · *(resposta à pergunta do professor de 2026-11-03)*
- **Decisão:** Quando um nó volta e os anúncios voltam a passar por ele, **não
  "reclama" nada**: é o nó de baixo que decide. Este só troca a sua Origem
  atual por outra se a nova rota for **mais de 10% melhor** (D25) em **3
  anúncios seguidos** (~6 s, D27). Aplica-se a qualquer troca por uma rota
  melhor, não só ao regresso de um nó.
- **Porquê:** Usa a melhor rota passado pouco tempo, mas um nó instável
  (que liga e desliga) não provoca trocas sucessivas (histerese).
- **Alternativas rejeitadas:**
  - Só a margem de 10% — trocaria no primeiro anúncio melhor; um nó instável
    provocaria uma troca de cada vez que volta.
  - Nunca voltar sozinho / só no fim do fluxo — um fluxo ao vivo pode nunca
    acabar; ficar-se-ia indefinidamente na rota pior.
- **Por decidir:** como se faz a troca em si (make-before-break ou não).

### D38 · Troca de Origem por make-before-break
- **Data:** 2026-11-17 · *(completa a resposta à pergunta do professor)*
- **Decisão:** Numa troca por uma rota melhor (D37), o nó: (1) envia `ATIVAR`
  à nova Origem, mantendo a antiga; (2) enquanto recebe pelos dois caminhos,
  reencaminha **cada frame uma só vez**, descartando os repetidos pelo nº de
  sequência do RTP (cada nó guarda, por fluxo, o último nº reencaminhado);
  (3) quando chega o primeiro frame pela nova Origem, envia `DESATIVAR` à
  antiga (que é podada como na D33). Numa falha (D35) não se aplica: a Origem
  antiga já não envia nada.
- **Porquê:** A troca não corta o vídeo — o cliente recebe todos os frames.
  Custo: um contador por fluxo em cada nó e uns instantes de tráfego
  duplicado no troço que muda.
- **Alternativas rejeitadas:** break-before-make — mais simples, mas perde os
  frames enviados enquanto a nova rota ainda não começou a transmitir.

### D39 · Suavização do atraso: média móvel exponencial com α = 0,25
- **Data:** 2026-11-17
- **Decisão:** O atraso de cada link é atualizado a cada medição de RTT (uma
  por heartbeat, a cada 2 s) por
  `atraso = 0,75 × atraso_anterior + 0,25 × nova_medição` (atraso ≈ RTT/2).
- **Porquê:** Filtra o ruído das medições mas reflete uma mudança real em
  ~8 s; somado à estabilidade de 3 anúncios da D37 (~6 s), uma degradação real
  de um link leva a troca de rota em ~8–14 s. É a técnica usada pelo TCP para
  o RTT (SRTT).
- **Alternativas rejeitadas:**
  - α = 0,125 (valor do TCP) — pensado para medições por pacote; com uma
    medição a cada 2 s demoraria ~16 s a reagir.
  - α = 0,5 — reage em ~4 s, mas oscila mais e, com a margem de 10%, poderia
    trocar de rota por ruído.
  - Média das últimas 5 medições — comportamento semelhante (~10 s), com mais
    estado.

### D40 · Tabela de rotas: uma entrada por fluxo, com as candidatas por vizinho
- **Data:** 2026-11-17
- **Decisão:** Cada nó guarda, por fluxo (D23):
  `estado` (ativa/inativa), `origem` (vizinho de onde recebe), `destinos`
  (vizinhos a quem reenvia), `sou_cliente`, `ultimo_seq` (último frame
  reencaminhado, D38), `ultimo_pacote` (hora, D36) e `candidatas` — uma por
  vizinho que anunciou o fluxo, com `atraso`, `saltos`, `caminho`,
  `ultimo_anuncio` (D30) e `melhor_seguidos` (contador da D37). Por servidor,
  guarda o identificador do último anúncio visto e o custo já anunciado
  (D29).
- **Porquê:** Corresponde diretamente ao enunciado ("uma rota por fluxo:
  Fluxo, Custo, Origem, Destinos, Estado"), com as reservas agrupadas no
  fluxo a que pertencem; Destinos e Origem guardados uma só vez.
- **Alternativas rejeitadas:** tabela plana com uma linha por (fluxo, vizinho)
  — mais simples de imprimir, mas Destinos repetidos e menos fiel ao
  enunciado.

### D41 · Inspeção da tabela durante os testes: comandos no terminal do nó
- **Data:** 2026-11-17
- **Decisão:** No terminal onde corre o nó, `t` + Enter imprime a tabela de
  rotas atual e `v` + Enter imprime os vizinhos com o atraso medido de cada
  link. Mantêm-se os logs de cada mudança (ex.: "Origem do F passou de B para
  A").
- **Porquê:** Permite ver o estado completo no momento exato (testes no CORE,
  Etapa 3 e capturas para o relatório) sem encher o ecrã.
- **Alternativas rejeitadas:** impressão periódica (enche o ecrã); só logs
  (não mostra o estado completo).

### D42 · Dados de teste já na Etapa 2
- **Data:** 2026-11-17
- **Decisão:** Na Etapa 2 o servidor já envia, a 20 pacotes/s por fluxo ativo,
  pacotes de dados **numerados** com o formato da D23 (id do fluxo + cabeçalho
  RTP + conteúdo de teste), que os nós reencaminham pelas rotas (UDP 6001,
  D12). O cliente conta recebidos, perdidos e duplicados. Na Etapa 4 o
  conteúdo de teste é substituído pelos frames do vídeo.
- **Porquê:** Permite implementar e testar já a D36 (deteção por falta de
  vídeo) e a D38 (descarte de duplicados), e demonstrar no CORE as falhas e a
  troca sem corte. A Etapa 4 fica reduzida ao vídeo real (ler o ficheiro e
  mostrar a janela).
- **Alternativas rejeitadas:** só o plano de controlo agora — a D36, a D38 e o
  reencaminhamento ficariam todos para a Etapa 4, sem forma de testar as
  falhas na Etapa 2.

### D43 · Empate no atraso: diferença < 10% **ou** < 1 ms (complementa a D25)
- **Data:** 2026-11-17
- **Decisão:** Dois atrasos empatam se a diferença for menor do que 10% do
  menor (D25) **ou** menor do que 1 ms.
- **Porquê:** Com links sem atraso configurado (testes locais ou CORE por
  omissão) o atraso medido é ~0,1 ms, quase só tempo de processamento; uma
  variação de 0,02 ms já excederia os 10% e as rotas mudariam por ruído. O
  mínimo absoluto faz com que, nesses casos, decidam os saltos; em links reais
  (vários ms) a regra dos 10% acordada com o professor mantém-se.
- **Alternativas rejeitadas:** só os 10% (obrigaria a configurar atrasos em
  todos os testes para evitar trocas por ruído).

### D44 · `ATIVAR_ERRO`: tentar as reservas antes de passar o erro (complementa a D34)
- **Data:** 2026-11-17
- **Decisão:** Quem recebe `ATIVAR_ERRO` da sua Origem apaga essa candidata
  para o fluxo e tenta a melhor reserva (como na D35). Só se não houver
  nenhuma é que passa o `ATIVAR_ERRO` aos seus Destinos (que os retiram e
  fazem o mesmo), até ao cliente.
- **Porquê:** Se existir outro caminho, o cliente recebe o vídeo em vez de um
  erro; mantém uma só regra para "a minha Origem não serve" (D35).
- **Alternativas rejeitadas:** passar o erro logo (D34 à letra) — o cliente
  receberia um erro mesmo havendo caminho alternativo.

### D45 · Vizinho que deixou de enviar vídeo: candidata suspensa ~10 s
- **Data:** 2026-11-17
- **Decisão:** Quando a D36 dispara (~1 s sem vídeo pela Origem), além de
  passar à reserva, o nó marca a candidata por essa Origem como **suspensa
  durante 10 s para esse fluxo**: continua a ser atualizada pelos anúncios,
  mas não pode ser escolhida (nem pela D35 nem pela D37) até a suspensão
  acabar.
- **Porquê:** Um vizinho pode estar vivo e a anunciar a melhor rota sem
  reencaminhar o vídeo (falha só no plano de dados: thread de UDP com erro,
  firewall no porto 6001, ou perda tão alta que o TCP ainda passa mas o UDP
  não). Sem suspensão, a D37 voltaria a ele após 3 anúncios e haveria um corte
  de ~1 s a cada ~7 s. Com suspensão, um problema temporário recupera em
  ~16 s e um persistente custa uma tentativa a cada ~17 s.
- **Alternativas rejeitadas:** sem suspensão (ciclo de cortes); só voltar a
  contar depois de a ligação ao vizinho cair e voltar (um problema temporário
  nunca seria recuperado).

### D46 · Pacotes de dados levam também o nome do vizinho que os enviou (complementa a D23)
- **Data:** 2026-11-17
- **Decisão:** O cabeçalho próprio dos pacotes de dados passa a ser:
  1 byte com o tamanho do id do fluxo + id do fluxo (D23) + 1 byte com o
  tamanho do nome do remetente + nome do remetente + pacote RTP. Cada nó, ao
  reencaminhar, escreve o seu próprio nome como remetente.
- **Porquê:** Na troca sem corte (D38), o nó tem de saber quando chega o
  primeiro pacote **pela nova Origem**; o IP de origem do UDP não serve, porque
  um nó com várias interfaces (ex.: um router) pode enviar por um IP diferente
  do configurado (mesmo problema da D09). Também permite à D36 contar só os
  pacotes vindos da Origem e ajuda nos logs/Wireshark. Custo: ~3 bytes.
- **Alternativas rejeitadas:**
  - Temporizador fixo (~0,5 s) — se a nova rota demorasse mais, haveria corte.
  - Confirmação `ATIVAR_OK` — mais uma mensagem, sem garantir que os pacotes
    já estão a chegar.

### D47 · Cascata na D36: sem reserva, o nó mantém a Origem
- **Data:** 2026-11-17
- **Problema (detetado nos testes locais):** quando um nó deixa de enviar
  dados, **todos** os nós abaixo dele ficam sem dados ao mesmo tempo e a D36
  disparava em todos ao mesmo tempo. Os clientes (sem reserva) suspendiam e
  desativavam a sua Origem, que ficava sem Destinos e podava a árvore inteira;
  num teste, um cliente perdeu ~24 s de vídeo (486 pacotes).
- **Decisão:** quando a D36 dispara num nó **sem nenhuma reserva**, o nó não
  faz nada: mantém a Origem, não suspende nem desativa (só regista no log).
  Só muda quem tem alternativa.
- **Porquê:** Só o nó imediatamente abaixo da falha deve reagir; os de baixo
  recebem o vídeo de volta assim que ele muda. Resolve o caso mais comum
  (clientes e nós com um só caminho) sem tornar a deteção mais lenta. No mesmo
  teste, a perda passou para 23 pacotes (~1,1 s, o tempo de deteção).
- **Alternativas rejeitadas:**
  - Tempo da D36 escalonado pela distância ao servidor — deteção mais lenta
    para nós longe do servidor.
  - Aviso `RECUPERAR` aos Destinos — mensagem nova e corrida com os
    temporizadores dos nós de baixo.
  - (O escalonamento pode juntar-se mais tarde se houver trocas
    desnecessárias em topologias com muitas reservas.)

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

## Detalhes de implementação da Etapa 2 (a rever pelo grupo)

Pequenas escolhas feitas ao escrever o código, dentro das decisões acima.
Confirmar ou alterar:

**Correção necessária encontrada nos testes**
- O `HELLO` passou a levar o **IP configurado** do nó (`ip`). Antes, o IP de
  um vizinho que se ligava a nós era o IP de origem da ligação TCP, que pode
  não ser o IP onde ele está à escuta (localmente é sempre 127.0.0.1; no CORE
  um router pode ligar por outra interface — o problema da D09). Os pacotes de
  dados iam para um IP sem ninguém à escuta.

**Medição do atraso (D24, D39)**
- `HEARTBEAT {t, eco, espera}`: RTT = agora − eco − espera, só com o relógio
  de quem mede. Ao ligar a um vizinho envia-se logo um heartbeat, e quem
  recebe um heartbeat sem eco responde de imediato — o atraso fica medido em
  milissegundos, sem esperar 2–4 s.
- A primeira estimativa de quem inicia a ligação é o RTT do `HELLO`.
- Enquanto um link não foi medido, usa-se 1 ms.

**Anúncios (D29)**
- Um anúncio não é reencaminhado para vizinhos que já estão no caminho
  (seriam descartados por eles).

**Escolha da Origem e ciclos**
- A Origem nunca é escolhida entre os Destinos do próprio fluxo.
- Um `ATIVAR` vindo da própria Origem (ou da nova Origem numa troca) é
  recusado com `ATIVAR_ERRO` ("pedido em ciclo").
- O contador da D37 conta anúncios recebidos de cada candidata e volta a 0
  sempre que ela não é melhor ou que a Origem muda.

**Plano de dados (D36, D38, D42)**
- Ao ativar uma rota, o temporizador da D36 começa nesse momento (dá 1 s até
  ao primeiro pacote).
- Só são aceites pacotes da Origem ou da nova Origem numa troca; os de outros
  remetentes são ignorados.
- O último nº de sequência mantém-se numa falha (evita reenviar repetidos se
  a nova Origem vier atrasada); se passar mais de 1 s sem pacotes aceita-se
  qualquer número (ex.: servidor reiniciado).
- Troca sem corte (D38) em que a nova Origem não envia nada em 1 s: é
  cancelada, a candidata recebe `DESATIVAR` e fica suspensa 10 s (como na
  D45). O vídeo continua pela Origem atual, sem corte.
- Fluxo que deixa de ter candidatas (ninguém o anuncia, ex.: servidor saiu):
  é apagado da tabela; se estava ativo, os Destinos recebem `ATIVAR_ERRO` e o
  cliente local é avisado.

**Servidor e cliente**
- O servidor só envia dados de um fluxo enquanto ele tem Destinos; o nº de
  sequência continua a contar mesmo sem Destinos. Conteúdo de teste de
  1000 bytes; timestamp RTP em ms; tipo RTP 26 (MJPEG).
- O servidor é um nó normal: também reencaminha anúncios e dados de outros
  servidores.
- Cliente: comandos `l` (lista), `ver <fluxo>`, `parar <fluxo>`,
  `e` (estatísticas); opção `--ver <fluxo>` no arranque (espera até 15 s que
  o fluxo apareça); resumo das estatísticas a cada 5 s.
- Comando de teste `x` em qualquer nó: simula uma falha **só nos dados** (o nó
  continua vivo e a anunciar, mas não reencaminha) — serve para demonstrar a
  D36/D45/D47.
- A manutenção (expirações, D36, trocas pendentes) corre a cada 0,2 s.

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

## Testes realizados (Etapa 2, local com 127.0.0.x)

Topologia `topologias/etapa2-local.json`: servidores S1 (`movie.Mjpeg`,
`video2.Mjpeg`) e S2 (`outro.Mjpeg`, `clip.Mjpeg`); o C chega ao S1 pelo A
(3 saltos) ou pelo B (4 saltos, via Y); clientes D e E ligados ao C.

| Teste | Resultado |
|---|---|
| T1 · Anúncios dos 2 servidores | Todos os nós conhecem os 4 fluxos; o C guarda as duas candidatas para os fluxos do S1 (pelo A e pelo B) com atraso, saltos e caminho ✔ |
| T2 · 2.º cliente pede o mesmo fluxo | O pedido do E pára no C (já ativo); o C replica para D e E ✔ (D32) |
| T3 · Fluxo do 2.º servidor + fluxo inexistente | `S2/outro` ativado pelo B ✔; `S9/nada` recusado localmente com a lista dos disponíveis, sem mensagens na rede ✔ (D34) |
| T4 · `kill -9` na Origem (A) | O C passa de imediato à reserva B; **0 pacotes perdidos** nos clientes ✔ (D35) |
| T5 · A volta (melhor em saltos) | Ao 3.º anúncio, troca sem corte para o A: **0 perdidos**, 1 duplicado descartado ✔ (D37, D38) |
| T6 · Falha só nos dados no A (comando `x`) | O C deteta em ~1 s e passa ao B, A suspenso 10 s ✔ (D36, D45); D e E sem reserva mantêm a Origem ✔ (D47): **23 pacotes perdidos** (~1,1 s). Na nova tentativa com o A ainda avariado, a troca é cancelada **sem corte** ✔; com o A reparado, troca sem corte ✔ |
| T6 (antes da D47) | Cascata: os clientes desativaram o C e perderam 486 pacotes (~24 s) ✘ → corrigido pela D47 |
| T7 · Poda | D e E deixam de ver: o C, o A e o X podam em cadeia até ao servidor ✔ (D33) |
| T8 · Servidor S2 sai | Os fluxos do S2 desaparecem das tabelas; o cliente que via `S2/outro` recebe `ATIVAR_ERRO` e é avisado ✔ |

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

- [ ] **Testar a Etapa 2 no CORE**: topologia com caminhos alternativos
      (Cenário 2) e um 2.º servidor com 2 fluxos; testes com atrasos e perdas
      configurados nos links (para ver as rotas seguirem o menor atraso).
- [ ] Rever os "Detalhes de implementação da Etapa 2".

- [x] **Pergunta do professor (2026-11-03) — falha de um nó com tráfego:**
      respondida pelas D35 (quem assume), D36 (deteção), D37 (regresso) e
      D38 (troca sem corte). Para demonstrar é preciso uma topologia com
      caminhos alternativos (Cenário 2 ou 3).

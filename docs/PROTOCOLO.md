# Protocolo do overlay

Especificação das mensagens de controlo e dos pacotes de dados (secção
"Especificação do(s) protocolo(s)" do relatório). Ver decisões em
[DECISOES.md](DECISOES.md).

## Transporte e formato

- **TCP** (D02). Bootstrapper na porta **5000**, nós na porta **6000** (D12).
- Cada mensagem é um **objeto JSON em UTF-8 terminado por `\n`** (D07).
- O campo `type` identifica a mensagem. Os nós são identificados pelo
  **nome lógico** (D09), não pelo IP.

## Mensagens — Etapa 1 (topologia overlay)

| `type` | Sentido | Campos | Significado |
|---|---|---|---|
| `REGISTER` | nó → bootstrapper | `nome` | Entrada no overlay; repetido a cada 5 s como refresh (D18) |
| `REGISTER_OK` | bootstrapper → nó | `nome`, `ip`, `vizinhos: [{nome, ip}]` | IP do próprio nó e vizinhos configurados **já ativos** (D13) |
| `REGISTER_ERR` | bootstrapper → nó | `motivo` | Nome não existe na topologia |
| `HELLO` | nó ↔ nó | `nome`, `ip` | Primeira mensagem numa ligação nova, nos dois sentidos. `ip` = IP configurado onde o nó está à escuta (TCP 6000, UDP 6001) |
| `HEARTBEAT` | nó ↔ nó | `nome`, `t`, `eco`, `espera` | Sinal de vida a cada 2 s (D15) e medição do RTT do link (D24) |
| `LEAVE` | nó → vizinhos e bootstrapper | `nome` | Saída limpa (D11) |

Exemplos:

```json
{"type": "REGISTER", "nome": "O3"}
{"type": "REGISTER_OK", "nome": "O3", "ip": "10.0.2.10", "vizinhos": [{"nome": "O2", "ip": "10.0.1.10"}]}
{"type": "HELLO", "nome": "O3", "ip": "10.0.0.20"}
```

## Mensagens — Etapa 2 (rotas)

| `type` | Sentido | Campos | Significado |
|---|---|---|---|
| `ANUNCIO` | servidor → todos (inundação) | `servidor`, `arranque`, `seq`, `fluxos`, `custo`, `saltos`, `caminho` | "Tenho estes fluxos". `custo` = atraso acumulado (ms) até ao servidor, `caminho` = nós por onde passou (D22, D24, D28, D29) |
| `ATIVAR` | nó → a sua Origem | `fluxo` | Quero receber o fluxo (D31); sobe até ao 1.º nó já ativo (D32) |
| `DESATIVAR` | nó → a sua Origem | `fluxo` | Deixei de precisar do fluxo (D31, poda D33, troca D38, suspensão D45) |
| `ATIVAR_ERRO` | nó → quem pediu | `fluxo`, `motivo` | Não tenho rota para esse fluxo (D34, D44) |

Exemplos:

```json
{"type": "ANUNCIO", "servidor": "S1", "arranque": 1794830400000, "seq": 42,
 "fluxos": ["movie.Mjpeg", "video2.Mjpeg"], "custo": 12.4, "saltos": 2, "caminho": ["S1", "X"]}
{"type": "ATIVAR", "fluxo": "S1/movie.Mjpeg"}
{"type": "ATIVAR_ERRO", "fluxo": "S1/movie.Mjpeg", "motivo": "sem rota para o fluxo"}
{"type": "HEARTBEAT", "nome": "C", "t": 81234.5, "eco": 90112.8, "espera": 3.1}
```

### Medição do atraso de cada link (D24, D39)

```
 A                                   B
  |--- HEARTBEAT {t: tA} ------------>|   B guarda tA e a hora a que o recebeu
  |<-- HEARTBEAT {t: tB, eco: tA, ----|
  |              espera: w}           |   A: RTT = agora − tA − w   (só o relógio de A)
  atraso(A,B) = 0,75·atraso + 0,25·(RTT/2)
```

### Anúncio e inundação (D22, D29, D30)

O servidor envia o anúncio a todos os vizinhos a cada 2 s. Cada nó que o recebe:

1. **descarta-o** se o seu nome já estiver no `caminho` (ciclo);
2. **ignora-o** se o identificador `(arranque, seq)` for mais antigo do que o último visto desse servidor;
3. **regista** em cada fluxo uma candidata "pelo vizinho que enviou", com
   `atraso = custo + atraso do link` e `saltos + 1`;
4. **reencaminha-o** a todos os vizinhos (menos a quem lho enviou e aos que já estão no caminho) se for a 1.ª cópia deste nº **ou** uma cópia com custo melhor do que a que já reencaminhou, atualizando `custo`, `saltos` e `caminho`.

Uma candidata sem novo anúncio há 6 s é apagada (D30).

### Comparação de rotas (D24–D26, D43)

1. atraso — ganha o menor; diferenças < 10% **ou** < 1 ms contam como empate;
2. nº de saltos — ganha o menor;
3. desempate — mantém a Origem atual; sem Origem, ganha o vizinho de nome menor.

### Ativação, propagação e poda (D31–D33)

```
 cliente D          C (inativo)        A (inativo)        X (ativo)
  |--- ATIVAR F ---->|                  |                  |
  |                  |--- ATIVAR F ---->|                  |
  |                  |                  |--- ATIVAR F ---->|  X já recebe F:
  |                  |                  |                  |  junta A aos Destinos e pára
  |<====== dados F ==|<====== dados F ==|<====== dados F ==|
```

Quando um nó (não cliente) fica sem Destinos, envia `DESATIVAR` à sua Origem, e a poda sobe enquanto os nós de cima também ficarem vazios.

### Falhas (D35–D38, D45, D47)

| Situação | Deteção | Reação |
|---|---|---|
| Origem perdida (`LEAVE`, TCP fechado, heartbeats) | imediata a 6 s | passa à melhor reserva (`ATIVAR`); sem reserva, espera por um anúncio |
| Rota da Origem expirou | 6 s sem anúncio | idem |
| Origem viva mas sem dados | ~1 s sem pacotes dela | com reserva: passa à reserva, a Origem recebe `DESATIVAR` e fica suspensa 10 s; **sem reserva: mantém a Origem** (D47) |
| `ATIVAR_ERRO` da Origem | — | tenta reservas; sem reserva, passa o erro aos Destinos |
| Rota melhor (>10% ou menos saltos) | 3 anúncios seguidos | troca sem corte: `ATIVAR` à nova, recebe pelas duas (descarta repetidos), `DESATIVAR` à antiga quando chega o 1.º pacote pela nova |

## Pacotes de dados (D23, D42, D46)

UDP, porta 6001. Cada nó, ao reencaminhar, escreve o seu nome como remetente.

```
+------+----------------------+------+--------------+---------------------------+
| 1 B  | id do fluxo (UTF-8)  | 1 B  | remetente    | RTP: cabeçalho 12 B +     |
| tam. | ex.: S1/movie.Mjpeg  | tam. | ex.: C       | conteúdo                  |
+------+----------------------+------+--------------+---------------------------+
```

- RTP versão 2, tipo 26 (MJPEG), nº de sequência de 16 bits, timestamp em ms.
- Na Etapa 2 o conteúdo é de teste (1000 bytes, 20 pacotes/s por fluxo); na Etapa 4 passa a ser um frame do vídeo.
- Um nó só aceita pacotes da sua Origem (ou da nova Origem numa troca), descarta os repetidos pelo nº de sequência e envia uma cópia a cada Destino.

## Interações — Etapa 1

### Entrada de um nó

```
 O3                     Bootstrapper                  O2 (já ativo)
  |--- REGISTER {O3} -------->|                          |
  |<-- REGISTER_OK {O2} ------|                          |
  |  (abre escuta TCP 6000)                              |
  |------------------- ligação TCP --------------------->|
  |------------------- HELLO {O3} ---------------------->|
  |<------------------ HELLO {O2} -----------------------|
  |<========= HEARTBEAT a cada 2 s, nos dois sentidos ===>|
```

- Pedidos ao bootstrapper usam uma ligação TCP curta (um pedido, uma
  resposta, fecha).
- Ligações entre vizinhos são persistentes durante toda a execução.
- Quem inicia a ligação é sempre o nó que chegou por último (D13).

### Saída

- **Limpa** (Ctrl+C ou SIGTERM): `LEAVE` a cada vizinho e ao bootstrapper.
- **Abrupta**: se o processo morre, o sistema operativo fecha o socket e o
  vizinho deteta-o de imediato. Se o nó fica sem resposta (link em baixo,
  processo bloqueado), o vizinho deteta-o ao fim de **6 s sem qualquer
  mensagem** (D15). Qualquer mensagem recebida, não só `HEARTBEAT`, conta
  como sinal de vida.

### Refresh e religação (D18)

```
 O1                     Bootstrapper                  O2
  |--- REGISTER {O1} -------->|   (a cada 5 s: renova o registo de O1)
  |<-- REGISTER_OK {O2} ------|
  |  O2 ativo mas não ligado e "O1" < "O2"  ->  O1 religa:
  |------------------- ligação TCP + HELLO ------------->|
```

- O `REGISTER` é idempotente: o mesmo pedido serve para entrar e para
  renovar. Se o bootstrapper reiniciar, os nós voltam a registar-se sozinhos
  no refresh seguinte.
- O bootstrapper esquece nós sem refresh há mais de **15 s**.
- Numa religação só o nó de nome menor inicia a ligação. Se ainda assim
  surgirem duas ligações para o mesmo par, os dois lados ficam com a iniciada
  pelo nó de nome menor.

### Heartbeats vs. refresh

| | Heartbeats | Refresh |
|---|---|---|
| Quem → quem | nó → cada vizinho (nos dois sentidos) | nó → bootstrapper |
| Por onde | ligação TCP persistente entre vizinhos | ligação TCP curta |
| Período / limite | 2 s / 6 s | 5 s / 15 s |
| Serve para | o vizinho detetar falhas depressa (visão local) | visão global do bootstrapper e religação |

## Como executar

Na pasta `src/`:

```bash
python3 bootstrapper.py ../topologias/<topologia>.json [ip_escuta] [-v]
python3 servidor.py <nome> <ip_bootstrapper> <video> [<video> ...]   # servidor
python3 ott.py      <nome> <ip_bootstrapper>                          # nó intermédio
python3 cliente.py  <nome> <ip_bootstrapper> [--ver <fluxo>]          # cliente
```

Comandos no terminal de cada nó (D41):

| Comando | Onde | Faz |
|---|---|---|
| `t` | todos | tabela de rotas (fluxos, Origem, Destinos, candidatas, estatísticas de dados) |
| `v` | todos | vizinhos e atraso medido de cada link |
| `x` | todos | liga/desliga uma falha simulada **só nos dados** (para testar a D36/D45/D47) |
| `h` | todos | ajuda |
| `l` | cliente | fluxos disponíveis |
| `ver <fluxo>` / `parar <fluxo>` | cliente | começar / deixar de ver |
| `e` | cliente | estatísticas: recebidos, perdidos, duplicados |

Teste local da Etapa 2 (Linux; um terminal por nó):

```bash
python3 bootstrapper.py ../topologias/etapa2-local.json 127.0.0.1
python3 servidor.py S1 127.0.0.1 movie.Mjpeg video2.Mjpeg
python3 servidor.py S2 127.0.0.1 outro.Mjpeg clip.Mjpeg
python3 ott.py X 127.0.0.1      # e o mesmo para Y, A, B, C
python3 cliente.py D 127.0.0.1 --ver S1/movie.Mjpeg
python3 cliente.py E 127.0.0.1
```

No macOS, criar antes os endereços: `for i in $(seq 21 29); do sudo ifconfig lo0 alias 127.0.0.$i up; done`.

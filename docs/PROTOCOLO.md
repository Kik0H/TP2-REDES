# Protocolo de controlo do overlay

Especificação das mensagens de controlo (secção "Especificação do(s)
protocolo(s)" do relatório). Ver decisões em [DECISOES.md](DECISOES.md).

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
| `HELLO` | nó ↔ nó | `nome` | Primeira mensagem numa ligação nova, nos dois sentidos |
| `HEARTBEAT` | nó ↔ nó | `nome` | Sinal de vida, a cada 2 s (D15) |
| `LEAVE` | nó → vizinhos e bootstrapper | `nome` | Saída limpa (D11) |

Exemplos:

```json
{"type": "REGISTER", "nome": "O3"}
{"type": "REGISTER_OK", "nome": "O3", "ip": "10.0.2.10", "vizinhos": [{"nome": "O2", "ip": "10.0.1.10"}]}
{"type": "HELLO", "nome": "O3"}
```

## Interações

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

## Como executar (Etapa 1)

```bash
cd src
python3 bootstrapper.py ../topologias/<topologia>.json [ip_escuta] [-v]
python3 ott.py <nome> <ip_bootstrapper> [-v]
```

Teste local sem CORE (Linux; cada nó usa um IP 127.0.0.x diferente):

```bash
python3 bootstrapper.py ../topologias/cenario1-local.json 127.0.0.1
python3 ott.py O1 127.0.0.1     # num terminal por nó
python3 ott.py O2 127.0.0.1
...
```

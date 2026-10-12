# Registro de alterações

## 2026-10-11 — .gitignore e requirements.txt
- Por quê: a publicação de E1 deve excluir logs, caches e documentos locais e declarar a única dependência externa do MPComm.
- O quê: ignorar artefatos do experimento e fixar `requests==2.34.2`.

## 2026-10-11 — reliableFifoUDP.py
- Por quê: o algoritmo de ordenação total exige canais confiáveis e FIFO, propriedades que o UDP original não fornece.
- O quê: implementar sequência por enlace direcionado, stop-and-wait, `LINK_ACK`, retransmissão, supressão de duplicatas, drenagem e falha explícita por timeout.

## 2026-10-11 — totalOrderMulticast.py
- Por quê: mensagens recebidas pelo MPComm eram entregues imediatamente na ordem de chegada de cada peer.
- O quê: implementar relógio de Lamport, fila de retenção, desempate determinístico, `ORDER_ACK` por mensagem e entrega somente da cabeça confirmada por todo o grupo.

## 2026-10-11 — peerCommunicatorOrdered.py
- Por quê: a solução precisava separar chegada de entrega, proteger o estado concorrente e terminar sem mensagens pendentes.
- O quê: integrar transporte e motor de ordenação, adicionar controle `PREPARE/GO/ROUND_STOP`, carga finita e logs separados de chegada, entrega e protocolo.

## 2026-10-11 — comparisonServerOrdered.py e GroupMngr.py
- Por quê: todos os peers precisam iniciar com a mesma composição fixa, e a validação deve cobrir participantes, posições e identificadores completos.
- O quê: congelar a lista de membros, distribuir `run_id` e fotografia comum do grupo, coletar relatórios identificados e validar integralmente as sequências entregues antes da drenagem.

## 2026-10-11 — run_ordered_local.py e test_totalOrderMulticast.py
- Por quê: a implementação precisava de reprodução local e verificação determinística independente da rede.
- O quê: automatizar três peers em aliases de loopback e testar concorrência, empate de timestamps, completude e igualdade das entregas.

## 2026-10-11 — README.md e constMP.py
- Por quê: a nova entrada, garantias, portas, evidências e limites precisavam ser reproduzíveis.
- O quê: documentar execução local/multimáquina, correspondência com a Figura 5.10, separar `LINK_ACK` de `ORDER_ACK` e registrar timeouts compartilhados.

## 2026-10-11 — baseline sem ordenação
- Por quê: o comparador original omitia o último peer e a última posição e fazia uma única leitura TCP.
- O quê: preservar o original, validar logs completos e habilitar três peers locais por aliases de loopback.

## 2026-10-11 — testes locais P-A7
- Por quê: as garantias de segurança, progresso e encerramento precisavam ser verificadas sob atraso, reordenação, duplicação e perda controlada.
- O quê: ampliar os testes do motor de ordenação, adicionar injeção de falhas no enlace UDP confiável e documentar o limite explícito para partição permanente.

## 2026-10-11 — preparação AWS P-A8
- Por quê: oito peers em duas regiões precisam executar o mesmo commit sem editar endereços diferentes no código.
- O quê: aceitar grupo, coordenador e endereço anunciado por variáveis de ambiente e adicionar validação integral dos logs coletados.

## 2026-10-12 — benchmark P-A9
- Por quê: a análise de complexidade e desempenho precisa de medições comparáveis, dados brutos e estatística descritiva.
- O quê: adicionar benchmark local com `time.monotonic_ns`, validação de perdas, resumo CSV/JSON e gráfico SVG sem nova dependência.

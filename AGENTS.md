# Contexto do Projeto (Leia Antes de Comecar)

Este arquivo guarda o contexto para retomada rapida do projeto.
Se voce (assistente de IA) abrir este repositorio, leia este arquivo antes de sugerir mudancas.

## Objetivo
- Projeto para renda extra como afiliado.
- Divulgar ofertas em grupo de WhatsApp para amigos e conhecidos.
- Canais alvo de afiliacao:
  - Mercado Livre
  - Amazon
  - Shopee

## O que a aplicacao faz hoje
- Sobe com Docker Compose:
  - `db` (PostgreSQL)
  - `baileys-worker` (Node + Baileys)
- O worker:
  - Le links na tabela `affiliate_links` com `processed = false`
  - Envia mensagem para grupo de WhatsApp
  - Tenta enriquecer com nome, imagem e preco
  - Marca enviado (`processed = true`, `sent_at`) ou erro (`attempts`, `last_error`)

## Formato atual da mensagem
- Titulo: `Oferta feita pra voce!`
- Campos:
  - `Produto: ...`
  - `Preco: ...` (quando encontrado)
  - `Fonte: ...`
  - `Link: ...`
Observacao: manter consistencia textual com preferencia do usuario (mensagem em PT-BR).

## Estado atual importante
- Nome e imagem do produto estao funcionando bem.
- Preco do Mercado Livre funciona na maior parte dos casos, mas pode falhar em links curtos/social (`meli.la`) por bloqueios (`403`) ou estrutura diferente da pagina.
- Foram adicionados varios fallbacks de preco no worker:
  - API de item do Mercado Livre (quando item id e identificado)
  - Campos alternativos de preco
  - Endpoint `/items/{id}/prices`
  - Meta tags e JSON-LD
  - `aria-label` (padrao Andes: "Agora: X reais com Y centavos")
  - Busca em pagina de produto destacada da pagina social

## Banco de dados
Tabela principal: `affiliate_links`
Campos usados:
- `id`
- `product_name`
- `affiliate_url`
- `source`
- `price_text`
- `image_url`
- `created_at`
- `processed`
- `sent_at`
- `attempts`
- `last_error`

## Como inserir links (resumo)
Exemplo em lote:
```sql
INSERT INTO affiliate_links (product_name, affiliate_url, source)
VALUES
  ('CODIGO-EXEMPLO', 'https://meli.la/xxxx', 'mercado_livre');
```

## Fluxo de retomada recomendado para o assistente
1. Ler `AGENTS.md`.
2. Ler `README.md` e `baileys-bot/index.js` para confirmar estado atual.
3. Antes de mudar logica de preco, reproduzir com 1 link novo em `affiliate_links`.
4. Validar no WhatsApp + consulta SQL final.

## Prioridades de negocio
1. Confiabilidade do envio para o grupo.
2. Nome e imagem corretos do produto.
3. Preco correto (quando disponivel).
4. Evoluir para Amazon e Shopee sem quebrar Mercado Livre.

## Regra de trabalho
- Fazer mudancas pequenas e validaveis.
- Evitar regressao no que ja esta funcionando.
- Sempre deixar comandos de teste prontos para o usuario executar no servidor.

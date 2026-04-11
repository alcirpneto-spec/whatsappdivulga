# whatsappdivulga

Um projeto de automação para analisar diariamente os produtos mais vendidos e divulgar os melhores links em grupos de WhatsApp.

## O que faz
- lê histórico de vendas diária
- seleciona os produtos com melhor desempenho
- **extrai automaticamente descrições dos produtos dos links**
- monta mensagem de divulgação automática com descrições
- envia para um grupo de WhatsApp via WhatsApp Web (Selenium)

## Instalação
1. Instale Python 3.11+.
2. No projeto, instale dependências:

```bash
pip install -r requirements.txt
```

3. Configure `config.py`:
- `GROUP_NAME`
- `CHROME_DRIVER_PATH`
- `SCHEDULE_INTERVAL_MINUTES` (padrão: 30, somente quando `USE_SHOPEE_API=True`)
- `CHECK_TIME` (usado apenas se `SCHEDULE_INTERVAL_MINUTES <= 0`)

4. Em `data/products_links.md`, coloque os links e nomes dos produtos.
5. Em `data/sales_data.csv`, atualize o histórico de vendas.
6. Para links novos (como do TikTok), adicione em `data/new_links.md`.

## Uso
Rode uma vez:

```bash
python main.py
```

Para rodar com agendamento:
- com `USE_SHOPEE_API=True`, executa a cada 30 minutos (padrão)
- sem Shopee API, usa horário diário (`CHECK_TIME`)

```bash
python -m services.scheduler
```

## Rodando em Docker

O projeto agora inclui suporte para Docker. Isso facilita colocar o bot em execução em um servidor remoto como AWS.

### Usando Docker
1. Construa a imagem:
```bash
docker build -t whatsappdivulga .
```
2. Execute o container:
```bash
docker run --rm -it \
  -e GROUP_NAME="Seu Grupo WhatsApp" \
  -e CHROME_DRIVER_PATH=/usr/bin/chromedriver \
  -e CHROME_BINARY_PATH=/usr/bin/chromium \
  -e WHATSAPP_PROFILE_DIR=/app/whatsapp-profile \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/whatsapp-profile:/app/whatsapp-profile" \
  whatsappdivulga
```

### Usando docker-compose
1. Ajuste `docker-compose.yml` se necessário.
2. Execute:
```bash
docker compose up --build
```

### Servidor AWS
Um servidor pequeno da AWS (EC2 t4g.nano / t3.micro ou similar) resolve bem, desde que:
- esteja sempre ligado
- tenha internet estável
- o container Docker esteja rodando
- o perfil do WhatsApp mantenha o login

## Como funciona
- `main.py` executa a análise e faz entrega.
- `services/agent.py` decide se usa novos links ou análise de vendas.
- `services/product_fetcher.py` carrega links e histórico.
- `services/analyzer.py` escolhe os melhores produtos ou processa novos links.
- `services/product_scraper.py` **extrai descrições automaticamente dos links**.
- `services/whatsapp_sender.py` envia mensagem para um grupo.

## Priorização de Novos Links
Se houver links em `data/new_links.md`, o sistema prioriza esses para divulgação, ignorando a análise de vendas. Isso é útil para promover produtos novos ou links específicos do TikTok ou Mercado Livre.

Formato em `new_links.md`:
```
https://link-do-tiktok - Nome do Produto
https://meli.la/2xM6Efi - Nome do Produto
```

## Extração de Descrições
O sistema acessa automaticamente cada link fornecido, resolve links curtos como `meli.la`, e extrai uma breve descrição do produto usando:
- Meta descriptions da página
- Open Graph tags
- Títulos e textos do produto
- Limita a 150 caracteres para mensagens concisas

Se não conseguir extrair, usa uma mensagem padrão.

## Observações
- O envio por WhatsApp depende de login no WhatsApp Web.
- É necessário baixar o ChromeDriver compatível com a versão do Chrome.
- Teste primeiro sem automação para validar as mensagens.

## API Shopee Affiliate - Possibilidades Avançadas

Com acesso à API de afiliados da Shopee, o sistema pode operar em **modo automático completo**:

### 🔍 **Descoberta Automática de Produtos**
- Buscar produtos por categoria ou palavra-chave
- Encontrar produtos em tendência automaticamente
- Identificar produtos com alta rotatividade

### 📊 **Dados Precisos e Estruturados**
- Preços atuais e históricos
- Número real de vendas
- Avaliações e ratings
- Informações completas do produto
- Variações e modelos disponíveis

### 🔗 **Links de Afiliado Otimizados**
- Geração automática de links de afiliado
- Rastreamento de cliques e conversões
- Sub-IDs para diferentes campanhas

### 📈 **Análise de Performance**
- Produtos mais vendidos por categoria
- Tendências de vendas por período
- Produtos em promoção/discount

### 🎯 **Segmentação Inteligente**
- Produtos por faixa de preço
- Categorias específicas
- Produtos com melhor rating
- Itens com alta margem de comissão

### 🤖 **Automação Completa**
- Sem necessidade de links manuais
- Atualização automática diária
- Mensagens sempre com produtos atuais

### Como Ativar
1. Cadastre-se no [Shopee Affiliate](https://affiliate.shopee.com.br/)
2. Obtenha `APP_ID`, `APP_SECRET` e `AFFILIATE_ID`
3. Configure por variáveis de ambiente (recomendado):

  PowerShell (sessão atual):
  ```powershell
  $env:USE_SHOPEE_API="True"
  $env:SHOPEE_APP_ID="seu_app_id"
  $env:SHOPEE_APP_SECRET="seu_app_secret"
  $env:SHOPEE_AFFILIATE_ID="seu_affiliate_id"
  ```

  Com Docker Compose, você também pode criar `.env` a partir de `.env.example`.
4. Rode: `python main.py`

O sistema passará a descobrir e promover produtos automaticamente!

### Como testar o retorno da API Shopee

1. Preencha suas variáveis no arquivo `.env`:
  - `USE_SHOPEE_API=True`
  - `SHOPEE_APP_ID=...`
  - `SHOPEE_APP_SECRET=...`
  - `SHOPEE_AFFILIATE_ID=...`

2. Rode o teste rápido:

```bash
python -m services.test_shopee_api
```

3. O script mostra:
  - se as variáveis foram carregadas
  - quantidade de produtos retornados
  - o JSON do primeiro item (quando houver)

Se retornar `Total retornado: 0`, normalmente é um destes pontos:
- credencial inválida ou incompleta
- `SHOPEE_AFFILIATE_ID` ausente
- autenticação ainda em modo demo no método `get_access_token` (sem OAuth real)

## Modo Baileys + Banco de Dados (Mercado Livre, Amazon e Shopee)

Este modo sobe dois containers no Docker:
- `db` (PostgreSQL): onde você insere os links de afiliado
- `baileys-worker`: conecta no WhatsApp com Baileys e envia links novos no grupo

### Como subir

1. Suba os containers:

```bash
docker compose up --build
```

2. No primeiro start, o `baileys-worker` vai imprimir um QR Code no log.
3. Escaneie o QR com seu WhatsApp para autenticar a sessão.
4. A sessão fica persistida na pasta `baileys-auth/`.

Se for usar descoberta automática com API Shopee no serviço Python (profile `legacy-python`), preencha as variáveis no arquivo `.env`:

```env
USE_SHOPEE_API=True
SHOPEE_APP_ID=seu_app_id
SHOPEE_APP_SECRET=seu_app_secret
SHOPEE_AFFILIATE_ID=seu_affiliate_id
```

### Configuração do grupo

No `docker-compose.yml`, configure uma das opções abaixo no serviço `baileys-worker`:
- `WHATSAPP_GROUP_NAME=Nome do Grupo` (busca pelo nome do grupo)
- `BAILEYS_GROUP_JID=1203xxxxxxxxxxxx@g.us` (ID direto do grupo)

Cadência de verificação do banco no `baileys-worker`:
- `POLL_INTERVAL_SECONDS=60` (padrão atual: 1 minuto)

### Inserindo links no banco

A tabela usada é `affiliate_links`.

Exemplo de insert:

```sql
INSERT INTO affiliate_links (product_name, affiliate_url, source, price_text, image_url, metadata_json)
VALUES
  (
    'Fone Bluetooth X',
    'https://mercadolivre.com/afiliado/abc123',
    'mercado_livre',
    '129,90',
    'https://http2.mlstatic.com/D_NQ_NP_123456-MLB00000000000_012024-O.webp',
    '{"shop_name":"Loja Exemplo","sales":120,"original_price":"59,90","discount_pct":"20"}'::jsonb
  );
```

Exemplos para outras fontes:

```sql
INSERT INTO affiliate_links (product_name, affiliate_url, source)
VALUES
  ('Kindle 16GB', 'https://amzn.to/seu-link-afiliado', 'amazon'),
  ('Escova Secadora', 'https://s.shopee.com.br/seu-link-afiliado', 'shopee');
```

Se `price_text` e `image_url` não forem enviados no insert, o worker tenta extrair automaticamente do link.
Se `metadata_json` for enviado, o worker aproveita dados extras na mensagem (ex.: vendas, loja, preço original e desconto).
Para links com bloqueio anti-bot, o recomendado é enviar `price_text` no insert para garantir consistência da oferta.

Você pode inserir usando qualquer cliente SQL conectado no PostgreSQL (`localhost:5432`), com:
- banco: `whatsappdivulga`
- usuário: `bot`
- senha: `botpass`

### Como o worker processa

- Busca registros com `processed = FALSE`
- Monta mensagem no formato "Oferta feita pra você!"
- Detecta a origem (`source`) entre Mercado Livre, Amazon e Shopee (ou infere pela URL)
- Tenta incluir preço e imagem do produto com estratégia por marketplace
- Faz merge de `metadata_json` com metadados de enriquecimento
- Inclui vendas, loja, preço original (`De`) e desconto quando esses dados existirem
- Envia mensagem no grupo
- Marca como enviado (`processed = TRUE`, `sent_at = NOW()`)
- Em caso de erro, incrementa `attempts` e salva `last_error`

Assim, sempre que entrar link novo na tabela, ele será enviado automaticamente.

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
- `CHECK_TIME`

4. Em `data/products_links.md`, coloque os links e nomes dos produtos.
5. Em `data/sales_data.csv`, atualize o histórico de vendas.
6. Para links novos (como do TikTok), adicione em `data/new_links.md`.

## Uso
Rode uma vez:

```bash
python main.py
```

Para rodar diariamente com agendamento:

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
3. Configure em `config.py`:
   ```python
   USE_SHOPEE_API = True
   SHOPEE_APP_ID = "seu_app_id"
   SHOPEE_APP_SECRET = "seu_app_secret"
   SHOPEE_AFFILIATE_ID = "seu_affiliate_id"
   ```
4. Rode: `python main.py`

O sistema passará a descobrir e promover produtos automaticamente!

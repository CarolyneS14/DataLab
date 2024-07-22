  # Criei nova tabela (produto)
  ## Exclui duplicados (ROW_NUMBER)
  ## Troquei caracteres especiais (REGEXP_REPLACE)
  ## Alterei valores nulos na coluna descricao (COALESCE)
  ## Calcular quartil das variaveis numericas (NTILE)

CREATE OR REPLACE TABLE `amazon-sale.data.produto` AS
WITH produto AS (
  SELECT 
    REGEXP_REPLACE(product_id, r'[^a-zA-Z0-9]', '') AS id_produto, 
    REGEXP_REPLACE(product_name, r'[^a-zA-Z0-9 ]', '') AS nome_produto, 
    LOWER(
      CASE
        WHEN REGEXP_CONTAINS(category, r'^Toys&Games|Arts&Crafts|Drawing&PaintingSupplies|ColouringPens&Markers') THEN 'Brinquedos e Jogos'
        WHEN REGEXP_CONTAINS(category, r'^Car&Motorbike|CarAccessories|InteriorAccessories|AirPurifiers&Ionizers') THEN 'Carro e Moto'
        WHEN REGEXP_CONTAINS(category, r'^Home&Kitchen') THEN 'Casa e Cozinha'
        WHEN REGEXP_CONTAINS(category, r'^Computers&Accessories') THEN 'Acessórios de Computador'
        WHEN REGEXP_CONTAINS(category, r'^MusicalInstruments') THEN 'Instrumentos Musicais'
        WHEN REGEXP_CONTAINS(category, r'^Home|Improvement') THEN 'Decoração e Mobília'
        WHEN REGEXP_CONTAINS(category, r'^Health&PersonalCare') THEN 'Saúde Pessoal'
        WHEN REGEXP_CONTAINS(category, r'^OfficeProducts') THEN 'Produtos de Escritório'
        WHEN REGEXP_CONTAINS(category, r'^Electronics|') THEN 'Eletrônicos'
        ELSE 'Outros'
      END
    ) AS categoria_produtos,
    CAST(discounted_price AS INT64) AS preco_com_desconto,
    NTILE(4) OVER (ORDER BY discounted_price) AS quartil_preco_com_desconto, 
    CAST(actual_price AS INT64) AS preco_atual,
    NTILE(4) OVER (ORDER BY actual_price) AS quartil_preco_atual, 
    CAST(discount_percentage * 100 AS INT) AS desconto_inteiro,
    discount_percentage AS porcentagem_desconto,
    NTILE(4) OVER (ORDER BY discount_percentage) AS quartil_porcent_desconto,
    actual_price - discounted_price AS valor_desconto,
    COALESCE(REGEXP_REPLACE(about_product, r'[^a-zA-Z0-9 ]', ''), 'sem descricao do produto') AS descricao_produto,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS row_num
  FROM `amazon-sale.data.product`
),
categoria AS (
  SELECT
    product_id,
    REGEXP_REPLACE(category, r'[^a-zA-Z0-9 ]', '') AS categoria,
  FROM `amazon-sale.data.product`
)
SELECT 
  p.id_produto,
  p.nome_produto,
  c.categoria,
  p.categoria_produtos,
  p.preco_com_desconto,
  p.quartil_preco_com_desconto,
  p.preco_atual,
  p.quartil_preco_atual,
  p.desconto_inteiro,
  p.porcentagem_desconto,
  p.quartil_porcent_desconto,
  p.valor_desconto,
  NTILE(4) OVER (ORDER BY p.valor_desconto) AS quartil_valor_desconto,
  p.descricao_produto
FROM produto p
JOIN categoria c ON p.id_produto = c.product_id
WHERE p.row_num = 1;


  ## Verificar nulos na tabela nova (produto)
-- Verificar quantidade de categorias
SELECT 
DISTINCT categoria_produtos
FROM `amazon-sale.data.produto`
ORDER BY categoria_produtos;

-- Quantas categorias eram
SELECT
DISTINCT category 
FROM `amazon-sale.data.product`


-- Verificar valores duplicados
SELECT 
    id_produto,
    COUNT(*) AS contagem
FROM 
    `amazon-sale.data.produto`
GROUP BY 
    id_produto
HAVING 
    COUNT(*) > 1;
-- Verificar valores nulos
SELECT 
    id_produto,
    nome_produto,
    categoria,
    categoria_produtos,
    preco_com_desconto,
    quartil_preco_com_desconto,
    preco_atual,
    quartil_preco_atual,
    porcentagem_desconto,
    desconto_inteiro,
    quartil_porcent_desconto,
    valor_desconto,
    quartil_valor_desconto,
    descricao_produto
FROM 
    `amazon-sale.data.produto`
WHERE 
    id_produto IS NULL OR
    nome_produto IS NULL OR
    categoria IS NULL OR
    categoria_produtos IS NULL OR
    preco_com_desconto IS NULL OR
    preco_atual IS NULL OR
    porcentagem_desconto IS NULL OR
    desconto_inteiro IS NULL OR
    valor_desconto IS NULL OR
    descricao_produto IS NULL;


  # Criei nova tabela (avaliacao)
  ## Exclui duplicados (ROW_NUMBER)
  ## Troquei caracteres especiais (REGEXP_REPLACE)
  ## Alterei valores nulos na coluna descricao (COALESCE)
  ## Calculei os quartis das variaveis numericas (NTILE)
CREATE OR REPLACE TABLE `amazon-sale.data.avaliacao` AS
WITH avaliacao AS (
  SELECT 
    REGEXP_REPLACE(user_id, r'[^a-zA-Z0-9]', '') AS id_usuario,
    REGEXP_REPLACE(user_name, r'[^a-zA-Z0-9 ]', '') AS nome_usuario,
    product_id AS id_produto,
    SAFE_CAST(REPLACE(rating, '|', '4') AS FLOAT64) AS classif_do_produto,
    NTILE(4) OVER (ORDER BY rating) AS quartil_classif_do_produto, 
    IFNULL(rating_count, 0) AS qtd_votos,
    NTILE(4) OVER (ORDER BY rating_count) AS quartil_votos,
    REGEXP_REPLACE(review_id, r'[^a-zA-Z0-9]', '') AS id_avaliacao,
    COALESCE(REGEXP_REPLACE(review_title, r'[^a-zA-Z0-9 ]', ''), 'sem descricao do produto') AS resumo_avaliacao,
    COALESCE(REGEXP_REPLACE(review_content, r'[^a-zA-Z0-9 ]', ''), 'sem descricao do produto') AS avaliacao_completa,
    COALESCE(img_link, 'Ausente') AS imagem_produto,
    COALESCE(product_link, 'Ausente' ) AS link_produto,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY review_id) AS row_num
  FROM `amazon-sale.data.review`
)
SELECT 
  id_usuario,
  nome_usuario, 
  id_avaliacao, 
  id_produto, 
  classif_do_produto,
  quartil_classif_do_produto,  
  qtd_votos,
  quartil_votos,
  resumo_avaliacao,
  avaliacao_completa,
  imagem_produto,
  link_produto
FROM avaliacao
WHERE row_num = 1; -- Mantém apenas a primeira ocorrência de cada usuário

  ## Verificar nulos na tabela nova (avaliacao)
-- Verificar valores duplicados
SELECT 
    id_avaliacao,
    COUNT(*) AS contagem
FROM 
    `amazon-sale.data.avaliacao`
GROUP BY 
    id_avaliacao
HAVING 
    COUNT(*) > 1;
-- Verificar valores nulos
SELECT 
    id_usuario, 
    nome_usuario, 
    id_avaliacao, 
    id_produto, 
    classif_do_produto, 
    qtd_votos, 
    resumo_avaliacao, 
    avaliacao_completa, 
    imagem_produto, 
    link_produto
FROM 
    `amazon-sale.data.avaliacao`
WHERE 
    id_usuario IS NULL OR
    nome_usuario IS NULL OR
    id_avaliacao IS NULL OR
    id_produto IS NULL OR
    classif_do_produto IS NULL OR
    qtd_votos IS NULL OR
    resumo_avaliacao IS NULL OR
    avaliacao_completa IS NULL OR
    imagem_produto IS NULL OR
    link_produto IS NULL;

  ## JOIN - Unir as tabelas limpas

CREATE OR REPLACE TABLE `amazon-sale.data.vendas` AS
WITH produto AS (
  SELECT
    id_produto,
    nome_produto,
    categoria,
    categoria_produtos,
    preco_com_desconto,
    quartil_preco_com_desconto,
    preco_atual,
    quartil_preco_atual,
    porcentagem_desconto,
    desconto_inteiro,
    quartil_porcent_desconto,
    valor_desconto,
    quartil_valor_desconto,
    descricao_produto
  FROM `amazon-sale.data.produto`
),
avaliacao AS (
  SELECT 
  id_usuario,
  nome_usuario, 
  id_avaliacao, 
  id_produto, 
  classif_do_produto,
  quartil_classif_do_produto, 
  qtd_votos,
  quartil_votos,
  resumo_avaliacao,
  avaliacao_completa,
  imagem_produto,
  link_produto
  FROM `amazon-sale.data.avaliacao`
)
SELECT
  p.id_produto,
  p.nome_produto,
  p.categoria,
  p.categoria_produtos,
  p.preco_com_desconto,
  p.quartil_preco_com_desconto, 
  p.preco_atual,
  p.quartil_preco_atual,
  p.desconto_inteiro,  
  p.porcentagem_desconto,
  p.quartil_porcent_desconto,  
  p.valor_desconto,
  p.quartil_valor_desconto, 
  p.descricao_produto,
  a.id_usuario,
  a.nome_usuario,
  a.id_avaliacao,
  a.classif_do_produto,
  a.quartil_classif_do_produto,
  a.qtd_votos,
  a.quartil_votos, 
  a.resumo_avaliacao,
  a.avaliacao_completa,
  a.imagem_produto,
  a.link_produto
FROM `amazon-sale.data.produto` p
JOIN avaliacao a ON p.id_produto = a.id_produto;

 ## Verificar nulos na tabela unificada (vendas)
-- Verificar valores duplicados
SELECT 
    id_avaliacao,
    COUNT(*) AS contagem
FROM 
    `amazon-sale.data.vendas`
GROUP BY 
    id_avaliacao
HAVING 
    COUNT(*) > 1;

-- Verificar valores nulos
SELECT 
  id_usuario, nome_usuario, id_avaliacao, id_produto, classif_do_produto, quartil_classif_do_produto, qtd_votos, quartil_votos, resumo_avaliacao, avaliacao_completa, imagem_produto, link_produto, id_produto, nome_produto, categoria, categoria_produtos, preco_com_desconto, quartil_preco_com_desconto, preco_atual, quartil_preco_atual, porcentagem_desconto, desconto_inteiro, quartil_porcent_desconto, valor_desconto, quartil_valor_desconto, descricao_produto
FROM 
    `amazon-sale.data.vendas`
WHERE 
    id_usuario IS NULL OR
    nome_usuario IS NULL OR
    id_avaliacao IS NULL OR
    id_produto IS NULL OR
    classif_do_produto IS NULL OR
    quartil_classif_do_produto IS NULL OR
    qtd_votos IS NULL OR
    resumo_avaliacao IS NULL OR
    avaliacao_completa IS NULL OR
    imagem_produto IS NULL OR
    link_produto IS NULL OR
    id_produto IS NULL OR
    nome_produto IS NULL OR
    categoria IS NULL OR
    categoria_produtos IS NULL OR
    preco_com_desconto IS NULL OR
    quartil_preco_com_desconto IS NULL OR
    quartil_preco_atual IS NULL OR
    preco_atual IS NULL OR
    porcentagem_desconto IS NULL OR
    desconto_inteiro IS NULL OR
    quartil_porcent_desconto IS NULL OR
    valor_desconto IS NULL OR
    quartil_valor_desconto IS NULL OR
    descricao_produto IS NULL;


-- Não existem mais dados para limpar na nossa base de dados! 

  #Segmentando os dados categoricos. em Boa e Ruim
CREATE OR REPLACE TABLE `amazon-sale.data.vendas_segmentada` AS
WITH vendas_segmentada AS (
  SELECT
    v.*,
    CASE
      WHEN quartil_classif_do_produto BETWEEN 1 AND 2 THEN 'BAIXA'
      WHEN quartil_classif_do_produto BETWEEN 3 AND 4 THEN 'ALTA'
    END AS segmentacao_classif_do_produto,
    CASE
      WHEN quartil_preco_com_desconto BETWEEN 1 AND 2 THEN 'BOM'
      WHEN quartil_preco_com_desconto BETWEEN 3 AND 4 THEN 'RUIM'
    END AS segmentacao_preco_com_desconto,
    CASE
      WHEN quartil_preco_atual BETWEEN 1 AND 2 THEN 'BOM'
      WHEN quartil_preco_atual BETWEEN 3 AND 4 THEN 'RUIM'
    END AS segmentacao_preco_atual,
    CASE
      WHEN quartil_porcent_desconto BETWEEN 1 AND 2 THEN 'RUIM'
      WHEN quartil_porcent_desconto BETWEEN 3 AND 4 THEN 'BOM'
    END AS segmentacao_porcent_desconto,
    CASE
      WHEN quartil_valor_desconto BETWEEN 1 AND 2 THEN 'RUIM'
      WHEN quartil_valor_desconto BETWEEN 3 AND 4 THEN 'BOM'
    END AS segmentacao_valor_desconto,
    CASE
      WHEN quartil_votos BETWEEN 1 AND 2 THEN 'BAIXA'
      WHEN quartil_votos BETWEEN 3 AND 4 THEN 'ALTA'
    END AS segmentacao_qtd_votos
  FROM `amazon-sale.data.vendas` v
)
SELECT *
FROM vendas_segmentada;

## Aplicar medidas de tendência central
SELECT
MIN(qtd_votos) AS min_votos,
MAX(qtd_votos) AS max_votos,
AVG(qtd_votos) AS media_votos,
APPROX_QUANTILES(qtd_votos, 100)[SAFE_ORDINAL(50)] AS mediana_votos,
STDDEV(qtd_votos) AS desvio_votos,

MIN(preco_atual) AS min_preco_atual,
MAX(preco_atual) AS max_preco_atual,
AVG(preco_atual) AS media_preco_atual,
APPROX_QUANTILES(preco_atual, 100)[SAFE_ORDINAL(50)] AS mediana_preco_atual,
STDDEV(preco_atual) AS desvio_preco_atual,

MIN(preco_com_desconto) AS min_preco_com_desconto,
MAX(preco_com_desconto) AS max_preco_com_desconto,
AVG(preco_com_desconto) AS media_preco_com_desconto,
APPROX_QUANTILES(preco_com_desconto, 100)[SAFE_ORDINAL(50)] AS mediana_preco_com_desconto,
STDDEV(preco_com_desconto) AS desvio_preco_com_desconto,

MIN(valor_desconto) AS min_valor_desconto,
MAX(valor_desconto) AS max_valor_desconto,
AVG(valor_desconto) AS media_valor_desconto,
APPROX_QUANTILES(valor_desconto, 100)[SAFE_ORDINAL(50)] AS mediana_valor_desconto,
STDDEV(valor_desconto) AS desvio_valor_desconto,

MIN(porcentagem_desconto) AS min_perc_desconto,
MAX(porcentagem_desconto) AS max_perc_desconto,
AVG(porcentagem_desconto) AS media_perc_desconto,
APPROX_QUANTILES(porcentagem_desconto, 100)[SAFE_ORDINAL(50)] AS mediana_perc_desconto,
STDDEV(porcentagem_desconto) AS desvio_perc_desconto,

MIN(desconto_inteiro) AS min_desconto,
MAX(desconto_inteiro) AS max_desconto,
AVG(desconto_inteiro) AS media_desconto,
APPROX_QUANTILES(desconto_inteiro, 100)[SAFE_ORDINAL(50)] AS mediana_desconto,
STDDEV(desconto_inteiro) AS desvio_desconto,
FROM `amazon-sale.data.vendas`; 

  ## Correlação

  SELECT
    CORR(porcentagem_desconto, classif_do_produto) AS Hipot1,
    CORR(qtd_votos, classif_do_produto) AS Hipot2
  FROM `amazon-sale.data.vendas_segmentada`


 SELECT 
CORR(classif_do_produto, preco_com_desconto),
CORR(qtd_votos, porcentagem_desconto),
CORR(qtd_votos, valor_desconto),
CORR(preco_atual, preco_com_desconto) 
FROM `amazon-sale.data.vendas_segmentada`;  
  




 

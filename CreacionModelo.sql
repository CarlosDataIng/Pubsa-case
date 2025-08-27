CREATE OR REPLACE MODEL `casepubsa.marketing_dw.customer_final_socioeconomic_model_3`
OPTIONS(
  model_type='kmeans',
  num_clusters=3,
  standardize_features = TRUE
) AS
SELECT
  -- Las 4 características clave 
  Income,
  Total_Children,
  Total_Spend,
  Customer_Age
FROM
  `casepubsa.marketing_dw.campaign_prod`
WHERE
  Customer_Age < 100 AND Income < 200000;

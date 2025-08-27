WITH CustomerClusters AS (
  SELECT
    *,
    (AcceptedCmp1 + AcceptedCmp2 + AcceptedCmp3 + AcceptedCmp4 + AcceptedCmp5 + Response) AS TotalCampaignsAccepted
  FROM
    ML.PREDICT(MODEL `casepubsa.marketing_dw.customer_final_socioeconomic_model_3`,
      (
        SELECT
          *
        FROM
          `casepubsa.marketing_dw.campaign_prod`
        WHERE
          Customer_Age < 100 AND Income < 200000
      )
    )
)
SELECT
  CASE
    WHEN CENTROID_ID = 1 THEN 'Adulto soltero de clase alta'
    WHEN CENTROID_ID = 3 THEN 'Padre Veterano de clase media'
    WHEN CENTROID_ID = 2 THEN 'Padre Jóven de clase media-baja'
    ELSE 'Indefinido'
  END AS persona_cliente,
  COUNT(*) AS total_clientes,
  ROUND(AVG(Income), 0) AS ingreso_promedio,
  ROUND(AVG(Total_Spend), 0) AS gasto_promedio,
  ROUND(AVG(Customer_Age), 0) AS edad_promedio,
  ROUND(AVG(Total_Children), 1) AS hijos_promedio,
  ROUND(AVG(MntWines), 0) AS gasto_vinos,
  ROUND(AVG(MntMeatProducts), 0) AS gasto_carnes,
  ROUND(AVG(MntFruits), 0) AS gasto_frutas,
  ROUND(AVG(NumDealsPurchases), 1) AS compras_con_descuento_promedio,
  -- CAMPOS AÑADIDOS PARA CANAL DE COMPRA
  ROUND(AVG(NumWebPurchases), 1) AS compras_web_promedio,
  ROUND(AVG(NumCatalogPurchases), 1) AS compras_catalogo_promedio,
  ROUND(AVG(NumStorePurchases), 1) AS compras_tienda_promedio,
  ROUND(AVG(TotalCampaignsAccepted), 1) AS campanas_aceptadas_promedio
FROM
  CustomerClusters
GROUP BY
  persona_cliente
ORDER BY
 ingreso_promedio;

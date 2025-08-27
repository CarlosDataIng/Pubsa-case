import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from datetime import date
import csv
import io

# --- Configuración del Pipeline ---
PROJECT_ID = 'casepubsa'
BUCKET = 'pubsa_case_customer_personality'
DATASET = 'marketing_dw'
TABLE = 'campaign_prod'

# --- Lógica de Transformación ---

def parse_csv(element):
    """Parsea una línea del CSV de forma robusta aceptando 29 columnas."""
    try:
        reader = csv.reader(io.StringIO(element), delimiter='\t')
        fields = next(reader)
        
        if len(fields) != 29: # <-- CORRECCIÓN: Ahora esperamos 29 columnas
            raise ValueError(f"Se esperaban 29 columnas, pero se encontraron {len(fields)}")

        # <-- CORRECCIÓN: Se añaden las 2 columnas extra (Z_CostContact, Z_Revenue)
        (id, year_birth, education, marital_status, income, kidhome, teenhome, 
         dt_customer, recency, mnt_wines, mnt_fruits, mnt_meat, mnt_fish, 
         mnt_sweet, mnt_gold, num_deals_purchases, num_web_purchases, 
         num_catalog_purchases, num_store_purchases, num_web_visits_month, 
         accepted_cmp3, accepted_cmp4, accepted_cmp5, accepted_cmp1, 
         accepted_cmp2, complain, z_cost_contact, z_revenue, response) = fields

        return {
            'ID': int(id),
            'Year_Birth': int(year_birth),
            'Education': education,
            'Marital_Status': marital_status,
            'Income': float(income) if income else 0.0,
            'Kidhome': int(kidhome),
            'Teenhome': int(teenhome),
            'Dt_Customer': dt_customer,
            'Recency': int(recency),
            'MntWines': int(mnt_wines),
            'MntFruits': int(mnt_fruits),
            'MntMeatProducts': int(mnt_meat),
            'MntFishProducts': int(mnt_fish),
            'MntSweetProducts': int(mnt_sweet),
            'MntGoldProds': int(mnt_gold),
            'NumDealsPurchases': int(num_deals_purchases),
            'NumWebPurchases': int(num_web_purchases),
            'NumCatalogPurchases': int(num_catalog_purchases),
            'NumStorePurchases': int(num_store_purchases),
            'NumWebVisitsMonth': int(num_web_visits_month),
            'AcceptedCmp1': int(accepted_cmp1),
            'AcceptedCmp2': int(accepted_cmp2),
            'AcceptedCmp3': int(accepted_cmp3),
            'AcceptedCmp4': int(accepted_cmp4),
            'AcceptedCmp5': int(accepted_cmp5),
            'Complain': int(complain),
            'Z_CostContact': int(z_cost_contact), # <-- CAMPO NUEVO
            'Z_Revenue': int(z_revenue),       # <-- CAMPO NUEVO
            'Response': int(response)
        }
    except Exception as e:
        logging.warning(f"Fila omitida por error de parseo: {e} -> {element}")
        return None

def feature_engineering(element):
    """Crea las nuevas columnas (Ingeniería de Características)."""
    if not element:
        return None
        
    current_year = date.today().year
    
    element['Customer_Age'] = current_year - element['Year_Birth']
    element['Total_Children'] = element['Kidhome'] + element['Teenhome']
    element['Total_Spend'] = (element['MntWines'] + element['MntFruits'] + 
                              element['MntMeatProducts'] + element['MntFishProducts'] + 
                              element['MntSweetProducts'] + element['MntGoldProds'])
    element['Has_Complained'] = True if element['Complain'] == 1 else False
    
    return element
    
def run():
    """Función principal que ejecuta el pipeline."""
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        temp_location=f'gs://{BUCKET}/temp',
        staging_location=f'gs://{BUCKET}/staging',
        region='us-central1'
    )
    
    # <-- CORRECCIÓN: Se añaden las 2 columnas extra al esquema de la tabla
    table_schema = (
        'ID:INTEGER, Year_Birth:INTEGER, Education:STRING, Marital_Status:STRING, '
        'Income:FLOAT, Kidhome:INTEGER, Teenhome:INTEGER, Dt_Customer:STRING, '
        'Recency:INTEGER, MntWines:INTEGER, MntFruits:INTEGER, MntMeatProducts:INTEGER, '
        'MntFishProducts:INTEGER, MntSweetProducts:INTEGER, MntGoldProds:INTEGER, '
        'NumDealsPurchases:INTEGER, NumWebPurchases:INTEGER, NumCatalogPurchases:INTEGER, '
        'NumStorePurchases:INTEGER, NumWebVisitsMonth:INTEGER, AcceptedCmp1:INTEGER, '
        'AcceptedCmp2:INTEGER, AcceptedCmp3:INTEGER, AcceptedCmp4:INTEGER, '
        'AcceptedCmp5:INTEGER, Complain:INTEGER, Z_CostContact:INTEGER, Z_Revenue:INTEGER, Response:INTEGER, '
        # Nuevas columnas de Feature Engineering
        'Customer_Age:INTEGER, Total_Children:INTEGER, Total_Spend:INTEGER, Has_Complained:BOOLEAN'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p | 'Leer CSV de GCS' >> beam.io.ReadFromText(f'gs://{BUCKET}/marketing_campaign.csv', skip_header_lines=1)
              | 'Parsear CSV' >> beam.Map(parse_csv)
              | 'Filtrar Filas con Error' >> beam.Filter(lambda x: x is not None)
              | 'Ingeniería de Características' >> beam.Map(feature_engineering)
              | 'Escribir a BigQuery' >> beam.io.WriteToBigQuery(
                  table=f'{PROJECT_ID}:{DATASET}.{TABLE}',
                  schema=table_schema,
                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
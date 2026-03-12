# --- cadastrar_parametros.py ---
import pandas as pd
from sqlalchemy import create_engine, text

def limpar(v):
    if pd.isna(v) or str(v).strip() in ['-', 'N.A.', '', 'None', 'Ausente']: return None
    try: return float(str(v).replace(',', '.'))
    except: return None

def main():
    engine = create_engine("postgresql://postgres.zmmylgtdorzdkdxpmnvj:FTNblind19@aws-1-sa-east-1.pooler.supabase.com:5432/postgres")
    arquivo = 'cadastro_parametros_opyta.xlsx'
    abas = {'Aguas_Superficiais': 'Água Superficial', 'Aguas_Subterraneas': 'Água Subterrânea', 'Sedimento': 'Sedimento', 'Efluentes': 'Efluente'}

    for nome_aba, matriz_db in abas.items():
        try:
            df = pd.read_excel(arquivo, sheet_name=nome_aba).dropna(subset=['Parametro'])
            df.columns = [c.lower() for c in df.columns] # Excel -> Minúsculo
            
            records = []
            for _, row in df.iterrows():
                records.append({
                    "n": str(row['parametro']).strip(),
                    "m": matriz_db,
                    "u": row.get('unidade_medida'),
                    "c1min": limpar(row.get('vmp_357_cl1_min')),
                    "c1max": limpar(row.get('vmp_357_cl1_max')),
                    "c2min": limpar(row.get('vmp_357_cl2_min')),
                    "c2max": limpar(row.get('vmp_357_cl2_max')),
                    "ch": limpar(row.get('vmp_396_consumo_humano')),
                    "an": limpar(row.get('vmp_396_dessedentacao_animal')),
                    "ir": limpar(row.get('vmp_396_irrigacao')),
                    "re": limpar(row.get('vmp_396_recreacao')),
                    "n1": limpar(row.get('vmp_454_n1')),
                    "n2": limpar(row.get('vmp_454_n2')),
                    "p430": limpar(row.get('vmp_430_padrao'))
                })

            query = text("""
                INSERT INTO public.parametros_analise 
                (nome_parametro, matriz, unidade_medida, 
                 vmp_357_cl1_min, vmp_357_cl1_max, vmp_357_cl2_min, vmp_357_cl2_max, 
                 vmp_396_consumo_humano, vmp_396_dessedentacao_animal, vmp_396_irrigacao, vmp_396_recreacao,
                 vmp_454_n1, vmp_454_n2, vmp_430_padrao)
                VALUES (:n, :m, :u, :c1min, :c1max, :c2min, :c2max, :ch, :an, :ir, :re, :n1, :n2, :p430)
                ON CONFLICT (nome_parametro, matriz) DO UPDATE SET 
                unidade_medida = EXCLUDED.unidade_medida,
                vmp_357_cl1_min=EXCLUDED.vmp_357_cl1_min, vmp_357_cl1_max=EXCLUDED.vmp_357_cl1_max,
                vmp_357_cl2_min=EXCLUDED.vmp_357_cl2_min, vmp_357_cl2_max=EXCLUDED.vmp_357_cl2_max,
                vmp_396_consumo_humano=EXCLUDED.vmp_396_consumo_humano, 
                vmp_396_dessedentacao_animal=EXCLUDED.vmp_396_dessedentacao_animal,
                vmp_396_irrigacao=EXCLUDED.vmp_396_irrigacao, vmp_396_recreacao=EXCLUDED.vmp_396_recreacao,
                vmp_454_n1=EXCLUDED.vmp_454_n1, vmp_454_n2=EXCLUDED.vmp_454_n2,
                vmp_430_padrao=EXCLUDED.vmp_430_padrao;
            """)
            with engine.begin() as conn: conn.execute(query, records)
            print(f"✅ {matriz_db} OK.")
        except Exception as e: print(f"❌ Erro {nome_aba}: {e}")

if __name__ == "__main__": main()
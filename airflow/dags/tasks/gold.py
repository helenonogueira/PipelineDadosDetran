import pandas as pd
import pymysql
from fpdf import FPDF
import matplotlib.pyplot as plt
from shapely.geometry import Point
import folium
from folium.plugins import HeatMap
import logging
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Conectar ao banco de dados MySQL."""
    logger.info("Conectando ao banco de dados MySQL...")
    return pymysql.connect(
        host="mysql-airflow",
        user="airflow_user",
        password="airflow_password",
        database="airflow",
        port=3306
    )

def fetch_data():
    """Buscar dados da tabela acidentes."""
    logger.info("Buscando dados da tabela 'acidentes_silver' no banco de dados...")
    conn = connect_to_db()
    query = "SELECT * FROM acidentes_silver"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Dados carregados com sucesso. Total de linhas: {len(df)}")
    return df

def analyze_data(df):
    """Analisar os dados e gerar insights."""
    logger.info("Iniciando análises dos dados...")
    # Média de acidentes por estado
    media_acidentes_estado = df.groupby('uf')['id'].count().mean()
    logger.info(f"Média de acidentes por estado calculada: {media_acidentes_estado:.2f}")
    
    # Acidentes por dia da semana
    df['dia_semana'] = pd.to_datetime(df['data_inversa']).dt.dayofweek
    acidentes_dia_semana = df['dia_semana'].apply(lambda x: 'Fim de semana' if x >= 5 else 'Dia útil').value_counts()
    logger.info("Acidentes por tipo de dia analisados com sucesso.")

    # Óbitos e acidentes por estado
    obitos_por_estado = df.groupby('uf').agg({
        'mortos': 'sum',
        'id': 'count'
    }).rename(columns={'id': 'total_acidentes'})
    logger.info("Óbitos e acidentes por estado calculados com sucesso.")

    # Acidentes em condições climáticas
    condicoes_normais = df[df['condicao_metereologica'] == 'Normal']['id'].count()
    condicoes_chuvosas = df[df['condicao_metereologica'] == 'Chuva']['id'].count()
    logger.info("Análise de condições climáticas concluída.")

    # Principais causas de acidentes
    principais_causas = df['causa_principal'].value_counts().head(5)
    logger.info("Principais causas de acidentes analisadas.")

    return {
        "media_acidentes_estado": media_acidentes_estado,
        "acidentes_dia_semana": acidentes_dia_semana,
        "obitos_por_estado": obitos_por_estado,
        "condicoes_normais": condicoes_normais,
        "condicoes_chuvosas": condicoes_chuvosas,
        "principais_causas": principais_causas
    }

def create_heatmap(df):
    """Criar um mapa de calor baseado nos acidentes."""
    logger.info("Criando mapa de calor com os dados de acidentes...")
    df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    mapa = folium.Map(location=[-15.7801, -47.9292], zoom_start=5)
    heat_data = [[row['latitude'], row['longitude']] for _, row in df.iterrows()]
    HeatMap(heat_data).add_to(mapa)
    mapa_path = "/opt/airflow/data/heatmap_acidentes.html"
    mapa.save(mapa_path)
    logger.info(f"Mapa de calor salvo em '{mapa_path}'.")
    return mapa_path

def create_graphs(analysis_results):
    """Criar gráficos com base nas análises e salvar como imagens."""
    output_dir = "/opt/airflow/data/graphs"
    os.makedirs(output_dir, exist_ok=True)

    # Gráfico de acidentes por tipo de dia (Pizza)
    plt.figure(figsize=(8, 6))
    analysis_results['acidentes_dia_semana'].plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['blue', 'orange'])
    plt.title("Acidentes por Tipo de Dia")
    plt.ylabel("")  # Remove o label desnecessário
    dia_path = os.path.join(output_dir, "acidentes_tipo_dia.png")
    plt.savefig(dia_path)
    logger.info(f"Gráfico de acidentes por tipo de dia salvo em '{dia_path}'")
    plt.close()

    # Gráfico de óbitos e acidentes por estado (Barras agrupadas)
    plt.figure(figsize=(10, 8))
    analysis_results['obitos_por_estado'].plot(kind='bar', width=0.8)
    plt.title("Óbitos e Acidentes por Estado")
    plt.xlabel("Estado")
    plt.ylabel("Quantidade")
    plt.tight_layout()
    estado_path = os.path.join(output_dir, "obitos_acidentes_estado.png")
    plt.savefig(estado_path)
    logger.info(f"Gráfico de óbitos e acidentes por estado salvo em '{estado_path}'")
    plt.close()

    # Gráfico de principais causas de acidentes (Barras verticais)
    plt.figure(figsize=(10, 6))
    analysis_results['principais_causas'].plot(kind='bar', color='red')
    plt.title("Principais Causas de Acidentes")
    plt.xlabel("Causa Principal")
    plt.ylabel("Número de Acidentes")
    plt.tight_layout()
    causa_path = os.path.join(output_dir, "principais_causas.png")
    plt.savefig(causa_path)
    logger.info(f"Gráfico de principais causas salvo em '{causa_path}'")
    plt.close()

    return [dia_path, estado_path, causa_path]

def generate_pdf_report(analysis_results, graph_paths, heatmap_path):
    """Gerar relatório em PDF com os resultados das análises, gráficos e heatmap."""
    pdf_path = "/opt/airflow/data/analise_acidentes.pdf"
    logger.info("Gerando relatório PDF com os resultados das análises e gráficos...")
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="Relatório de Análises - Camada Gold", ln=True, align='C')
    pdf.ln(10)

    pdf.cell(200, 10, txt=f"Média de acidentes por estado: {analysis_results['media_acidentes_estado']:.2f}", ln=True)
    pdf.cell(200, 10, txt="Acidentes por tipo de dia:", ln=True)
    for dia, count in analysis_results['acidentes_dia_semana'].items():
        pdf.cell(200, 10, txt=f"  - {dia}: {count}", ln=True)
    pdf.ln(5)

    pdf.cell(200, 10, txt="Óbitos e acidentes por estado:", ln=True)
    for estado, row in analysis_results['obitos_por_estado'].iterrows():
        pdf.cell(200, 10, txt=f"  - {estado}: {row['mortos']} óbitos, {row['total_acidentes']} acidentes", ln=True)
    pdf.ln(5)

    pdf.cell(200, 10, txt=f"Acidentes em condições normais: {analysis_results['condicoes_normais']}", ln=True)
    pdf.cell(200, 10, txt=f"Acidentes em condições de chuva: {analysis_results['condicoes_chuvosas']}", ln=True)
    pdf.ln(5)

    pdf.cell(200, 10, txt="Principais causas de acidentes:", ln=True)
    for causa, count in analysis_results['principais_causas'].items():
        pdf.cell(200, 10, txt=f"  - {causa}: {count}", ln=True)

    # Adicionando gráficos ao PDF
    for graph_path in graph_paths:
        pdf.add_page()
        pdf.image(graph_path, x=10, y=30, w=190)

    # Adicionando Heatmap ao PDF
    pdf.add_page()
    pdf.cell(200, 10, txt="Mapa de Calor de Acidentes:", ln=True)
    pdf.image(heatmap_path, x=10, y=30, w=190)

    pdf.output(pdf_path)
    logger.info(f"Relatório PDF gerado em '{pdf_path}'.")

def main():
    """Pipeline da camada Gold."""
    logger.info("Iniciando a camada Gold...")
    try:
        df = fetch_data()
        analysis_results = analyze_data(df)
        heatmap_path = create_heatmap(df)
        graph_paths = create_graphs(analysis_results)
        generate_pdf_report(analysis_results, graph_paths, heatmap_path)
        logger.info("Camada Gold concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro na execução da camada Gold: {e}")

if __name__ == "__main__":
    main()

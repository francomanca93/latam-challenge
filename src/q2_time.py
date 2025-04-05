# imports nativos
from typing import List, Tuple
import time

# imports externas
import duckdb
from rich import print

# imports propios
from utils import init_metrics, print_performance_table, measure_memory
from utils.extract_emoji import extract_emojis

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """Función principal para procesar el contenido de un tweet y obtener los emojis más utilizados.
    Esta funcion utiliza DuckDB para leer el archivo NDJSON y realizar consultas SQL

    Parameters:
        file_path (str): Ruta al archivo NDJSON
    Returns:
        (List[Tuple[str, int]]): Lista de tuplas (emoji, cantidad)
    """
    # Variables para tracking de tiempo y memoria
    time_metrics, mem_metrics = init_metrics()
    
    # Medición inicial de memoria
    mem_metrics['before'] = measure_memory()
    
    result = []
    
    try:
        # Etapa 1: Conexión
        conn = duckdb.connect(database=':memory:')
        mem_metrics['after_conn'] = measure_memory()
        
        # Etapa 2: Lectura del archivo
        time_metrics['read_start'] = time.time()
        read_query = f"""
            create table farmers_protest_raw as 
            select * from read_ndjson_auto('{file_path}')
        """
        conn.execute(read_query)

        time_metrics['read_end'] = time.time()
        mem_metrics['after_read'] = measure_memory()
        
        # Etapa 3: Análisis y consulta
        time_metrics['query_start'] = time.time()
        
        # Registrar la función en DuckDB
        conn.create_function(
            name='extract_emojis',
            function=extract_emojis,
            return_type='VARCHAR[]', 
            parameters=['VARCHAR']
        )
        
        query = """
            with emoji_data as (
                select extract_emojis(renderedContent) as emojis 
                from farmers_protest_raw
                where content is not null
            ),
            unnested AS (
                select unnest(emojis) as emoji_char
                from emoji_data
                where array_length(emojis) > 0
            )
            select 
                emoji_char,
                count(*) as count
            from unnested
            group by emoji_char
            order by count desc
            limit 10
        """
        
        result = conn.execute(query).fetchall()
        final_result = [(row[0], row[1]) for row in result]
        
        time_metrics['query_end'] = time.time()
        mem_metrics['after_query'] = measure_memory()
        
        
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")
        final_result = []
        
    finally:
        # Ante cualquier error, cierro la conexión
        if 'conn' in locals():
            conn.close()
        
        time_metrics['end_total'] = time.time()
        # Imprimir tablas de rendimiento
        print_performance_table(time_metrics, mem_metrics)
        
        return final_result


if __name__ == "__main__":
    
    #Ejecutar la función
    resultados = q2_time("farmers-protest-tweets-2021-2-4.json")

    #Mostrar resultados
    print("\nRESULTADOS TIEMPO:")
    print(resultados)
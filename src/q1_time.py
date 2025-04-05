# imports nativos
from typing import List, Tuple
from datetime import datetime
import time

# imports externas
import duckdb
from rich import print

# imports propios
from utils import init_metrics, print_performance_table, measure_memory


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """Función principal para procesar tweets y obtener el usuario más activo por fecha.
    Esta función utiliza DuckDB para leer el archivo NDJSON y realizar consultas SQL.
    
    Parameters:
        file_path (str): Ruta al archivo NDJSON
    Returns:
        (List[Tuple[datetime.date, str]]): Lista de tuplas (fecha, usuario)
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
        
        query = """
            -- Obtener top 10 fechas con mayor interacción
            with top_dates as (
                select 
                    cast(date AS date) as date_day,
                    count(*) as count
                from farmers_protest_raw
                group by date_day
                order by count desc
                limit 10
            ),

            -- Usuario con mayor interacción por cada fecha top
            top_users AS (
                select 
                    cast(fp.date as date) as date_day,
                    fp.user.username as username,
                    td.count,
                    count(*) as user_count,
                    row_number() over (partition by cast(fp.date as date) order by count(*) desc) AS rank
                from farmers_protest_raw as fp
                join top_dates td
                    on cast(fp.date as date) = td.date_day
                group by cast(fp.date as date), fp.user.username, td.count
            )

            -- Resultado final
            select 
                date_day AS date,
                username,
            from top_users
            where rank = 1
            order by count desc;
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
    resultados = q1_time("farmers-protest-tweets-2021-2-4.json")

    #Mostrar resultados
    print("\nRESULTADOS TIEMPO:")
    print(resultados)
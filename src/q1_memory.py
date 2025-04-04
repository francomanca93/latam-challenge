# imports nativos
from typing import List, Tuple, Generator
from datetime import datetime, date
from collections import defaultdict, Counter
import time

# imports externas
import ujson
from rich import print

# imports propios
from utils import init_metrics, print_performance_table, measure_memory


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """Función principal para procesar tweets y obtener el usuario más activo por fecha.
    Esta función utiliza un enfoque de lectura línea por línea para optimizar el uso de memoria.

    Parameters:
        file_path (str): Ruta al archivo NDJSON
    Returns:
        (List[Tuple[datetime.date, str]]): Lista de tuplas (fecha, usuario)
    """
    # Inicializar métricas
    time_metrics, mem_metrics = init_metrics()
    
    mem_metrics['before'] = measure_memory()
    
    try:
        # Contadores para acumular resultados
        date_user_counter = defaultdict(Counter)
        
        # Etapa 1: Lectura y procesamiento línea por línea
        time_metrics['read_start'] = time.time()
        
        for tweet in process_tweets(file_path):
            tweet_date = datetime.strptime(tweet['date'][:10], '%Y-%m-%d').date() # Extraer fecha y limito el parseo a 10 caracteres
            username = tweet['user']['username']
            date_user_counter[tweet_date][username] += 1
        
        mem_metrics['after_read'] = measure_memory()
        time_metrics['read_end'] = time.time()
        
        # Etapa 2: Procesamiento de resultados
        time_metrics['query_start'] = time.time()
        
        # Obtener top 10 fechas con más tweets
        top_dates = sorted(
            date_user_counter.keys(),
            key=lambda d: sum(date_user_counter[d].values()),
            reverse=True
        )[:10] # Limitar a 10 fechas
        
        # Obtener usuario más activo por cada fecha top
        result = [
            (date, date_user_counter[date].most_common(1)[0][0])
            for date in top_dates
        ]
        
        time_metrics['query_end'] = time.time()
        mem_metrics['after_query'] = measure_memory()
        
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")
        result = []
    
    finally:
        time_metrics['end_total'] = time.time()
        print_performance_table(time_metrics, mem_metrics)
        return result


def process_tweets(file_path: str) -> Generator[Tuple[date, str], None, None]:
    """Generador que lee el archivo línea por línea y extrae fecha y usuario
    para cada tweet. Utiliza ujson para una carga rápida.

    Parameters:
        file_path (str): Ruta al archivo NDJSON
    Yields:
        (date, str): Tuplas (fecha, usuario)
    """
    with open(file_path, encoding='utf-8') as f:
        for line in f:
            try:
                yield ujson.loads(line)  # Cargar el tweet
            except (KeyError, ValueError, ujson.JSONDecodeError):
                continue


if __name__ == "__main__":
    #Ejecutar la función
    resultados = q1_memory("farmers-protest-tweets-2021-2-4.json")

    #Mostrar resultados
    print("\nRESULTADOS TIEMPO:")
    print(resultados)
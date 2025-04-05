# imports nativos
from typing import List, Tuple, Generator
from datetime import date
from collections import Counter
import time

# imports externas
import ujson
from rich import print

# imports propios
from utils import init_metrics, print_performance_table, measure_memory


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """Función principal para procesar el contenido de un tweet y obtener los emojis más utilizados.
    Esta funcion utiliza ujson para una carga rápida y un generador para procesar el archivo línea por línea.
    
    Parameters:
        file_path (str): Ruta al archivo NDJSON
    Returns:
        (List[Tuple[str, int]]): Lista de tuplas (emoji, cantidad)
    """
    # Inicializar métricas
    time_metrics, mem_metrics = init_metrics()
    
    mem_metrics['before'] = measure_memory()
    
    try:
        # Contadores para acumular resultados
        username_counter = Counter()
        
        # Etapa 1: Lectura y procesamiento línea por línea
        time_metrics['read_start'] = time.time()
        
        for tweet in process_tweets(file_path):
            mentioned_users = tweet.get('mentionedUsers')  # Devuelve None si la clave no existe
            
            if not mentioned_users:  # Captura None y listas vacías
                continue
                
            try:
                usernames = [mention['username'] for mention in mentioned_users]
                username_counter.update(usernames)
            except (TypeError, KeyError):
                # TypeError: Si algún mention no es diccionario
                # KeyError: Si algún mention no tiene 'username'
                continue
        
        mem_metrics['after_read'] = measure_memory()
        time_metrics['read_end'] = time.time()
        
        # Etapa 2: Procesamiento de resultados
        time_metrics['query_start'] = time.time()
        
        result = username_counter.most_common(10)
        
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
    resultados = q3_memory("farmers-protest-tweets-2021-2-4.json")

    #Mostrar resultados
    print("\nRESULTADOS MEMORIA:")
    print(resultados)



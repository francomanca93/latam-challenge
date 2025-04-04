# imports nativos
import time

# imports externos
from rich.console import Console
from rich.table import Table
from memory_profiler import memory_usage


def measure_memory() -> float:
    """Función para medir el uso de memoria en el proceso actual
    Returns:
        float: Uso de memoria en MB
    """
    return memory_usage(-1, interval=0.1, timeout=1)[0]


def init_metrics() -> tuple[dict, dict]:
    """Inicializa las métricas de tiempo y memoria para el seguimiento
    Returns:
        tuple: Diccionarios para métricas de tiempo y memoria
    """
    time_metrics = {
        'start_total': time.time(),
        'read_start': 0,
        'read_end': 0,
        'query_start': 0,
        'query_end': 0
    }
    
    mem_metrics = {
        'before': 0,
        'after_conn': 0,
        'after_read': 0,
        'after_query': 0
    }

    return (time_metrics, mem_metrics)


def print_performance_table(time_metrics: dict, mem_metrics: dict):
    """Imprime las métricas en una tabla estilo Rich
    Parameters:
        time_metrics (dict): Diccionario con métricas de tiempo
        mem_metrics (dict): Diccionario con métricas de memoria
    """
    
    # Crear tabla de tiempos
    time_table = Table(title="📊 [bold]Métricas de Tiempo[/bold]", show_header=True, header_style="bold magenta")
    time_table.add_column("Fase", style="cyan")
    time_table.add_column("Duración", justify="right")
    
    time_table.add_row("Conexión y preparación", f"{(time_metrics['read_start'] - time_metrics['start_total']):.4f} segundos")
    time_table.add_row("Lectura del archivo", f"{(time_metrics['read_end'] - time_metrics['read_start']):.4f} segundos")
    time_table.add_row("Análisis y consulta", f"{(time_metrics['query_end'] - time_metrics['query_start']):.4f} segundos")
    time_table.add_row("[bold]TOTAL[/bold]", f"[bold]{(time_metrics['end_total'] - time_metrics['start_total']):.4f} segundos[/bold]")
    
    # Crear tabla de memoria
    mem_table = Table(title="💾 [bold]Métricas de Memoria (MB)[/bold]", show_header=True, header_style="bold blue")
    mem_table.add_column("Fase", style="cyan")
    mem_table.add_column("Valor", justify="right")
    mem_table.add_column("Δ", justify="right")
    
    mem_table.add_row("Memoria base", f"{mem_metrics['before']:.2f}", "")
    mem_table.add_row("Después de conexión", 
                     f"{mem_metrics['after_conn']:.2f}", 
                     f"[green]{mem_metrics['after_conn']-mem_metrics['before']:+.2f}[/green]")
    mem_table.add_row("Después de lectura", 
                     f"{mem_metrics['after_read']:.2f}", 
                     f"[green]{mem_metrics['after_read']-mem_metrics['after_conn']:+.2f}[/green]")
    mem_table.add_row("Después de consulta", 
                     f"{mem_metrics['after_query']:.2f}", 
                     f"[red]{mem_metrics['after_query']-mem_metrics['after_read']:+.2f}[/red]")
    mem_table.add_row("[bold]Consumo máximo[/bold]", 
                     f"[bold]{max(mem_metrics.values()):.2f}[/bold]", "")
    
    # Imprimir tablas
    console = Console()
    console.print(time_table)
    console.print(mem_table)
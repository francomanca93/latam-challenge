from typing import List
import emoji


def extract_emojis(text) -> List[str]:
    """Función para extraer emojis de un texto dado.

    Parameters:
        text (str): Texto del cual extraer emojis.
    Returns:
        List[str]: Lista de emojis extraídos.
    """
    # Usamos emoji.emoji_list() que devuelve información sobre cada emoji encontrado
    emoji_list = emoji.emoji_list(text)
    # Extraemos solo el texto del emoji (caracteres)
    return [e['emoji'] for e in emoji_list]
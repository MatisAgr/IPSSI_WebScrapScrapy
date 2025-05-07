import sys

# Définition des couleurs ANSI
class Colors:
    RESET = '\033[0m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'

def debug_print(message, level="info", end='\n', file=sys.stdout):
    color_map = {
        "info": Colors.BLUE,
        "success": Colors.GREEN,
        "warning": Colors.YELLOW,
        "error": Colors.RED,
        "debug": Colors.MAGENTA,
        "fetch": Colors.CYAN,
    }

    color = color_map.get(level.lower(), Colors.RESET)
    prefix = f"[{level.upper()}] "

    lines = str(message).splitlines()
    if not lines:
        print(f"{color}{prefix}{Colors.RESET}", end=end, file=file)
        return

    print(f"{color}{prefix}{lines[0]}{Colors.RESET}", file=file)
    for line in lines[1:]:
        print(f"{color}{' ' * (len(prefix))}{line}{Colors.RESET}", file=file)

    if end != '\n' and len(lines) > 1:
         print("", end=end, file=file)
    elif end != '\n' and len(lines) == 1:
         pass
    elif end == '\n' and len(lines) > 1:
         pass


if __name__ == '__main__':
    debug_print("Ceci est une information.", level="info")
    debug_print("Opération réussie !", level="success")
    debug_print("Attention, quelque chose d'inattendu.", level="warning")
    debug_print("Une erreur critique est survenue.", level="error")
    debug_print("Variable x = 10", level="debug")
    debug_print("Récupération de la page...", level="fetch")
    debug_print("Message\nsur\nplusieurs lignes.", level="info")
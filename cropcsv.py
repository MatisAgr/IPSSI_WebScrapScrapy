import csv
import os
import argparse
from utils.debug_color import debug_print

def crop_csv(input_file, output_file, max_rows):
    try:
        # Vérifier si le fichier d'entrée existe
        if not os.path.exists(input_file):
            debug_print(f"Le fichier {input_file} n'existe pas!", "error")
            return False
        
        # Compter le nombre total de lignes dans le fichier d'entrée
        with open(input_file, 'r', newline='', encoding='utf-8') as f:
            total_rows = sum(1 for _ in f)
        
        if max_rows >= total_rows:
            debug_print(f"Le fichier contient déjà {total_rows} lignes, ce qui est inférieur ou égal à {max_rows}.", "info")
            debug_print(f"Le fichier ne sera pas modifié.", "info")
            return False
        
        # Ouvrir le fichier d'entrée et créer le fichier de sortie
        with open(input_file, 'r', newline='', encoding='utf-8') as f_in, \
             open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            
            # Copier les max_rows premières lignes
            for i, row in enumerate(reader):
                if i < max_rows:
                    writer.writerow(row)
                else:
                    break
        
        debug_print(f"Fichier réduit créé avec succès: {output_file}", "success")
        debug_print(f"Nombre de lignes réduites de {total_rows} à {max_rows}", "info")
        return True
    
    except Exception as e:
        debug_print(f"Erreur lors de la réduction du fichier: {e}", "error")
        return False

def main():
    # Configurer l'analyse des arguments de ligne de commande
    parser = argparse.ArgumentParser(description='Réduire un fichier CSV à un nombre spécifique de lignes.')
    parser.add_argument('--input', '-i', type=str, default='enterprise.csv',
                        help='Fichier CSV d\'entrée (défaut: enterprise.csv)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Fichier CSV de sortie (défaut: enterprise_cropped.csv)')
    parser.add_argument('--rows', '-r', type=int, required=True,
                        help='Nombre maximum de lignes à conserver')

    args = parser.parse_args()
    
    # Définir le fichier de sortie si non spécifié
    if args.output is None:
        base_name, ext = os.path.splitext(args.input)
        args.output = f"{base_name}_cropped{ext}"
    
    debug_print(f"Réduction du fichier {args.input} à {args.rows} lignes...", "info")
    crop_csv(args.input, args.output, args.rows)

if __name__ == "__main__":
    main()
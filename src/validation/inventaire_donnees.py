from pathlib import Path
import pandas as pd

DOSSIERS = [
    Path("data/brut/transfermarkt"),
    Path("data/brut/football_datasets"),
    Path("data/brut/api"),
]

EXTENSIONS_CSV = {".csv"}
EXTENSIONS_JSON = {".json"}


def taille_fichier_mo(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 2)


def explorer_csv(path: Path) -> tuple[int, int]:
    try:
        df = pd.read_csv(path, nrows=1000)
        nb_colonnes = len(df.columns)
        nb_lignes_total = sum(1 for _ in open(path, "r", encoding="utf-8", errors="ignore")) - 1
        return nb_lignes_total, nb_colonnes
    except Exception as e:
        print(f"[ERREUR CSV] {path}: {e}")
        return -1, -1


def explorer_json(path: Path) -> tuple[int, int]:
    try:
        import json
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "matches" in data:
            nb_lignes = len(data["matches"])
            if nb_lignes > 0 and isinstance(data["matches"][0], dict):
                nb_colonnes = len(data["matches"][0].keys())
            else:
                nb_colonnes = 0
            return nb_lignes, nb_colonnes

        return 0, 0
    except Exception as e:
        print(f"[ERREUR JSON] {path}: {e}")
        return -1, -1


def main() -> None:
    print("\n===== INVENTAIRE DES DONNÉES =====\n")

    for dossier in DOSSIERS:
        print(f"\n--- Dossier : {dossier} ---")

        if not dossier.exists():
            print("Dossier inexistant.")
            continue

        fichiers = sorted([p for p in dossier.rglob("*") if p.is_file()])

        if not fichiers:
            print("Aucun fichier trouvé.")
            continue

        for fichier in fichiers:
            ext = fichier.suffix.lower()
            taille_mo = taille_fichier_mo(fichier)

            if ext in EXTENSIONS_CSV:
                nb_lignes, nb_colonnes = explorer_csv(fichier)
                print(
                    f"{fichier} | type=CSV | taille={taille_mo} Mo | "
                    f"lignes={nb_lignes} | colonnes={nb_colonnes}"
                )

            elif ext in EXTENSIONS_JSON:
                nb_lignes, nb_colonnes = explorer_json(fichier)
                print(
                    f"{fichier} | type=JSON | taille={taille_mo} Mo | "
                    f"enregistrements={nb_lignes} | champs≈{nb_colonnes}"
                )

            else:
                print(f"{fichier} | type={ext or 'inconnu'} | taille={taille_mo} Mo")


if __name__ == "__main__":
    main()
import pickle
import numpy as np

with open("models/model_projection_carriere.pkl", "rb") as f:
    bundle = pickle.load(f)

print("Clés du bundle :", list(bundle.keys()))
print()

# Modèle global (sans segmentation par poste)
model_global = bundle["model_global"]
features = bundle["features"]

importances = model_global.feature_importances_
total = importances.sum()

print(f"Somme brute des importances : {total:.4f}")
print()
print(f"{'Feature':<25} {'Importance (%)':>15}")
print("-" * 42)

# Trié du plus important au moins important
paires = sorted(zip(features, importances), key=lambda x: x[1], reverse=True)
for nom, val in paires:
    pct = (val / total) * 100
    print(f"{nom:<25} {pct:>14.2f}%")
    
print("\n--- Recalcul SANS valeur_moyenne_club ---")
paires_sans_club = [(n, v) for n, v in zip(features, importances) if n != "valeur_moyenne_club"]
total_sans_club = sum(v for _, v in paires_sans_club)

for nom, val in sorted(paires_sans_club, key=lambda x: x[1], reverse=True):
    pct = (val / total_sans_club) * 100
    print(f"{nom:<25} {pct:>14.2f}%")
import psycopg
import pandas as pd
from scipy import stats

conn = psycopg.connect(host='127.0.0.1', port=5433, dbname='football_db', user='football_reader', password='reader_pass_2024')

df = pd.read_sql('''
    SELECT home_goals, away_goals
    FROM marts_clubs.unified_matches
    WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
''', conn)

conn.close()

print(f"Nombre de matchs analysés : {len(df)}")
print(f"Moyenne buts à domicile : {df['home_goals'].mean():.3f}")
print(f"Moyenne buts à l'extérieur : {df['away_goals'].mean():.3f}")
print(f"Écart : {df['home_goals'].mean() - df['away_goals'].mean():.3f}")

# Test t de Student pour échantillons appariés
t_stat, p_value = stats.ttest_rel(df['home_goals'], df['away_goals'])

print(f"\n===== TEST T DE STUDENT APPARIÉ =====")
print(f"Statistique t : {t_stat:.4f}")
print(f"p-value : {p_value:.10f}")

alpha = 0.05
if p_value < alpha:
    print(f"\np-value < {alpha} → On REJETTE H0")
    print("Conclusion : L'avantage du terrain est statistiquement significatif.")
else:
    print(f"\np-value >= {alpha} → On NE REJETTE PAS H0")
    print("Conclusion : Pas de différence significative entre domicile et extérieur.")

# Répartition des résultats
print(f"\n===== RÉPARTITION DES RÉSULTATS =====")
repartition = df.apply(
    lambda row: 'home' if row['home_goals'] > row['away_goals']
    else ('away' if row['away_goals'] > row['home_goals'] else 'draw'),
    axis=1
).value_counts()
print(repartition)
print(f"\nPourcentage victoires domicile : {repartition.get('home', 0) / len(df) * 100:.1f}%")
# Seeds - référentiels versionnés

## `community_areas.csv`

Les 77 *community areas* officielles de la ville de Chicago (clé de jointure
`pickup_community_area` / `dropoff_community_area` du dataset Taxi Trips).

| Colonne | Source |
|---|---|
| `community_area_id`, `name` | City of Chicago - *Boundaries - Community Areas (current)*, dataset SODA `igwz-8jzy` (colonnes `area_numbe`, `community`), noms remis en casse titre. Vérifié le 2026-11-06 via `https://data.cityofchicago.org/resource/igwz-8jzy.csv?$select=area_numbe,community` |
| `side` | Regroupement usuel des community areas en 9 « sides » (Far North, North, Northwest, West, Central, South, Southwest, Far Southwest, Far Southeast), tel que publié par la ville de Chicago / Wikipedia *Community areas in Chicago* |

Chargé en `iceberg.silver.dim_community_area` par la tâche `load_reference_data`
(remplacement complet à chaque exécution ; contrôle : exactement les identifiants 1 à 77).

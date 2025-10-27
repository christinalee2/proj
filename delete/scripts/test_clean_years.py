from datetime import datetime
from database.connection import DatabaseConnection

sample = [
    {'created_at': datetime(2020,1,1), 'last_verified': datetime(2021,6,1)},
    {'created_at': '2022', 'last_verified': '2023'},
    {'created_at': None, 'last_verified': None},
    {'created_at': 2024, 'last_verified': 2025},
    {'created_at': 'notayear', 'last_verified': 'alsobad'}
]

# Convert to DataFrame-like by creating a list of dicts and using the internal helper
# We'll call _to_year via the class by creating a small DataFrame replacement

cleaned = []
for row in sample:
    new_row = {}
    for k, v in row.items():
        if k in ['created_at', 'last_verified']:
            new_row[k] = DatabaseConnection._clean_dataframe_for_insert.__globals__['_to_year'](v)
        else:
            new_row[k] = v
    cleaned.append(new_row)

print('Cleaned rows:')
for r in cleaned:
    print(r)

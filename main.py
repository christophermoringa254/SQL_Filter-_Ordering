"""
SQL Filter, Order, and Group Lab
=================================
Explores advanced SQL queries using three databases:
  - planets.db      : Planets in our solar system
  - dogs.db         : Famous fictional dog characters
  - babe_ruth.db    : Babe Ruth's baseball career statistics
"""

import pandas as pd
import sqlite3


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
conn1 = sqlite3.connect("planets.db")
conn2 = sqlite3.connect("dogs.db")
conn3 = sqlite3.connect("babe_ruth.db")


# ---------------------------------------------------------------------------
# PART 1: Basic Filtering  (planets.db)
# ---------------------------------------------------------------------------

# Preview all planets
all_planets = pd.read_sql("SELECT * FROM planets;", conn1)
print("=== All Planets ===")
print(all_planets, "\n")

# Step 1 – Planets with 0 moons
df_no_moons = pd.read_sql(
    """
    SELECT * FROM planets
    WHERE num_of_moons = 0;
    """,
    conn1,
)
print("=== Step 1: Planets with 0 moons ===")
print(df_no_moons, "\n")

# Step 2 – Name and mass of planets whose name has exactly 7 letters
df_name_seven = pd.read_sql(
    """
    SELECT name, mass FROM planets
    WHERE LENGTH(name) = 7;
    """,
    conn1,
)
print("=== Step 2: Planets with 7-letter names ===")
print(df_name_seven, "\n")


# ---------------------------------------------------------------------------
# PART 2: Advanced Filtering  (planets.db)
# ---------------------------------------------------------------------------

# Step 3 – Name and mass for planets with mass <= 1.00
df_mass = pd.read_sql(
    """
    SELECT name, mass FROM planets
    WHERE mass <= 1.00;
    """,
    conn1,
)
print("=== Step 3: Planets with mass <= 1.00 ===")
print(df_mass, "\n")

# Step 4 – All columns for planets with at least 1 moon AND mass < 1.00
df_mass_moon = pd.read_sql(
    """
    SELECT * FROM planets
    WHERE num_of_moons >= 1
      AND mass < 1.00;
    """,
    conn1,
)
print("=== Step 4: Planets with >= 1 moon and mass < 1.00 ===")
print(df_mass_moon, "\n")

# Step 5 – Name and color of planets whose color contains "blue"
df_blue = pd.read_sql(
    """
    SELECT name, color FROM planets
    WHERE color LIKE '%blue%';
    """,
    conn1,
)
print("=== Step 5: Planets with 'blue' in their color ===")
print(df_blue, "\n")


# ---------------------------------------------------------------------------
# PART 3: Ordering and Limiting  (dogs.db)
# ---------------------------------------------------------------------------

# Preview all dogs
all_dogs = pd.read_sql("SELECT * FROM dogs;", conn2)
print("=== All Dogs ===")
print(all_dogs, "\n")

# Step 6 – Name, age, breed of hungry dogs sorted youngest → oldest
df_hungry = pd.read_sql(
    """
    SELECT name, age, breed FROM dogs
    WHERE hungry = 1
    ORDER BY age ASC;
    """,
    conn2,
)
print("=== Step 6: Hungry dogs (youngest to oldest) ===")
print(df_hungry, "\n")

# Step 7 – Name, age, hungry for hungry dogs aged 2–7, sorted alphabetically
df_hungry_ages = pd.read_sql(
    """
    SELECT name, age, hungry FROM dogs
    WHERE hungry = 1
      AND age BETWEEN 2 AND 7
    ORDER BY name ASC;
    """,
    conn2,
)
print("=== Step 7: Hungry dogs aged 2–7 (alphabetical) ===")
print(df_hungry_ages, "\n")

# Step 8 – Name, age, breed of the 4 oldest dogs, sorted by breed
df_4_oldest = pd.read_sql(
    """
    SELECT name, age, breed FROM dogs
    ORDER BY age DESC
    LIMIT 4;
    """,
    conn2,
)
df_4_oldest = df_4_oldest.sort_values("breed").reset_index(drop=True)
print("=== Step 8: 4 oldest dogs (sorted by breed) ===")
print(df_4_oldest, "\n")


# ---------------------------------------------------------------------------
# PART 4: Aggregation  (babe_ruth.db)
# ---------------------------------------------------------------------------

# Preview all stats
all_ruth = pd.read_sql("SELECT * FROM babe_ruth_stats;", conn3)
print("=== All Babe Ruth Stats ===")
print(all_ruth, "\n")

# Step 9 – Total number of years Babe Ruth played
df_ruth_years = pd.read_sql(
    """
    SELECT COUNT(year) AS total_years
    FROM babe_ruth_stats;
    """,
    conn3,
)
print("=== Step 9: Total years played ===")
print(df_ruth_years, "\n")

# Step 10 – Total career home runs
df_hr_total = pd.read_sql(
    """
    SELECT SUM(HR) AS total_home_runs
    FROM babe_ruth_stats;
    """,
    conn3,
)
print("=== Step 10: Total career home runs ===")
print(df_hr_total, "\n")


# ---------------------------------------------------------------------------
# PART 5: Grouping and Aggregation  (babe_ruth.db)
# ---------------------------------------------------------------------------

# Step 11 – Each team and the number of years Ruth played on it
df_teams_years = pd.read_sql(
    """
    SELECT team, COUNT(year) AS number_years
    FROM babe_ruth_stats
    GROUP BY team;
    """,
    conn3,
)
print("=== Step 11: Years played per team ===")
print(df_teams_years, "\n")

# Step 12 – Teams where Ruth averaged more than 200 at bats
df_at_bats = pd.read_sql(
    """
    SELECT team, AVG(at_bats) AS average_at_bats
    FROM babe_ruth_stats
    GROUP BY team
    HAVING AVG(at_bats) > 200;
    """,
    conn3,
)
print("=== Step 12: Teams with avg at bats > 200 ===")
print(df_at_bats, "\n")


# ---------------------------------------------------------------------------
# Close connections
# ---------------------------------------------------------------------------
conn1.close()
conn2.close()
conn3.close()

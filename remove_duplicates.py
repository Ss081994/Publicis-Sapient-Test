data = [("Amit", 21, "London"),
        ("Cynthia", 28, "Belfast"),
        ("Wendy", 26, "Manchester"),
        ("Gareth", 21, "Cardiff"),
        ("Charles", 29, "Edinburgh")]

unique_data = {}
for name, age, location in data:
    key = (name, age)
    if key not in unique_data:
        unique_data[key] = location

result = [(name, age, location) for (name, age), location in unique_data.items()]
print(result)

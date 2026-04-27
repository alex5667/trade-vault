lines_to_fix = [
    2838,
    2875,
    3006, 3007,
    3010,
    3059,
    3063,
    3108, 3109,
    3158, 3159, 3160,
    3163, 3164,
    3219, 3220, 3221,
    3224, 3225,
    3231, 3232, 3233,
    3236, 3237,
    3290, 3291,
    3334, 3335,
    3338
]

with open("docker-compose-timers.yml", "r") as f:
    lines = f.readlines()

for idx in lines_to_fix:
    i = idx - 1
    # Check if not already ending with \ (ignoring trailing whitespace)
    if not lines[i].rstrip().endswith("\\"):
        lines[i] = lines[i].rstrip() + " \\\n"

with open("docker-compose-timers.yml", "w") as f:
    f.writelines(lines)
print("Done fixing docker-compose-timers.yml")

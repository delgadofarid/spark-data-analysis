import requests
from random import randint, uniform

# Generate Orders
with open("customer-orders.csv", "w") as f:
    for i in range(10000):
        f.write(f"{randint(1, 100)},{randint(1000, 9999)},{uniform(1, 100):.2f}\n")

# Generate Names
# ref: https://larymak.hashnode.dev/build-a-random-name-generator-using-python)
r = requests.get('https://svnweb.freebsd.org/csrg/share/dict/propernames?revision=61766&view=co')
names = r.text.split()
with open("customer-names.csv", "w") as f:
    for i in range(1, 101):
        firstName = names[randint(0, len(names))]
        lastName = names[randint(0, len(names))]
        f.write(f"{i},{firstName} {lastName}\n")
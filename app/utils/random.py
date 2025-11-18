import secrets

def generate_random_string(lowercase_letters=True, uppercase_letters=True, digits=True, length=24) -> str:
    characters = []
    if lowercase_letters:
        characters += list("abcdefghijklmnopqrstuvwxyz")
    if uppercase_letters:
        characters += list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    if digits:
        characters += list("0123456789")

    result = ""
    for i in range(length):
        result += secrets.choice(characters)

    return result

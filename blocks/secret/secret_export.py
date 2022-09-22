import json

from prefect.blocks.system import Secret

from .secret_list import secrets_list


def main():
    secrets_dict = {}
    for secret_name in secrets_list:
        secret = Secret.load(secret_name)
        secrets_dict[secret_name] = secret.get()

    with open("secrets.json", "w") as secrets_file:
        json.dump(secrets_dict, secrets_file, indent=4)


if __name__ == "__main__":
    main()

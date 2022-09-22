import json

from prefect.blocks.system import Secret


def main():
    with open("secrets.json", "r") as secrets_file:
        secrets_dict = json.load(secrets_file)

    for secret_name, secret_value in secrets_dict.items():
        secret_block = Secret(value=secret_value)
        secret_block.save(secret_name, overwrite=True)


if __name__ == "__main__":
    main()

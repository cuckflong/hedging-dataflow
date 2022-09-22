import json

from prefect.blocks.system import String


def main():
    with open("strings.json", "r") as strings_file:
        strings_dict = json.load(strings_file)

    for string_name, string_value in strings_dict.items():
        String(value=string_value).save(string_name, overwrite=True)


if __name__ == "__main__":
    main()

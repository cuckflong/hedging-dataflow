import json

from prefect.blocks.system import String

from .string_list import strings_list


def main():
    strings_dict = {}
    for string_name in strings_list:
        string = String.load(string_name)
        strings_dict[string_name] = string.value

    with open("strings.json", "w") as strings_file:
        json.dump(strings_dict, strings_file, indent=4)


if __name__ == "__main__":
    main()

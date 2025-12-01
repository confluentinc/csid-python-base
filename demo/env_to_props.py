import argparse
import os
from typing import List, Dict


def handle_arguments():
    parser = argparse.ArgumentParser(description="Adds properties from env vars to a template file. Add a prefix "
                                                 "(default = CONNECT_) and use _ to separate elements. "
                                                 "CONNECT_BOOTSTRAP_SERVERS becomes 'bootstrap.servers'")

    parser.add_argument("input", help="Input template file with as-is properties. Properties coming from "
                                      "env vars will be added at the end of the file.")

    parser.add_argument("output", help="Output file")

    parser.add_argument("--prefix",
                        help="Env vars prefix (default= CONNECT_)",
                        required=False, default="CONNECT_")

    return parser.parse_args()


def property_name(name: str, prefix: str) -> str:
    return name[len(prefix):].replace("_", ".").lower()


def extract_prefixed_vars(prefix: str) -> Dict[str, str]:
    result = {}
    for (name, value) in os.environ.items():
        if name.startswith(prefix):
            result[property_name(name, prefix)] = value
    return result


def add_to_template(template_file_name: str, output_file_name: str, properties: Dict[str, str]):
    output_text = ""
    try:
        with open(template_file_name, 'r') as f:
            output_text = f.read()
        output_text += "\n"
    except FileNotFoundError:
        pass

    for k, v in properties.items():
        output_text += f"{k}={v}\n"

    with open(output_file_name, 'w') as f:
        f.write(output_text)


if __name__ == '__main__':
    args = handle_arguments()
    properties = extract_prefixed_vars(args.prefix)
    add_to_template(args.input, args.output, properties)

import argshell
from pathier import Pathier

root = Pathier(__file__).parent


def get_args() -> argshell.Namespace:
    parser = argshell.ArgumentParser()

    parser.add_argument("shellname", help=""" The name for the custom shell. """)
    args = parser.parse_args()

    return args


def create_shell(name: str):
    """Generate a template file in the current working directory for a custom DBShell class.

    `name` will be used to name the generated file as well as several components in the file content.
    """
    custom_file = (Pathier.cwd() / name.replace(" ", "_")).with_suffix(".py")
    if custom_file.exists():
        raise FileExistsError(
            f"Error: {custom_file.name} already exists in this location."
        )
    else:
        variable_name = "_".join(word for word in name.lower().split())
        class_name = "".join(word.capitalize() for word in name.split())
        content = (Pathier(__file__).parent / "customshell.py").read_text()
        content = content.replace("CustomShell", class_name)
        content = content.replace("customshell", variable_name)
        custom_file.write_text(content)


def main(args: argshell.Namespace | None = None):
    if not args:
        args = get_args()
    create_shell(args.shellname)


if __name__ == "__main__":
    main(get_args())

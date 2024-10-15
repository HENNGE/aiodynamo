import sys


def main() -> None:
    if sys.argv[1] == "-x":
        sys.exit(-1)
    else:
        sys.stdout.write(sys.argv[1])


if __name__ == "__main__":
    main()

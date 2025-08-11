import argparse


def run_strategy(strategy: str) -> None:
    """Dispatch function for bot strategies.

    Parameters
    ----------
    strategy: str
        Name of the strategy to run. Currently supports:
        - ``bm``: baseline model placeholder
        - ``turtle``: turtle trading strategy placeholder
        - ``mm``: market maker model placeholder

    This function only prints a message for each strategy and serves as a
    placeholder for future strategy implementations.
    """
    if strategy == "bm":
        print("Running baseline model strategy (placeholder)")
    elif strategy == "turtle":
        print("Running turtle strategy (placeholder)")
    elif strategy == "mm":
        print("Running market maker strategy (placeholder)")
    else:
        raise ValueError(f"Unsupported strategy: {strategy}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Educational SMC bot")
    parser.add_argument(
        "--strategy",
        "-s",
        choices=["bm", "turtle", "mm"],
        default="bm",
        help="Trading strategy to execute",
    )
    args = parser.parse_args()
    run_strategy(args.strategy)


if __name__ == "__main__":
    main()

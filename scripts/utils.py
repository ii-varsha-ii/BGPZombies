from scripts.constants import RRC, RRCs, YEAR


def __validate_constants():
    if RRC and RRC not in RRCs:
        print(f"RRC {RRC} does not exist")
        return 0
    if not YEAR:
        print("Year not provided")
        return 0
    return 1

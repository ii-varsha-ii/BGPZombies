from scripts.utils import __validate_constants
from scripts.zombies import run_zombies_test

if __name__ == '__main__':
    if __validate_constants():
        run_zombies_test()
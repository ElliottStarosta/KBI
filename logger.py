# logger.py
import sys
import time
from colorama import init, Fore, Back, Style
from tqdm import tqdm

# Initialize colorama
init(autoreset=True)

class Logger:
    """
    A colorful logger class with different log levels and progress bars
    """
    
    @staticmethod
    def header(message, width=60):
        """
        Print a formatted header with centered text
        """
        print(f"\n{Back.BLUE}{Fore.WHITE}{'='*width}{Style.RESET_ALL}")
        print(f"{Back.BLUE}{Fore.WHITE}{message.center(width)}{Style.RESET_ALL}")
        print(f"{Back.BLUE}{Fore.WHITE}{'='*width}{Style.RESET_ALL}\n")

    @staticmethod
    def info(message):
        """
        Print an informational message
        """
        print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} {message}")

    @staticmethod
    def success(message):
        """
        Print a success message
        """
        print(f"{Fore.GREEN}[✓ SUCCESS]{Style.RESET_ALL} {message}")

    @staticmethod
    def warning(message):
        """
        Print a warning message
        """
        print(f"{Fore.YELLOW}[! WARNING]{Style.RESET_ALL} {message}")

    @staticmethod
    def error(message):
        """
        Print an error message (to stderr)
        """
        print(f"{Fore.RED}[✗ ERROR]{Style.RESET_ALL} {message}", file=sys.stderr)

    @staticmethod
    def progress(iterable, desc=None, total=None):
        """
        Create and return a progress bar
        """
        return tqdm(
            iterable,
            desc=f"{Fore.MAGENTA}{desc}{Style.RESET_ALL}" if desc else None,
            total=total,
            bar_format="{l_bar}{bar:40}{r_bar}",
            ncols=80
        )

    @staticmethod
    def timed_step(message):
        """
        Create a timed step context manager
        """
        return TimedStep(message)

class TimedStep:
    """
    Context manager for timed operations
    """
    def __init__(self, message):
        self.message = message
        self.start_time = None

    def __enter__(self):
        print(f"{Fore.CYAN}[...]{Style.RESET_ALL} {self.message}", end="", flush=True)
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        status = f"{Fore.GREEN}✓" if exc_type is None else f"{Fore.RED}✗"
        print(f"\r{status}{Style.RESET_ALL} {self.message} {Fore.WHITE}({elapsed:.2f}s){Style.RESET_ALL}")
        return False  # Don't suppress exceptions
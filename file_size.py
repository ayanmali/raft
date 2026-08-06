import os
import sys


def main():
    if len(sys.argv) < 2 or sys.argv[1] != '-f':
        print("""
            Usage:
                python3 file_size.py -f {FILE_PATH}
            """)
        return
    if sys.argv[1] == '-f':
        path = sys.argv[2]
        if not os.path.exists(path):
            print(f"File {path} does not exist")
            return
        size_bytes = os.path.getsize(path)
        print(size_bytes)

if __name__ == "__main__":
    main()

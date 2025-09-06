import os
import sys

# List of folder names to exclude
EXCLUDE_FOLDERS = {".pytest_cache", "__pycache__", ".vscode", ".git", "docs"}

# List of file names or extensions to exclude
EXCLUDE_FILES = {".bak", ".tmp", ".gitignore"}

def print_tree(start_path, prefix=''):
    entries = sorted(os.listdir(start_path))
    # Filter out excluded folders and files
    filtered_entries = [
        e for e in entries
        if not (os.path.isdir(os.path.join(start_path, e)) and e in EXCLUDE_FOLDERS)
        and not (os.path.isfile(os.path.join(start_path, e)) and (e in EXCLUDE_FILES or any(e.endswith(ext) for ext in EXCLUDE_FILES)))
    ]
    
    # Separate folders and files
    folders = [e for e in filtered_entries if os.path.isdir(os.path.join(start_path, e))]
    files = [e for e in filtered_entries if os.path.isfile(os.path.join(start_path, e))]
    
    # Combine folders first, then files
    ordered_entries = folders + files
    entries_count = len(ordered_entries)

    for index, entry in enumerate(ordered_entries):
        path = os.path.join(start_path, entry)
        connector = "└── " if index == entries_count - 1 else "├── "
        print(prefix + connector + entry)

        if os.path.isdir(path):
            extension = "    " if index == entries_count - 1 else "│   "
            print_tree(path, prefix + extension)
            # Add an extra blank line after each directory
            print(prefix + "│")

if __name__ == "__main__":
    # Allow user to specify a path as an argument
    start_path = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    root_name = os.path.basename(os.path.abspath(start_path))
    
    # Extra blank line before root
    print()
    print(root_name)
    # Extra blank line after root
    print("   │")
    
    print_tree(start_path, prefix="   ")
import ast
import os

PROJECT_ROOT = "src"

def extract_functions(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    functions = []
    classes = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            functions.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)

    return classes, functions


for root, _, files in os.walk(PROJECT_ROOT):
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            classes, functions = extract_functions(path)

            print(f"\n📄 {path}")
            if classes:
                print("  Classes:")
                for c in classes:
                    print(f"    - {c}")
            if functions:
                print("  Functions:")
                for f in functions:
                    print(f"    - {f}")

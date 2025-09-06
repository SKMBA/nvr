# # # # import sys
# # # # import os
# # # # import io
# # # # import tokenize
# # # # import ast

# # # # def remove_comments_and_docstrings(input_file, output_file, preserve_newlines=False):
# # # #     # Step 1: Read source
# # # #     with open(input_file, 'r', encoding='utf-8') as f:
# # # #         source = f.read()

# # # #     # Step 2: Find all docstring positions using AST
# # # #     docstring_lines = set()

# # # #     class DocstringVisitor(ast.NodeVisitor):
# # # #         def visit_FunctionDef(self, node):
# # # #             if ast.get_docstring(node):
# # # #                 first_stmt = node.body[0]
# # # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # # #             self.generic_visit(node)

# # # #         def visit_ClassDef(self, node):
# # # #             if ast.get_docstring(node):
# # # #                 first_stmt = node.body[0]
# # # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # # #             self.generic_visit(node)

# # # #         def visit_Module(self, node):
# # # #             if ast.get_docstring(node):
# # # #                 first_stmt = node.body[0]
# # # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # # #             self.generic_visit(node)

# # # #     tree = ast.parse(source)
# # # #     DocstringVisitor().visit(tree)

# # # #     # Step 3: Remove comments and docstrings using tokenize
# # # #     output_tokens = []
# # # #     tokens = tokenize.generate_tokens(io.StringIO(source).readline)
# # # #     for tok_type, tok_string, (srow, scol), (erow, ecol), line in tokens:
# # # #         # Skip comments
# # # #         if tok_type == tokenize.COMMENT:
# # # #             continue
# # # #         # Skip docstrings based on line numbers
# # # #         if tok_type == tokenize.STRING and srow in docstring_lines:
# # # #             continue
# # # #         output_tokens.append((tok_type, tok_string))

# # # #     # Reconstruct code preserving spacing
# # # #     cleaned_code = tokenize.untokenize(output_tokens).decode('utf-8') if isinstance(tokenize.untokenize(output_tokens), bytes) else tokenize.untokenize(output_tokens)

# # # #     # Step 4: Handle empty lines
# # # #     if not preserve_newlines:
# # # #         cleaned_code_lines = [line for line in cleaned_code.splitlines() if line.strip()]
# # # #         cleaned_code = '\n'.join(cleaned_code_lines) + '\n'
# # # #     else:
# # # #         # Collapse multiple empty lines
# # # #         lines = cleaned_code.splitlines()
# # # #         new_lines = []
# # # #         empty_line = False
# # # #         for l in lines:
# # # #             if l.strip() == '':
# # # #                 if not empty_line:
# # # #                     new_lines.append('')
# # # #                     empty_line = True
# # # #             else:
# # # #                 new_lines.append(l)
# # # #                 empty_line = False
# # # #         cleaned_code = '\n'.join(new_lines) + '\n'

# # # #     # Step 5: Add relative path comment at top
# # # #     relative_path = os.path.relpath(input_file)
# # # #     cleaned_code = f"# {relative_path}\n{cleaned_code}"

# # # #     with open(output_file, 'w', encoding='utf-8') as f:
# # # #         f.write(cleaned_code)


# # # # if __name__ == "__main__":
# # # #     if len(sys.argv) < 2:
# # # #         print("Usage: python remove_comments.py <input_file.py> [--preserve-newlines]")
# # # #         sys.exit(1)

# # # #     input_file = sys.argv[1]
# # # #     preserve_newlines = '--preserve-newlines' in sys.argv

# # # #     base, ext = os.path.splitext(input_file)
# # # #     output_file = f"{base}_nocomment{ext}"

# # # #     remove_comments_and_docstrings(input_file, output_file, preserve_newlines)
# # # #     print(f"Comments and docstrings removed. Output saved to {output_file}")
# # # # remove_comments.py
# # # import sys
# # # import os
# # # import io
# # # import tokenize
# # # import ast

# # # def remove_comments_and_docstrings(input_file, output_file, preserve_newlines=False):
# # #     # Step 1: Read source
# # #     with open(input_file, 'r', encoding='utf-8') as f:
# # #         source = f.read()

# # #     # Step 2: Find all docstring positions using AST
# # #     docstring_lines = set()

# # #     class DocstringVisitor(ast.NodeVisitor):
# # #         def visit_FunctionDef(self, node):
# # #             if ast.get_docstring(node):
# # #                 first_stmt = node.body[0]
# # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # #             self.generic_visit(node)

# # #         def visit_ClassDef(self, node):
# # #             if ast.get_docstring(node):
# # #                 first_stmt = node.body[0]
# # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # #             self.generic_visit(node)

# # #         def visit_Module(self, node):
# # #             if ast.get_docstring(node):
# # #                 first_stmt = node.body[0]
# # #                 docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# # #             self.generic_visit(node)

# # #     tree = ast.parse(source)
# # #     DocstringVisitor().visit(tree)

# # #     # Step 3: Find standalone multiline strings (block comments)
# # #     standalone_string_lines = set()
    
# # #     class StringVisitor(ast.NodeVisitor):
# # #         def visit_Expr(self, node):
# # #             # If it's an expression statement containing only a string
# # #             if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
# # #                 # Check if it's not already identified as a docstring
# # #                 if node.lineno not in docstring_lines:
# # #                     standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
# # #             elif isinstance(node.value, ast.Str):  # For older Python versions
# # #                 if node.lineno not in docstring_lines:
# # #                     standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
# # #             self.generic_visit(node)
    
# # #     StringVisitor().visit(tree)

# # #     # Step 4: Remove comments, docstrings, and standalone strings using tokenize
# # #     output_tokens = []
# # #     tokens = tokenize.generate_tokens(io.StringIO(source).readline)
# # #     for tok_type, tok_string, (srow, scol), (erow, ecol), line in tokens:
# # #         # Skip comments
# # #         if tok_type == tokenize.COMMENT:
# # #             continue
# # #         # Skip docstrings and standalone multiline strings based on line numbers
# # #         if tok_type == tokenize.STRING and (srow in docstring_lines or srow in standalone_string_lines):
# # #             continue
# # #         output_tokens.append((tok_type, tok_string))

# # #     # Reconstruct code preserving spacing
# # #     cleaned_code = tokenize.untokenize(output_tokens)
# # #     if isinstance(cleaned_code, bytes):
# # #         cleaned_code = cleaned_code.decode('utf-8')

# # #     # Step 5: Handle empty lines
# # #     if not preserve_newlines:
# # #         cleaned_code_lines = [line for line in cleaned_code.splitlines() if line.strip()]
# # #         cleaned_code = '\n'.join(cleaned_code_lines) + '\n'
# # #     else:
# # #         # Collapse multiple empty lines
# # #         lines = cleaned_code.splitlines()
# # #         new_lines = []
# # #         empty_line = False
# # #         for l in lines:
# # #             if l.strip() == '':
# # #                 if not empty_line:
# # #                     new_lines.append('')
# # #                     empty_line = True
# # #             else:
# # #                 new_lines.append(l)
# # #                 empty_line = False
# # #         cleaned_code = '\n'.join(new_lines) + '\n'

# # #     # Step 6: Add relative path comment at top
# # #     relative_path = os.path.relpath(input_file)
# # #     cleaned_code = f"# {relative_path}\n{cleaned_code}"

# # #     with open(output_file, 'w', encoding='utf-8') as f:
# # #         f.write(cleaned_code)


# # # if __name__ == "__main__":
# # #     if len(sys.argv) < 2:
# # #         print("Usage: python remove_comments.py <input_file.py> [--preserve-newlines]")
# # #         sys.exit(1)

# # #     input_file = sys.argv[1]
# # #     preserve_newlines = '--preserve-newlines' in sys.argv

# # #     base, ext = os.path.splitext(input_file)
# # #     output_file = f"{base}_nocomment{ext}"

# # #     remove_comments_and_docstrings(input_file, output_file, preserve_newlines)
# # #     print(f"Comments and docstrings removed. Output saved to {output_file}")

# # # remove_comments.py
# # import sys
# # import os
# # import io
# # import tokenize
# # import ast
# # import shutil

# # def remove_comments_and_docstrings(input_file, preserve_newlines=False):
# #     # Step 1: Create backup file
# #     backup_file = input_file + '.org'
# #     try:
# #         shutil.copy2(input_file, backup_file)
# #         print(f"Backup created: {backup_file}")
# #     except Exception as e:
# #         print(f"Error creating backup: {e}")
# #         return False

# #     try:
# #         # Step 2: Read source
# #         with open(input_file, 'r', encoding='utf-8') as f:
# #             source = f.read()

# #         # Step 3: Find all docstring positions using AST
# #         docstring_lines = set()

# #         class DocstringVisitor(ast.NodeVisitor):
# #             def visit_FunctionDef(self, node):
# #                 if ast.get_docstring(node):
# #                     first_stmt = node.body[0]
# #                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# #                 self.generic_visit(node)

# #             def visit_ClassDef(self, node):
# #                 if ast.get_docstring(node):
# #                     first_stmt = node.body[0]
# #                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# #                 self.generic_visit(node)

# #             def visit_Module(self, node):
# #                 if ast.get_docstring(node):
# #                     first_stmt = node.body[0]
# #                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
# #                 self.generic_visit(node)

# #         tree = ast.parse(source)
# #         DocstringVisitor().visit(tree)

# #         # Step 4: Find standalone multiline strings (block comments)
# #         standalone_string_lines = set()
        
# #         class StringVisitor(ast.NodeVisitor):
# #             def visit_Expr(self, node):
# #                 # If it's an expression statement containing only a string
# #                 if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
# #                     # Check if it's not already identified as a docstring
# #                     if node.lineno not in docstring_lines:
# #                         standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
# #                 elif isinstance(node.value, ast.Str):  # For older Python versions
# #                     if node.lineno not in docstring_lines:
# #                         standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
# #                 self.generic_visit(node)
        
# #         StringVisitor().visit(tree)

# #         # Step 5: Remove comments, docstrings, and standalone strings using tokenize
# #         output_tokens = []
# #         tokens = tokenize.generate_tokens(io.StringIO(source).readline)
# #         for tok_type, tok_string, (srow, scol), (erow, ecol), line in tokens:
# #             # Skip comments
# #             if tok_type == tokenize.COMMENT:
# #                 continue
# #             # Skip docstrings and standalone multiline strings based on line numbers
# #             if tok_type == tokenize.STRING and (srow in docstring_lines or srow in standalone_string_lines):
# #                 continue
# #             output_tokens.append((tok_type, tok_string))

# #         # Reconstruct code preserving spacing
# #         cleaned_code = tokenize.untokenize(output_tokens)
# #         if isinstance(cleaned_code, bytes):
# #             cleaned_code = cleaned_code.decode('utf-8')

# #         # Step 6: Handle empty lines
# #         if not preserve_newlines:
# #             cleaned_code_lines = [line for line in cleaned_code.splitlines() if line.strip()]
# #             cleaned_code = '\n'.join(cleaned_code_lines) + '\n'
# #         else:
# #             # Collapse multiple empty lines
# #             lines = cleaned_code.splitlines()
# #             new_lines = []
# #             empty_line = False
# #             for l in lines:
# #                 if l.strip() == '':
# #                     if not empty_line:
# #                         new_lines.append('')
# #                         empty_line = True
# #                 else:
# #                     new_lines.append(l)
# #                     empty_line = False
# #             cleaned_code = '\n'.join(new_lines) + '\n'

# #         # Step 7: Add relative path comment at top
# #         relative_path = os.path.relpath(input_file)
# #         cleaned_code = f"# {relative_path}\n{cleaned_code}"

# #         # Step 8: Write back to original file
# #         with open(input_file, 'w', encoding='utf-8') as f:
# #             f.write(cleaned_code)

# #         print(f"Comments and docstrings removed from {input_file}")
# #         print(f"Original file backed up as {backup_file}")
# #         return True

# #     except Exception as e:
# #         print(f"Error processing file: {e}")
# #         # Try to restore from backup if something went wrong
# #         try:
# #             shutil.copy2(backup_file, input_file)
# #             print(f"Restored original file from backup due to error")
# #         except:
# #             print(f"Failed to restore from backup. Manual restoration from {backup_file} may be needed.")
# #         return False


# # def restore_from_backup(input_file):
# #     """Restore original file from backup"""
# #     backup_file = input_file + '.org'
# #     if not os.path.exists(backup_file):
# #         print(f"Backup file {backup_file} not found")
# #         return False
    
# #     try:
# #         shutil.copy2(backup_file, input_file)
# #         print(f"Successfully restored {input_file} from backup")
# #         return True
# #     except Exception as e:
# #         print(f"Error restoring from backup: {e}")
# #         return False


# # if __name__ == "__main__":
# #     if len(sys.argv) < 2:
# #         print("Usage:")
# #         print("  python remove_comments.py <input_file.py> [--preserve-newlines]")
# #         print("  python remove_comments.py <input_file.py> --restore")
# #         print("")
# #         print("Options:")
# #         print("  --preserve-newlines : Keep single empty lines (collapse multiple)")
# #         print("  --restore          : Restore file from .org backup")
# #         sys.exit(1)

# #     input_file = sys.argv[1]
    
# #     # Check if restore option is used
# #     if '--restore' in sys.argv:
# #         restore_from_backup(input_file)
# #         sys.exit(0)
    
# #     # Check if file exists
# #     if not os.path.exists(input_file):
# #         print(f"Error: File {input_file} not found")
# #         sys.exit(1)
    
# #     preserve_newlines = '--preserve-newlines' in sys.argv

# #     success = remove_comments_and_docstrings(input_file, preserve_newlines)
    
# #     if not success:
# #         print("Operation failed. Check error messages above.")
# #         sys.exit(1)

# # remove_comments.py
# import sys
# import os
# import io
# import tokenize
# import ast
# import shutil

# def remove_comments_and_docstrings(input_file, preserve_newlines=False):
#     # Step 1: Create backup file
#     backup_file = input_file + '.org'
#     try:
#         shutil.copy2(input_file, backup_file)
#         print(f"Backup created: {backup_file}")
#     except Exception as e:
#         print(f"Error creating backup: {e}")
#         return False

#     try:
#         # Step 2: Read source
#         with open(input_file, 'r', encoding='utf-8') as f:
#             source = f.read()

#         # Step 3: Find all docstring positions using AST
#         docstring_lines = set()

#         class DocstringVisitor(ast.NodeVisitor):
#             def visit_FunctionDef(self, node):
#                 if ast.get_docstring(node):
#                     first_stmt = node.body[0]
#                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
#                 self.generic_visit(node)

#             def visit_ClassDef(self, node):
#                 if ast.get_docstring(node):
#                     first_stmt = node.body[0]
#                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
#                 self.generic_visit(node)

#             def visit_Module(self, node):
#                 if ast.get_docstring(node):
#                     first_stmt = node.body[0]
#                     docstring_lines.update(range(first_stmt.lineno, first_stmt.end_lineno + 1))
#                 self.generic_visit(node)

#         tree = ast.parse(source)
#         DocstringVisitor().visit(tree)

#         # Step 4: Find standalone multiline strings (block comments)
#         standalone_string_lines = set()
        
#         class StringVisitor(ast.NodeVisitor):
#             def visit_Expr(self, node):
#                 # If it's an expression statement containing only a string
#                 if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
#                     # Check if it's not already identified as a docstring
#                     if node.lineno not in docstring_lines:
#                         standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
#                 elif isinstance(node.value, ast.Str):  # For older Python versions
#                     if node.lineno not in docstring_lines:
#                         standalone_string_lines.update(range(node.lineno, node.end_lineno + 1))
#                 self.generic_visit(node)
        
#         StringVisitor().visit(tree)

#         # Step 5: Remove comments, docstrings, and standalone strings using tokenize
#         output_tokens = []
#         tokens = tokenize.generate_tokens(io.StringIO(source).readline)
#         for tok_type, tok_string, (srow, scol), (erow, ecol), line in tokens:
#             # Skip comments
#             if tok_type == tokenize.COMMENT:
#                 continue
#             # Skip docstrings and standalone multiline strings based on line numbers
#             if tok_type == tokenize.STRING and (srow in docstring_lines or srow in standalone_string_lines):
#                 continue
#             output_tokens.append((tok_type, tok_string))

#         # Reconstruct code preserving spacing
#         cleaned_code = tokenize.untokenize(output_tokens)
#         if isinstance(cleaned_code, bytes):
#             cleaned_code = cleaned_code.decode('utf-8')

#         # Step 6: Handle empty lines
#         if not preserve_newlines:
#             cleaned_code_lines = [line for line in cleaned_code.splitlines() if line.strip()]
#             cleaned_code = '\n'.join(cleaned_code_lines) + '\n'
#         else:
#             # Collapse multiple empty lines
#             lines = cleaned_code.splitlines()
#             new_lines = []
#             empty_line = False
#             for l in lines:
#                 if l.strip() == '':
#                     if not empty_line:
#                         new_lines.append('')
#                         empty_line = True
#                 else:
#                     new_lines.append(l)
#                     empty_line = False
#             cleaned_code = '\n'.join(new_lines) + '\n'

#         # Step 7: Add relative path comment at top
#         relative_path = os.path.relpath(input_file)
#         cleaned_code = f"# {relative_path}\n{cleaned_code}"

#         # Step 8: Write back to original file
#         with open(input_file, 'w', encoding='utf-8') as f:
#             f.write(cleaned_code)

#         print(f"Comments and docstrings removed from {input_file}")
#         print(f"Original file backed up as {backup_file}")
#         return True

#     except Exception as e:
#         print(f"Error processing file: {e}")
#         # Try to restore from backup if something went wrong
#         try:
#             shutil.copy2(backup_file, input_file)
#             print(f"Restored original file from backup due to error")
#         except:
#             print(f"Failed to restore from backup. Manual restoration from {backup_file} may be needed.")
#         return False


# def find_python_files(directory, recursive=False):
#     """Find all Python files in directory"""
#     python_files = []
    
#     if recursive:
#         # Use os.walk for recursive search
#         for root, dirs, files in os.walk(directory):
#             for file in files:
#                 if file.endswith('.py') and not file.endswith('.org'):
#                     python_files.append(os.path.join(root, file))
#     else:
#         # List files in current directory only
#         try:
#             files = os.listdir(directory)
#             for file in files:
#                 if file.endswith('.py') and not file.endswith('.org'):
#                     full_path = os.path.join(directory, file)
#                     if os.path.isfile(full_path):
#                         python_files.append(full_path)
#         except Exception as e:
#             print(f"Error reading directory {directory}: {e}")
#             return []
    
#     return sorted(python_files)


# def process_multiple_files(files, preserve_newlines=False):
#     """Process multiple Python files"""
#     total_files = len(files)
#     success_count = 0
    
#     print(f"Found {total_files} Python file(s) to process:")
#     for file in files:
#         print(f"  - {file}")
#     print()
    
#     for i, file in enumerate(files, 1):
#         print(f"[{i}/{total_files}] Processing: {file}")
        
#         # Skip backup files
#         if file.endswith('.org'):
#             print("  Skipping backup file")
#             continue
            
#         if remove_comments_and_docstrings(file, preserve_newlines):
#             success_count += 1
#         print()
    
#     print(f"Summary: {success_count}/{total_files} files processed successfully")
#     return success_count == total_files


# def restore_from_backup(input_file):
#     """Restore original file from backup"""
#     backup_file = input_file + '.org'
#     if not os.path.exists(backup_file):
#         print(f"Backup file {backup_file} not found")
#         return False
    
#     try:
#         shutil.copy2(backup_file, input_file)
#         print(f"Successfully restored {input_file} from backup")
#         return True
#     except Exception as e:
#         print(f"Error restoring from backup: {e}")
#         return False


# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("Usage:")
#         print("  python remove_comments.py <input_file.py> [--preserve-newlines]")
#         print("  python remove_comments.py <input_file.py> --restore")
#         print("  python remove_comments.py -d [--preserve-newlines]")
#         print("  python remove_comments.py -r [--preserve-newlines]")
#         print("")
#         print("Options:")
#         print("  <input_file.py>    : Process single file")
#         print("  -d                 : Process all .py files in current directory")
#         print("  -r                 : Process all .py files recursively")
#         print("  --preserve-newlines: Keep single empty lines (collapse multiple)")
#         print("  --restore          : Restore file from .org backup (single file only)")
#         sys.exit(1)

#     first_arg = sys.argv[1]
#     preserve_newlines = '--preserve-newlines' in sys.argv
    
#     # Handle directory processing
#     if first_arg == '-d':
#         current_dir = '.'
#         python_files = find_python_files(current_dir, recursive=False)
        
#         if not python_files:
#             print("No Python files found in current directory")
#             sys.exit(0)
            
#         success = process_multiple_files(python_files, preserve_newlines)
#         sys.exit(0 if success else 1)
    
#     # Handle recursive processing
#     elif first_arg == '-r':
#         current_dir = '.'
#         python_files = find_python_files(current_dir, recursive=True)
        
#         if not python_files:
#             print("No Python files found in current directory and subdirectories")
#             sys.exit(0)
            
#         success = process_multiple_files(python_files, preserve_newlines)
#         sys.exit(0 if success else 1)
    
#     # Handle single file processing
#     else:
#         input_file = first_arg
        
#         # Check if restore option is used
#         if '--restore' in sys.argv:
#             restore_from_backup(input_file)
#             sys.exit(0)
        
#         # Check if file exists
#         if not os.path.exists(input_file):
#             print(f"Error: File {input_file} not found")
#             sys.exit(1)
        
#         success = remove_comments_and_docstrings(input_file, preserve_newlines)
        
#         if not success:
#             print("Operation failed. Check error messages above.")
#             sys.exit(1)

# remove_comments.py
import sys 
import os 
import io 
import tokenize 
import ast 
import shutil 
def remove_comments_and_docstrings (input_file ,preserve_newlines =False ):
    # Step 1: Create backup file
    backup_file =input_file +'.org'
    try :
        shutil .copy2 (input_file ,backup_file )
        print (f"Backup created: {backup_file }")
    except Exception as e :
        print (f"Error creating backup: {e }")
        return False 
    try :
        # Step 2: Read source
        with open (input_file ,'r',encoding ='utf-8')as f :
            source =f .read ()

        # Step 3: Find all docstring positions using AST
        docstring_lines =set ()
        class DocstringVisitor (ast .NodeVisitor ):
            def visit_FunctionDef (self ,node ):
                if ast .get_docstring (node ):
                    first_stmt =node .body [0 ]
                    docstring_lines .update (range (first_stmt .lineno ,first_stmt .end_lineno +1 ))
                self .generic_visit (node )
            def visit_ClassDef (self ,node ):
                if ast .get_docstring (node ):
                    first_stmt =node .body [0 ]
                    docstring_lines .update (range (first_stmt .lineno ,first_stmt .end_lineno +1 ))
                self .generic_visit (node )
            def visit_Module (self ,node ):
                if ast .get_docstring (node ):
                    first_stmt =node .body [0 ]
                    docstring_lines .update (range (first_stmt .lineno ,first_stmt .end_lineno +1 ))
                self .generic_visit (node )
        tree =ast .parse (source )
        DocstringVisitor ().visit (tree )

        # Step 4: Find standalone multiline strings (block comments)
        standalone_string_lines =set ()
        
        class StringVisitor (ast .NodeVisitor ):
            def visit_Expr (self ,node ):
                # If it's an expression statement containing only a string
                if isinstance (node .value ,ast .Constant )and isinstance (node .value .value ,str ):
                    # Check if it's not already identified as a docstring
                    if node .lineno not in docstring_lines :
                        standalone_string_lines .update (range (node .lineno ,node .end_lineno +1 ))
                elif isinstance(node.value, ast.Str):  # For older Python versions
                    if node .lineno not in docstring_lines :
                        standalone_string_lines .update (range (node .lineno ,node .end_lineno +1 ))
                self .generic_visit (node )
        
        StringVisitor ().visit (tree )

        # Step 5: Remove comments, docstrings, and standalone strings using tokenize
        output_tokens =[]
        tokens =tokenize .generate_tokens (io .StringIO (source ).readline )
        for tok_type ,tok_string ,(srow ,scol ),(erow ,ecol ),line in tokens :
            # Skip comments
            if tok_type ==tokenize .COMMENT :
                continue 
            # Skip docstrings and standalone multiline strings based on line numbers
            if tok_type ==tokenize .STRING and (srow in docstring_lines or srow in standalone_string_lines ):
                continue 
            output_tokens .append ((tok_type ,tok_string ))

        # Reconstruct code preserving spacing
        cleaned_code =tokenize .untokenize (output_tokens )
        if isinstance (cleaned_code ,bytes ):
            cleaned_code =cleaned_code .decode ('utf-8')

        # Step 6: Handle empty lines
        if not preserve_newlines :
            cleaned_code_lines =[line for line in cleaned_code .splitlines ()if line .strip ()]
            cleaned_code ='\n'.join (cleaned_code_lines )+'\n'
        else :
            # Collapse multiple empty lines
            lines =cleaned_code .splitlines ()
            new_lines =[]
            empty_line =False 
            for l in lines :
                if l .strip ()=='':
                    if not empty_line :
                        new_lines .append ('')
                        empty_line =True 
                else :
                    new_lines .append (l )
                    empty_line =False 
            cleaned_code ='\n'.join (new_lines )+'\n'

        # Step 7: Add relative path comment at top
        relative_path =os .path .relpath (input_file )
        cleaned_code =f"# {relative_path }\n{cleaned_code }"

        # Step 8: Write back to original file
        with open (input_file ,'w',encoding ='utf-8')as f :
            f .write (cleaned_code )
        print (f"Comments and docstrings removed from {input_file }")
        print (f"Original file backed up as {backup_file }")
        return True 
    except Exception as e :
        print (f"Error processing file: {e }")
        # Try to restore from backup if something went wrong
        try :
            shutil .copy2 (backup_file ,input_file )
            print (f"Restored original file from backup due to error")
        except :
            print (f"Failed to restore from backup. Manual restoration from {backup_file } may be needed.")
        return False 
def find_python_files (directory ,recursive =False ):
    """Find all Python files in directory"""
    python_files =[]
    
    if recursive :
        # Use os.walk for recursive search
        for root ,dirs ,files in os .walk (directory ):
            for file in files :
                if file .endswith ('.py')and not file .endswith ('.org'):
                    python_files .append (os .path .join (root ,file ))
    else :
        # List files in current directory only
        try :
            files =os .listdir (directory )
            for file in files :
                if file .endswith ('.py')and not file .endswith ('.org'):
                    full_path =os .path .join (directory ,file )
                    if os .path .isfile (full_path ):
                        python_files .append (full_path )
        except Exception as e :
            print (f"Error reading directory {directory }: {e }")
            return []
    
    return sorted (python_files )
def process_multiple_files (files ,preserve_newlines =False ):
    """Process multiple Python files"""
    total_files =len (files )
    success_count =0 
    
    print (f"Found {total_files } Python file(s) to process:")
    for file in files :
        print (f"  - {file }")
    print ()
    
    for i ,file in enumerate (files ,1 ):
        print (f"[{i }/{total_files }] Processing: {file }")
        
        # Skip backup files
        if file .endswith ('.org'):
            print ("  Skipping backup file")
            continue 
            
        if remove_comments_and_docstrings (file ,preserve_newlines ):
            success_count +=1 
        print ()
    
    print (f"Summary: {success_count }/{total_files } files processed successfully")
    return success_count ==total_files 
def restore_from_backup (input_file ):
    """Restore original file from backup"""
    backup_file =input_file +'.org'
    if not os .path .exists (backup_file ):
        print (f"Backup file {backup_file } not found")
        return False 
    
    try :
        shutil .copy2 (backup_file ,input_file )
        print (f"Successfully restored {input_file } from backup")
        return True 
    except Exception as e :
        print (f"Error restoring from backup: {e }")
        return False 
if __name__ =="__main__":
    if len (sys .argv )<2 :
        print ("Usage:")
        print ("  python remove_comments.py <input_file.py> [--preserve-newlines]")
        print ("  python remove_comments.py <input_file.py> --restore")
        print ("  python remove_comments.py -d [--preserve-newlines]")
        print ("  python remove_comments.py -r [--preserve-newlines]")
        print ("")
        print ("Options:")
        print ("  <input_file.py>    : Process single file")
        print ("  -d                 : Process all .py files in current directory")
        print ("  -r                 : Process all .py files recursively")
        print ("  --preserve-newlines: Keep single empty lines (collapse multiple)")
        print ("  --restore          : Restore file from .org backup (single file only)")
        sys .exit (1 )
    first_arg =sys .argv [1 ]
    preserve_newlines ='--preserve-newlines'in sys .argv 
    
    # Handle directory processing
    if first_arg =='-d':
        current_dir ='.'
        python_files =find_python_files (current_dir ,recursive =False )
        
        if not python_files :
            print ("No Python files found in current directory")
            sys .exit (0 )
            
        success =process_multiple_files (python_files ,preserve_newlines )
        sys .exit (0 if success else 1 )
    
    # Handle recursive processing
    elif first_arg =='-r':
        current_dir ='.'
        python_files =find_python_files (current_dir ,recursive =True )
        
        if not python_files :
            print ("No Python files found in current directory and subdirectories")
            sys .exit (0 )
            
        success =process_multiple_files (python_files ,preserve_newlines )
        sys .exit (0 if success else 1 )
    
    # Handle single file processing
    else :
        input_file =first_arg 
        
        # Check if restore option is used
        if '--restore'in sys .argv :
            restore_from_backup (input_file )
            sys .exit (0 )
        
        # Check if file exists
        if not os .path .exists (input_file ):
            print (f"Error: File {input_file } not found")
            sys .exit (1 )
        
        success =remove_comments_and_docstrings (input_file ,preserve_newlines )
        
        if not success :
            print ("Operation failed. Check error messages above.")
            sys .exit (1 )
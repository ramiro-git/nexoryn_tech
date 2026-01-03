
import sys
import os
from pathlib import Path
import flet as ft

# Ensure project root is in path (running from scripts/)
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from desktop_app.ui_basic import main

if __name__ == "__main__":
    print("Starting web app on port 8550...")
    ft.app(target=main, port=8550, view=ft.AppView.WEB_BROWSER)

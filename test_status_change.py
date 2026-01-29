import flet as ft
from desktop_app.ui_basic import _status_pill

def test_status_logic():
    print("Testing _status_pill logic...")

    # Case 1: CONFIRMADO, no CAE -> Should be CONFIRMADO
    ctrl = _status_pill("CONFIRMADO", {"cae": None})
    val = ctrl.content.value
    print(f"Case 1 (CONFIRMADO, no CAE): {'PASS' if val == 'CONFIRMADO' else 'FAIL'} - Got: {val}")

    # Case 2: CONFIRMADO, with CAE -> Should be FACTURADO
    ctrl = _status_pill("CONFIRMADO", {"cae": "12345678901234"})
    val = ctrl.content.value
    print(f"Case 2 (CONFIRMADO, with CAE): {'PASS' if val == 'FACTURADO' else 'FAIL'} - Got: {val}")

    # Case 3: PAGADO, with CAE -> Should keep PAGADO (as per my logic, only CONFIRMADO changes? Or should PAGADO also show FACTURADO?
    # User request: "cuando exista el cae en la base de datos, no diga 'CONFIRMADO' sino 'FACTURADO'"
    # This implies specifically replacing CONFIRMADO. PAGADO implies money received, which is a different state usually.
    # Let's check what I implemented.
    ctrl = _status_pill("PAGADO", {"cae": "12345678901234"})
    val = ctrl.content.value
    print(f"Case 3 (PAGADO, with CAE): {'PASS' if val == 'PAGADO' else 'FAIL'} - Got: {val}")

if __name__ == "__main__":
    test_status_logic()

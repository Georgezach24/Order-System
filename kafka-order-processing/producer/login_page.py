import hashlib
import tkinter as tk
from tkinter import messagebox
import threading
import requests
import json
from kafka import KafkaConsumer

current_user = None


def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

# === Kafka listener for login responses ===
def kafka_listener():
    consumer = KafkaConsumer(
        'login_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id='gui-login-listener',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        if data['username'] == current_user:
            if data['status'] == 'success':
                root.after(0, lambda: handle_login_success(current_user))
                break
            else:
                root.after(0, lambda: messagebox.showerror("Login Failed", "Invalid credentials"))
                break

# === Called on login success ===
def handle_login_success(username):
    messagebox.showinfo("Success", f"Welcome {username}!")
    root.withdraw()
    open_main_app(username)

# === Send login request to Flask API ===
def login():
    global current_user
    current_user = entry_user.get()
    password = entry_pass.get()
    try:
        res = requests.post("http://localhost:5000/login", json={
            "username": current_user,
            "password": hash_password(password)
        })
        if res.status_code == 200:
            messagebox.showinfo("Login", "Login request sent.\nWaiting for response...")
        else:
            messagebox.showerror("Error", "Login request failed.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# === Dashboard GUI ===
def open_main_app(username):
    dashboard = tk.Toplevel()
    dashboard.title("Order System")
    dashboard.geometry("300x250")

    tk.Label(dashboard, text=f"Welcome, {username}", font=("Arial", 14)).pack(pady=10)

    tk.Button(dashboard, text="Create Order", width=25, command=create_order_window).pack(pady=5)
    tk.Button(dashboard, text="Update Order", width=25, command=update_order_window).pack(pady=5)
    tk.Button(dashboard, text="Cancel Order", width=25, command=cancel_order_window).pack(pady=5)

# === Placeholder Order Windows ===
def create_order_window():
    win = tk.Toplevel()
    win.title("Create Order")
    tk.Label(win, text="Create Order Form Here").pack(pady=10)

def update_order_window():
    win = tk.Toplevel()
    win.title("Update Order")
    tk.Label(win, text="Update Order Form Here").pack(pady=10)

def cancel_order_window():
    win = tk.Toplevel()
    win.title("Cancel Order")
    tk.Label(win, text="Cancel Order Form Here").pack(pady=10)

# === Login Window ===
root = tk.Tk()
root.title("Login")
root.geometry("300x200")

tk.Label(root, text="Username").pack()
entry_user = tk.Entry(root)
entry_user.pack()

tk.Label(root, text="Password").pack()
entry_pass = tk.Entry(root, show='*')
entry_pass.pack()

tk.Button(root, text="Login", command=login).pack(pady=10)

# === Start Kafka listener in background ===
threading.Thread(target=kafka_listener, daemon=True).start()

# === Start GUI ===
root.mainloop()

import tkinter as tk
from tkinter import messagebox
import requests
import json
from kafka import KafkaConsumer
import threading

current_user = None

def login():
    global current_user
    current_user = entry_user.get()
    password = entry_pass.get()

    try:
        res = requests.post("http://localhost:5000/login", json={
            "username": current_user,
            "password": password
        })
        if res.status_code == 200:
            messagebox.showinfo("Login", "Waiting for login response...")
        else:
            messagebox.showerror("Error", "Login failed to send.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

def kafka_listener():
    consumer = KafkaConsumer(
        'login_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id='gui',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        if data['username'] == current_user:
            if data['status'] == 'success':
                messagebox.showinfo("Success", f"Welcome {current_user}")
                break
            else:
                messagebox.showerror("Login Failed", "Invalid credentials")
                break

threading.Thread(target=kafka_listener, daemon=True).start()

# GUI setup
root = tk.Tk()
root.title("Login")
tk.Label(root, text="Username:").pack()
entry_user = tk.Entry(root)
entry_user.pack()
tk.Label(root, text="Password:").pack()
entry_pass = tk.Entry(root, show='*')
entry_pass.pack()
tk.Button(root, text="Login", command=login).pack()
root.mainloop()

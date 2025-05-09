# --- Import necessary modules ---
import hashlib
import tkinter as tk
from tkinter import messagebox
import threading
import requests
import json
import datetime
import random
from kafka import KafkaProducer, KafkaConsumer

# --- Function to hash passwords before sending them to the server ---
def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

# --- Global variable to keep track of the current logged-in user ---
current_user = None

# --- Kafka producer for sending order data ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Background thread: listens for login responses from Kafka ---
def kafka_login_listener():
    consumer = KafkaConsumer(
        'login_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id='gui-login-listener',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        # Check if the response is for the currently logged-in user
        if data['username'] == current_user:
            if data['status'] == 'success':
                # Schedule login success handler in GUI thread
                root.after(0, lambda: handle_login_success(current_user))
                break
            else:
                # Show login failure message
                root.after(0, lambda: messagebox.showerror("Login Failed", "Invalid credentials"))
                break

# --- Callback for successful login: opens the main app window ---
def handle_login_success(username):
    messagebox.showinfo("Success", f"Welcome {username}!")
    root.withdraw()  # Hide login window
    open_main_app(username)  # Open dashboard

# --- Triggered when user clicks Login button ---
def login():
    global current_user
    current_user = entry_user.get()
    password = entry_pass.get()
    try:
        # Send login request to Flask API (which pushes to Kafka)
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

# --- Window to enter and submit a new order ---
def create_order_window():
    win = tk.Toplevel()
    win.title("Create Order")

    tk.Label(win, text="Order Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

    # Called when the user clicks "Submit Order"
    def submit_order():
        description = desc_entry.get()
        now = datetime.datetime.now()
        order = {
            "user": current_user,
            "order_id": random.randint(1000, 9999),
            "dt": now.strftime("%Y-%m-%d"),
            "tm": now.strftime("%H:%M:%S"),
            "description": description
        }
        # Send order to Kafka topic "orders"
        producer.send('orders', order)
        messagebox.showinfo("Success", "Order sent to Kafka.")
        win.destroy()

    tk.Button(win, text="Submit Order", command=submit_order).pack(pady=10)

# --- Dashboard window shown after login ---
def open_main_app(username):
    dash = tk.Toplevel()
    dash.title("Order Dashboard")
    dash.geometry("300x250")

    tk.Label(dash, text=f"Logged in as: {username}", font=("Arial", 14)).pack(pady=10)

    # Buttons for order operations
    tk.Button(dash, text="Create Order", width=25, command=create_order_window).pack(pady=5)
    tk.Button(dash, text="Update Order", width=25, command=lambda: messagebox.showinfo("Coming Soon", "Update feature coming")).pack(pady=5)
    tk.Button(dash, text="Cancel Order", width=25, command=lambda: messagebox.showinfo("Coming Soon", "Cancel feature coming")).pack(pady=5)

# --- Login window setup (main GUI entry point) ---
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

# --- Start background Kafka listener for login response ---
threading.Thread(target=kafka_login_listener, daemon=True).start()

# --- Start the Tkinter event loop ---
root.mainloop()

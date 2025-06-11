import hashlib
import tkinter as tk
from tkinter import messagebox
import threading
import requests
import json
import datetime
import random
import uuid
from kafka import KafkaProducer, KafkaConsumer
import queue

# --- Global variables ---
current_user = None
current_user_is_admin = False
order_response_queue = queue.Queue()
session_id = str(uuid.uuid4())  # Unique ID to identify this session with Kafka

# Hash a password using SHA-256
def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

# Initialize Kafka producer for sending messages
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Listen for login responses on Kafka and handle them in the GUI
def kafka_login_listener():
    consumer = KafkaConsumer(
        'login_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id=f'gui-login-listener-{session_id}',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        if data.get('session_id') == session_id:
            if data['status'] == 'success':
                root.after(0, lambda: handle_login_success(data['username'], data.get('isAdmin', False)))
            else:
                root.after(0, lambda: messagebox.showerror("Login Failed", "Invalid credentials"))

# Listen for order responses and queue them for display
def kafka_order_response_listener():
    consumer = KafkaConsumer(
        'order_query_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id=f'order-query-listener-{session_id}',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        order_response_queue.put(data['orders'])

# Handle successful login, store user info, and open main app window
def handle_login_success(username, is_admin):
    global current_user, current_user_is_admin
    current_user = username
    current_user_is_admin = is_admin
    messagebox.showinfo("Success", f"Welcome {username}! (Admin: {is_admin})")
    root.withdraw()
    open_main_app(username)

# Send login request to backend
def login():
    global current_user
    current_user = entry_user.get()
    password = entry_pass.get()
    try:
        res = requests.post("http://localhost:5000/login", json={
            "username": current_user,
            "password": hash_password(password),
            "session_id": session_id
        })
        if res.status_code == 200:
            print("Login request sent.")
        else:
            print("Login request failed.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# GUI window for creating a new order
def create_order_window():
    win = tk.Toplevel()
    win.title("Create Order")

    # UI fields for order input
    user_label = tk.Label(win, text="Username (optional for admin)")
    user_label.pack()
    user_entry = tk.Entry(win, width=30)
    user_entry.insert(0, current_user)
    user_entry.pack()

    tk.Label(win, text="Order Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

    # Submit the order to backend
    def submit_order():
        username = user_entry.get() if current_user_is_admin else current_user
        description = desc_entry.get()
        now = datetime.datetime.now()
        order = {
            "user": username,
            "order_id": random.randint(1000, 9999),
            "dt": now.strftime("%Y-%m-%d"),
            "tm": now.strftime("%H:%M:%S"),
            "description": description
        }
        try:
            res = requests.post("http://localhost:5000/create-order", json=order)
            if res.status_code == 200:
                print(f"Order #{order['order_id']} created for {username}")
            else:
                print("Order creation failed.")
        except Exception as e:
            messagebox.showerror("Error", str(e))
        win.destroy()

    tk.Button(win, text="Submit Order", command=submit_order).pack(pady=10)

# GUI for updating an existing order
def update_order_window():
    win = tk.Toplevel()
    win.title("Update Order")

    # UI for selecting order and new description
    tk.Label(win, text="Order ID to Update").pack()
    order_id_entry = tk.Entry(win, width=30)
    order_id_entry.pack()

    user_label = tk.Label(win, text="Username (optional for admin)")
    user_label.pack()
    user_entry = tk.Entry(win, width=30)
    user_entry.insert(0, current_user)
    user_entry.pack()

    tk.Label(win, text="New Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

    # Submit update request
    def submit_update():
        try:
            order_id = int(order_id_entry.get())
        except ValueError:
            messagebox.showerror("Invalid Input", "Order ID must be a number.")
            return

        username = user_entry.get() if current_user_is_admin else current_user
        description = desc_entry.get()
        now = datetime.datetime.now()

        order_update = {
            "user": username,
            "order_id": order_id,
            "dt": now.strftime("%Y-%m-%d"),
            "tm": now.strftime("%H:%M:%S"),
            "description": description
        }

        try:
            res = requests.post("http://localhost:5000/update-order", json=order_update)
            if res.status_code == 200:
                print(f"Update sent for order #{order_id} by {username}")
            else:
                print("Update failed.")
        except Exception as e:
            messagebox.showerror("Error", str(e))
        win.destroy()

    tk.Button(win, text="Submit Update", command=submit_update).pack(pady=10)

# GUI for canceling an order
def cancel_order_window():
    win = tk.Toplevel()
    win.title("Cancel Order")

    # UI for entering order ID
    tk.Label(win, text="Order ID to Cancel").pack()
    order_id_entry = tk.Entry(win, width=30)
    order_id_entry.pack()

    user_label = tk.Label(win, text="Username (optional for admin)")
    user_label.pack()
    user_entry = tk.Entry(win, width=30)
    user_entry.insert(0, current_user)
    user_entry.pack()

    # Submit cancellation request
    def submit_cancel():
        try:
            order_id = int(order_id_entry.get())
        except ValueError:
            messagebox.showerror("Invalid Input", "Order ID must be a number.")
            return

        username = user_entry.get() if current_user_is_admin else current_user

        cancel_data = {
            "user": username,
            "order_id": order_id
        }

        try:
            res = requests.post("http://localhost:5000/cancel-order", json=cancel_data)
            if res.status_code == 200:
                print(f"Cancellation requested for order #{order_id} by {username}")
            else:
                print("Cancellation failed.")
        except Exception as e:
            messagebox.showerror("Error", str(e))
        win.destroy()

    tk.Button(win, text="Submit Cancellation", command=submit_cancel).pack(pady=10)

# Request order list from backend
def view_my_orders():
    try:
        request_data = {
            "user": "*" if current_user_is_admin else current_user,
            "isAdmin": current_user_is_admin
        }
        res = requests.post("http://localhost:5000/view_orders", json=request_data)
        if res.status_code == 200:
            print("Order fetch requested.")
        else:
            print("Failed to request orders.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# GUI for user registration (admin-only)
def register_user_window():
    win = tk.Toplevel()
    win.title("Register New User")

    tk.Label(win, text="New Username").pack()
    new_user_entry = tk.Entry(win)
    new_user_entry.pack()

    tk.Label(win, text="Password").pack()
    new_pass_entry = tk.Entry(win, show='*')
    new_pass_entry.pack()

    is_admin_var = tk.BooleanVar()
    tk.Checkbutton(win, text="Is Admin", variable=is_admin_var).pack()

    # Send registration request to backend
    def submit_registration():
        username = new_user_entry.get()
        password = new_pass_entry.get()
        is_admin = is_admin_var.get()

        try:
            res = requests.post("http://localhost:5000/register-user", json={
                "username": username,
                "password": hash_password(password),
                "isAdmin": is_admin
            })
            if res.status_code == 200:
                messagebox.showinfo("Success", "User registration request sent.")
            else:
                messagebox.showerror("Error", "Registration failed.")
        except Exception as e:
            messagebox.showerror("Error", str(e))
        win.destroy()

    tk.Button(win, text="Register", command=submit_registration).pack(pady=10)

# GUI for displaying a list of orders
def show_order_window(orders):
    win = tk.Toplevel()
    win.title("Orders")
    win.geometry("600x300")

    scrollbar = tk.Scrollbar(win)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    listbox = tk.Listbox(win, width=90, yscrollcommand=scrollbar.set)
    listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.config(command=listbox.yview)

    if not orders:
        listbox.insert(tk.END, "No orders found.")
    else:
        for order in orders:
            user_part = f"{order.get('user')} | " if current_user_is_admin else ""
            line = f"{user_part}Order #{order['order_id']} | {order['dt']} {order['tm']} | {order['description']}"
            listbox.insert(tk.END, line)

# Periodically check for new order responses from Kafka
def check_for_order_response():
    try:
        orders = order_response_queue.get_nowait()
        show_order_window(orders)
    except queue.Empty:
        pass
    root.after(500, check_for_order_response)

# Main dashboard GUI after login
def open_main_app(username):
    dash = tk.Toplevel()
    dash.title("Order Dashboard")
    dash.geometry("300x350")

    tk.Label(dash, text=f"Logged in as: {username}", font=("Arial", 14)).pack(pady=10)
    tk.Button(dash, text="Create Order", width=25, command=create_order_window).pack(pady=5)
    tk.Button(dash, text="Update Order", width=25, command=update_order_window).pack(pady=5)
    tk.Button(dash, text="Cancel Order", width=25, command=cancel_order_window).pack(pady=5)
    tk.Button(dash, text="View Orders", width=25, command=view_my_orders).pack(pady=5)

    if current_user_is_admin:
        tk.Button(dash, text="Register User", width=25, command=register_user_window).pack(pady=5)

# --- Initial Login Window Setup ---
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

# --- Start background listeners and GUI loop ---
threading.Thread(target=kafka_login_listener, daemon=True).start()
threading.Thread(target=kafka_order_response_listener, daemon=True).start()
check_for_order_response()

root.mainloop()

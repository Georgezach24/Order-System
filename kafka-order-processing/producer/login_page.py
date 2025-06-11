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

# --- Globals ---
current_user = None
current_user_is_admin = False
order_response_queue = queue.Queue()
session_id = str(uuid.uuid4())

def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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

def handle_login_success(username, is_admin):
    global current_user, current_user_is_admin
    current_user = username
    current_user_is_admin = is_admin
    messagebox.showinfo("Success", f"Welcome {username}! (Admin: {is_admin})")
    root.withdraw()
    open_main_app(username)

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

def create_order_window():
    win = tk.Toplevel()
    win.title("Create Order")

    user_label = tk.Label(win, text="Username (optional for admin)")
    user_label.pack()
    user_entry = tk.Entry(win, width=30)
    user_entry.insert(0, current_user)
    user_entry.pack()

    tk.Label(win, text="Order Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

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

def update_order_window():
    win = tk.Toplevel()
    win.title("Update Order")

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

def cancel_order_window():
    win = tk.Toplevel()
    win.title("Cancel Order")

    tk.Label(win, text="Order ID to Cancel").pack()
    order_id_entry = tk.Entry(win, width=30)
    order_id_entry.pack()

    user_label = tk.Label(win, text="Username (optional for admin)")
    user_label.pack()
    user_entry = tk.Entry(win, width=30)
    user_entry.insert(0, current_user)
    user_entry.pack()

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

def check_for_order_response():
    try:
        orders = order_response_queue.get_nowait()
        show_order_window(orders)
    except queue.Empty:
        pass
    root.after(500, check_for_order_response)

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

# --- Login window setup ---
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

# --- Start Kafka listeners and UI loop ---
threading.Thread(target=kafka_login_listener, daemon=True).start()
threading.Thread(target=kafka_order_response_listener, daemon=True).start()
check_for_order_response()

root.mainloop()

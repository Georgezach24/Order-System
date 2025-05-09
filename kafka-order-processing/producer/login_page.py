import hashlib
import tkinter as tk
from tkinter import messagebox
import threading
import requests
import json
import datetime
import random
from kafka import KafkaProducer, KafkaConsumer
import queue

# --- Globals ---
current_user = None
order_response_queue = queue.Queue()

# --- Hash password before sending to backend ---
def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

# --- Kafka Producer setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Kafka Consumer for login responses ---
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
        if data['username'] == current_user:
            if data['status'] == 'success':
                root.after(0, lambda: handle_login_success(current_user))
                break
            else:
                root.after(0, lambda: messagebox.showerror("Login Failed", "Invalid credentials"))
                break

# --- Kafka Consumer for user order query responses ---
def kafka_order_response_listener():
    consumer = KafkaConsumer(
        'order_query_responses',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id='order-query-listener',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        if data['user'] == current_user:
            order_response_queue.put(data['orders'])

# --- Handle successful login ---
def handle_login_success(username):
    messagebox.showinfo("Success", f"Welcome {username}!")
    root.withdraw()
    open_main_app(username)

# --- Login action ---
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

# --- Create Order Form ---
def create_order_window():
    win = tk.Toplevel()
    win.title("Create Order")

    tk.Label(win, text="Order Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

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
        producer.send('orders', order)
        messagebox.showinfo("Success", "Order sent to Kafka.")
        win.destroy()

    tk.Button(win, text="Submit Order", command=submit_order).pack(pady=10)

# --- Update Order Form ---
def update_order_window():
    win = tk.Toplevel()
    win.title("Update Order")

    tk.Label(win, text="Order ID to Update").pack()
    order_id_entry = tk.Entry(win, width=30)
    order_id_entry.pack()

    tk.Label(win, text="New Description").pack()
    desc_entry = tk.Entry(win, width=40)
    desc_entry.pack()

    def submit_update():
        try:
            order_id = int(order_id_entry.get())
        except ValueError:
            messagebox.showerror("Invalid Input", "Order ID must be a number.")
            return

        description = desc_entry.get()
        now = datetime.datetime.now()

        order_update = {
            "user": current_user,
            "order_id": order_id,
            "dt": now.strftime("%Y-%m-%d"),
            "tm": now.strftime("%H:%M:%S"),
            "description": description
        }

        producer.send('order_updates', order_update)
        messagebox.showinfo("Success", "Order update sent to Kafka.")
        win.destroy()

    tk.Button(win, text="Submit Update", command=submit_update).pack(pady=10)

# --- View User Orders ---
def view_my_orders():
    producer.send('order_query_requests', {"user": current_user})
    messagebox.showinfo("Info", "Requesting your orders...")
    
def cancel_order_window():
    win = tk.Toplevel()
    win.title("Cancel Order")

    tk.Label(win, text="Order ID to Cancel").pack()
    order_id_entry = tk.Entry(win, width=30)
    order_id_entry.pack()

    def submit_cancel():
        try:
            order_id = int(order_id_entry.get())
        except ValueError:
            messagebox.showerror("Invalid Input", "Order ID must be a number.")
            return

        cancel_data = {
            "user": current_user,
            "order_id": order_id
        }

        producer.send('order_cancellations', cancel_data)
        messagebox.showinfo("Cancelled", f"Cancel request for order #{order_id} sent.")
        win.destroy()

    tk.Button(win, text="Submit Cancellation", command=submit_cancel).pack(pady=10)


# --- Show Orders List Window ---
def show_order_window(orders):
    win = tk.Toplevel()
    win.title("Your Orders")
    win.geometry("500x300")

    scrollbar = tk.Scrollbar(win)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    listbox = tk.Listbox(win, width=80, yscrollcommand=scrollbar.set)
    listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.config(command=listbox.yview)

    if not orders:
        listbox.insert(tk.END, "No orders found.")
    else:
        for order in orders:
            line = f"Order #{order['order_id']} | {order['dt']} {order['tm']} | {order['description']}"
            listbox.insert(tk.END, line)

# --- Periodically check for Kafka order query responses ---
def check_for_order_response():
    try:
        orders = order_response_queue.get_nowait()
        show_order_window(orders)
    except queue.Empty:
        pass
    root.after(500, check_for_order_response)

# --- Dashboard after login ---
def open_main_app(username):
    dash = tk.Toplevel()
    dash.title("Order Dashboard")
    dash.geometry("300x300")

    tk.Label(dash, text=f"Logged in as: {username}", font=("Arial", 14)).pack(pady=10)
    tk.Button(dash, text="Create Order", width=25, command=create_order_window).pack(pady=5)
    tk.Button(dash, text="Update Order", width=25, command=update_order_window).pack(pady=5)
    tk.Button(dash, text="Cancel Order", width=25, command=cancel_order_window).pack(pady=5)
    tk.Button(dash, text="View My Orders", width=25, command=view_my_orders).pack(pady=5)

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

# --- Start Kafka listeners and Tkinter loop ---
threading.Thread(target=kafka_login_listener, daemon=True).start()
threading.Thread(target=kafka_order_response_listener, daemon=True).start()
check_for_order_response()

root.mainloop()

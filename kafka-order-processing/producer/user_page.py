import tkinter as tk
from tkinter import messagebox
import sys
import datetime
import random
import json
from kafka import KafkaProducer

username = sys.argv[1] if len(sys.argv) > 1 else "unknown"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def open_dashboard():
    dash = tk.Tk()
    dash.title("Order Dashboard")
    dash.geometry("300x250")

    tk.Label(dash, text=f"Logged in as: {username}", font=("Arial", 14)).pack(pady=10)

    tk.Button(dash, text="Create Order", width=25, command=create_order_window).pack(pady=5)
    tk.Button(dash, text="Update Order", width=25, command=update_stub).pack(pady=5)
    tk.Button(dash, text="Cancel Order", width=25, command=cancel_stub).pack(pady=5)

    dash.mainloop()

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
            "user": username,
            "order_id": random.randint(1000, 9999),
            "dt": now.strftime("%Y-%m-%d"),
            "tm": now.strftime("%H:%M:%S"),
            "description": description
        }
        producer.send('orders', order)
        messagebox.showinfo("Success", "Order sent to Kafka.")
        win.destroy()

    tk.Button(win, text="Submit Order", command=submit_order).pack(pady=10)

def update_stub():
    messagebox.showinfo("Update Order", "Coming soon...")

def cancel_stub():
    messagebox.showinfo("Cancel Order", "Coming soon...")

open_dashboard()

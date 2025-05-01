import tkinter as tk
from tkinter import messagebox
import requests
import random
import json
from datetime import datetime

def send_order():
    try:
        order_id = random.randint(1000, 9999)
        user_id = int(entry_user.get())
        items = entry_items.get().split(",")
        total = float(entry_total.get())

        order = {
            "order_id": order_id,
            "user_id": user_id,
            "items": [item.strip() for item in items],
            "total": total,
            "created_at": datetime.now().isoformat()
        }

        response = requests.post("http://localhost:5000/create-order", json=order)

        if response.status_code == 200:
            messagebox.showinfo("Success", f"Order {order_id} sent!")
        else:
            messagebox.showerror("Error", f"Failed: {response.status_code}")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# GUI Setup
root = tk.Tk()
root.title("Create Order")

tk.Label(root, text="User ID:").pack()
entry_user = tk.Entry(root)
entry_user.pack()

tk.Label(root, text="Items (comma-separated):").pack()
entry_items = tk.Entry(root)
entry_items.pack()

tk.Label(root, text="Total:").pack()
entry_total = tk.Entry(root)
entry_total.pack()

tk.Button(root, text="Send Order", command=send_order).pack(pady=10)

root.mainloop()

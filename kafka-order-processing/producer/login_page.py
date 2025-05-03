import tkinter as tk
from tkinter import messagebox
import mysql.connector

# Replace with your actual MySQL credentials
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # default in XAMPP
    'database': 'order_system_db'
}

def login():
    username = entry_user.get()
    password = entry_pass.get()

    #alln this need to be happen inside the api.py file
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE username=%s AND password=%s", (username, password))
        result = cursor.fetchone()

        if result:
            messagebox.showinfo("Success", f"Welcome, {username}!")
            root.destroy()
            open_main_app(username)
        else:
            messagebox.showerror("Error", "Invalid username or password.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

def open_main_app(username):
    main = tk.Tk()
    main.title("Order System Dashboard")

    tk.Label(main, text=f"Logged in as: {username}", font=('Arial', 12)).pack(pady=10)

    tk.Button(main, text="Create Order", width=20).pack(pady=5)
    tk.Button(main, text="Update Order", width=20).pack(pady=5)
    tk.Button(main, text="Cancel Order", width=20).pack(pady=5)

    main.mainloop()

# Login window
root = tk.Tk()
root.title("Login")

tk.Label(root, text="Username:").pack()
entry_user = tk.Entry(root)
entry_user.pack()

tk.Label(root, text="Password:").pack()
entry_pass = tk.Entry(root, show="*")
entry_pass.pack()

tk.Button(root, text="Login", command=login).pack(pady=10)

root.mainloop()

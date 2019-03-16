import tkinter as tk
'''
Potential way to make gui
http://effbot.org/tkinterbook/grid.htm
'''

root = tk.Tk()

year_txt = tk.Label(root, text='Year:')
year_txt.grid(row=0, column=0)
e = tk.Entry(root)
e.grid(row=0, column=1)

def go():

    button = tk.Button(root, text='Query',
                            command=build_dict)


    button.grid(row=1, column=0, columnspan=4)
    root.mainloop()

def build_dict():
    print(e.get())
import tkinter as tk
import process_tickets
'''
Potential way to make gui
http://effbot.org/tkinterbook/grid.htm
'''
TICKETS_FILEPATH = 'data/tickets/sample_tickets_1000.csv'
VIOLATIONS_FILEPATH = 'data/tickets/violations_dict.csv'

class WelcomeDialog():

    def __init__(self):
        self.window = tk.Tk()
        
        txt_label = tk.Label(self.window, text=('Tickets or permits data? (for'+
                                           ' linked data, choose tickets)'))
        txt_label.grid(row=0, column=1, columnspan=2)

        tickets_button = tk.Button(self.window, text="Tickets",
                                   command=self.select_tickets)
        tickets_button.grid(row=1, column=1, rowspan=5, columnspan=2)

        permits_button = tk.Button(self.window, text="Permits",
                                   command=self.select_permits)
        permits_button.grid(row=1, column=2, rowspan=5, columnspan=2)

        self.window.mainloop()


    def select_tickets(self):
        print('Selected tickets')
        self.window.withdraw()
        message = tk.Toplevel()
        loading_label = tk.Message(message, text='Loading dataset, please wait')
        loading_label.grid(row=0, column=0)

        tickets = process_tickets.import_tickets(TICKETS_FILEPATH, VIOLATIONS_FILEPATH)
        message.withdraw()
        self.window.destroy()



    def select_permits(self):
        print('Selected permits')
'''
class TicketsDialog()

    def __init__(self):
'''

if __name__ == '__main__':
   welcome = WelcomeDialog()

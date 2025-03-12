import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk
import os
import time
import EM133XM_HMI_Library as HMI_Library
from tkinter import ttk
import re

# Default page dimensions
WINDOW_WIDTH=800
WINDOW_HEIGHT=480
WINDOW_GEOMETRY = str(WINDOW_WIDTH) + "x" + str(WINDOW_HEIGHT) + "+0+0"  #"800x480+0+0"
MENU_HEIGHT = 30
PAGE_HEADER_HEIGHT = 30
PAGE_FRAME_WIDTH = 300
BUTTON_WIDTH = 12
BUTTON_COLOUR = 'gray'
PAGE_HEADIING_COLOUR = 'slate gray'
PAGE_H1_COLOUR = 'light gray'
PAGE_FRAME_HEIGHT = WINDOW_HEIGHT - MENU_HEIGHT

# Miscellaneous
TITTLE_BAR = "EM133XM"
MenuBg = 'light blue'

# Page class
class Page(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)      
    def show(self):
        self.lift()

# Billing page
class Page1(Page):
   def __init__(self, *args, **kwargs):
       Page.__init__(self, *args, **kwargs)
       print("Page1 being instanciated!")

       # Display frame  
       self.P1_DspFrame = tk.Frame(self, height = PAGE_FRAME_HEIGHT, width = PAGE_FRAME_WIDTH , borderwidth = 2,bg = "dark grey")
       self.P1_DspFrame.pack(side="top", fill = "both", expand = False)

       # Page title frame
       self.P1_Title_Frame = tk.Frame(self.P1_DspFrame,height = PAGE_HEADER_HEIGHT, bd=1, relief='solid', bg=PAGE_HEADIING_COLOUR)
       self.P1_Title_Frame.pack(side = "top", fill = "both",expand = False)
       self.P1_Title_Frame.pack_propagate(0)
       
       self.P1_Title = tk.Label(self.P1_Title_Frame, text="BILLING INFORMATION" ,justify = "right", font=30, bg=PAGE_HEADIING_COLOUR)
       self.P1_Title.pack(side="left", fill = "both", anchor="n", expand=True, pady=(7,5))

       # Billing update button
       self.Updatebtn = tk.Button(self.P1_Title_Frame, height = PAGE_HEADER_HEIGHT, width = 10, text="UPDATE",
                                  command=lambda:[self.UpdateBillInfo()], bg= BUTTON_COLOUR)
       self.Updatebtn.pack(anchor = "ne")
       self.Updatebtn.pack_propagate(0)     

       # Table sizes
       self.P1_Table_Header_Height = 40
       self.P1_Table_Sub_Header_Height = 20
       self.P1_Table_B_Height = int((PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - self.P1_Table_Header_Height)/2)
       self.P1_Table_Data_Row_Height = self.P1_Table_B_Height - self.P1_Table_Sub_Header_Height

       self.P1_Table_B1_Col_Width = int(WINDOW_WIDTH*0.20)
       self.P1_Table_B2_Col_Width = int((WINDOW_WIDTH - self.P1_Table_B1_Col_Width)/4)
       self.P1_Table_B1_Col_Padx = 1

       # Table frames
       self.P1_Table_Container_Header_Frame = tk.Frame(self.P1_DspFrame,height = self.P1_Table_Header_Height)
       self.P1_Table_Container_Header_Frame.pack(side = "top", fill = "both",expand = False)  
       self.P1_Table_Container_B1_Frame = tk.Frame(self.P1_DspFrame,height = self.P1_Table_B_Height)
       self.P1_Table_Container_B1_Frame.pack(side = "top", fill = "both",expand = False)  
       self.P1_Table_Container_B2_Frame = tk.Frame(self.P1_DspFrame,height = self.P1_Table_B_Height)
       self.P1_Table_Container_B2_Frame.pack(side = "top", fill = "both",expand = False)  
       self.P1_Blank_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = self.P1_Table_Header_Height,
                                      width= self.P1_Table_B1_Col_Width, bd=1, relief='solid')
       self.P1_Blank_Frame.pack(side="left", anchor = "nw", fill = "y")
       self.P1_Blank_Frame.pack_propagate(0)
       
       self.P1_L1_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = self.P1_Table_Header_Height,
                                   width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_L1_Frame.pack(side="left", anchor = "nw", fill = "y")
       self.P1_L1_Frame.pack_propagate(0)
       self.P1_L1_Label1 = tk.Label(self.P1_L1_Frame, text="3 PHASE TOTAL", justify = "center")
       self.P1_L1_Label1.pack(side="top", fill="y", expand=True)

       self.P1_L2_Header1_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = (self.P1_Table_Header_Height/2),
                                           width= (self.P1_Table_B2_Col_Width*3), bd=1, relief='solid')
       self.P1_L2_Header1_Frame.pack(side="top", anchor = "nw")
       self.P1_L2_Header1_Frame.pack_propagate(0)
       self.P1_L1_Label2 = tk.Label(self.P1_L2_Header1_Frame, text="SINGLE PHASE TOTALS", justify = "center")
       self.P1_L1_Label2.pack(side="top", expand=False)
       
       self.P1_L2_Header2_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = (self.P1_Table_Header_Height/2),
                                           width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_L2_Header2_Frame.pack(side="left", anchor = "sw")
       self.P1_L2_Header2_Frame.pack_propagate(0)
       self.P1_L1_Label3 = tk.Label(self.P1_L2_Header2_Frame, text="LINE 1", justify = "center")
       self.P1_L1_Label3.pack(side="top", expand=False)
              
       self.P1_L2_Header3_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = (self.P1_Table_Header_Height/2),
                                           width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_L2_Header3_Frame.pack(side="left", anchor = "sw")
       self.P1_L2_Header3_Frame.pack_propagate(0)
       self.P1_L1_Label4 = tk.Label(self.P1_L2_Header3_Frame, text="LINE 2", justify = "center")
       self.P1_L1_Label4.pack(side="top", expand=False)

       self.P1_L2_Header4_Frame = tk.Frame(self.P1_Table_Container_Header_Frame,height = (self.P1_Table_Header_Height/2),
                                           width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_L2_Header4_Frame.pack(side="left", anchor = "sw")
       self.P1_L2_Header4_Frame.pack_propagate(0)
       self.P1_L1_Label5 = tk.Label(self.P1_L2_Header4_Frame, text="LINE 3", justify = "center")
       self.P1_L1_Label5.pack(side="top", expand=False)

       # Current month header
       self.P1_Current_Month = tk.Frame(self.P1_Table_Container_B1_Frame,height = self.P1_Table_Sub_Header_Height,
                                        width= WINDOW_WIDTH, bd=1, relief='solid', bg = PAGE_H1_COLOUR)
       self.P1_Current_Month.pack(side="top", anchor = "nw")
       self.P1_Current_Month.pack_propagate(0)
       self.P1_Current_Month_Label = tk.Label(self.P1_Current_Month, text="CURRENT MONTH", justify = "center", bg = PAGE_H1_COLOUR)
       self.P1_Current_Month_Label.pack(side="top", expand=False)

       self.P1_Cur_Mon_Title_Frame = tk.Frame(self.P1_Table_Container_B1_Frame,height = self.P1_Table_Data_Row_Height ,
                                              width= self.P1_Table_B1_Col_Width, bd=1, relief='solid')
       self.P1_Cur_Mon_Title_Frame.pack(side="left", anchor = "nw", fill = "y")
       self.P1_Cur_Mon_Title_Frame.pack_propagate(0)

       self.Data_Headings = ["Peak", "Shoulder", "Off Peak", "Max Demand kW", "Max Demand kVA", "Total kWh"]

       self.P1_Cur_Mon_Container_Title_Frame = tk.Frame(self.P1_Cur_Mon_Title_Frame)
       self.P1_Cur_Mon_Container_Title_Frame.pack(side="left", anchor = "nw", expand=False, padx=(self.P1_Table_B1_Col_Padx,0))
       
       for i in self.Data_Headings:
           self.P1_3PT_Cur_Mon_Label = tk.Label(self.P1_Cur_Mon_Container_Title_Frame, text= i , justify = "left", bd=-2)
           self.P1_3PT_Cur_Mon_Label.pack(side="top", anchor = "w", expand=False)

       self.P1_Data_Col1_Row1 = tk.Frame(self.P1_Table_Container_B1_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col1_Row1.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col1_Row1.pack_propagate(0)
       self.P1_Data_Col2_Row1 = tk.Frame(self.P1_Table_Container_B1_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col2_Row1.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col2_Row1.pack_propagate(0)
       self.P1_Data_Col3_Row1 = tk.Frame(self.P1_Table_Container_B1_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col3_Row1.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col3_Row1.pack_propagate(0)
       self.P1_Data_Col4_Row1 = tk.Frame(self.P1_Table_Container_B1_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col4_Row1.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col4_Row1.pack_propagate(0)
       
       # 3phase current month data
       self.P1_3PT_Cur_Mon_Data_Frame = tk.Frame(self.P1_Data_Col1_Row1, height = self.P1_Table_Data_Row_Height,
                                                 width = int(200-120))
       self.P1_3PT_Cur_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_3PT_Cur_Mon_Data_Frame.pack_propagate(0)
       self.P1_3PT_Cur_Mon_Data1 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Cur_Mon_Data2 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Cur_Mon_Data3 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Cur_Mon_Data4 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Cur_Mon_Data5 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Cur_Mon_Data6 = tk.Label(self.P1_3PT_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Cur_Mon_Data6.pack(side="top", anchor = "w", expand=False)
       
       # Line1 current month data
       self.P1_L1_Cur_Mon_Data_Frame = tk.Frame(self.P1_Data_Col2_Row1, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L1_Cur_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L1_Cur_Mon_Data_Frame.pack_propagate(0)
       self.P1_L1_Cur_Mon_Data1 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Cur_Mon_Data2 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Cur_Mon_Data3 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data3.pack(side="top", anchor ="w", expand=False)
       self.P1_L1_Cur_Mon_Data4 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Cur_Mon_Data5 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Cur_Mon_Data6 = tk.Label(self.P1_L1_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Cur_Mon_Data6.pack(side="top", anchor = "w", expand=False)

       # Line2 current month data
       self.P1_L2_Cur_Mon_Data_Frame = tk.Frame(self.P1_Data_Col3_Row1, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L2_Cur_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L2_Cur_Mon_Data_Frame.pack_propagate(0)
       self.P1_L2_Cur_Mon_Data1 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Cur_Mon_Data2 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Cur_Mon_Data3 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Cur_Mon_Data4 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Cur_Mon_Data5 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Cur_Mon_Data6 = tk.Label(self.P1_L2_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Cur_Mon_Data6.pack(side="top", anchor = "w", expand=False)
       
       # Line3 current month data
       self.P1_L3_Cur_Mon_Data_Frame = tk.Frame(self.P1_Data_Col4_Row1, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L3_Cur_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L3_Cur_Mon_Data_Frame.pack_propagate(0)
       self.P1_L3_Cur_Mon_Data1 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Cur_Mon_Data2 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Cur_Mon_Data3 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Cur_Mon_Data4 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Cur_Mon_Data5 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Cur_Mon_Data6 = tk.Label(self.P1_L3_Cur_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Cur_Mon_Data6.pack(side="top", anchor = "w", expand=False)
       
       # Previous month header
       self.P1_Previous_Month = tk.Frame(self.P1_Table_Container_B2_Frame,height = self.P1_Table_Sub_Header_Height,
                                         width= WINDOW_WIDTH, bd=1, relief='solid', bg = PAGE_H1_COLOUR)
       self.P1_Previous_Month.pack(side="top", anchor = "nw")
       self.P1_Previous_Month.pack_propagate(0)
       self.P1_Previous_Month_Title = tk.Label(self.P1_Previous_Month, text="PREVIOUS MONTH", justify = "center", bg = PAGE_H1_COLOUR)
       self.P1_Previous_Month_Title.pack(side="top", expand=False)
       
       self.P1_Pre_Mon_Title_Frame = tk.Frame(self.P1_Table_Container_B2_Frame,height = self.P1_Table_Data_Row_Height ,
                                              width= self.P1_Table_B1_Col_Width, bd=1, relief='solid')
       self.P1_Pre_Mon_Title_Frame.pack(side="left", anchor = "nw", fill = "y")
       self.P1_Pre_Mon_Title_Frame.pack_propagate(0)
       
       self.P1_Pre_Mon_Container_Title_Frame = tk.Frame(self.P1_Pre_Mon_Title_Frame)
       self.P1_Pre_Mon_Container_Title_Frame.pack(side="left", anchor = "nw", expand=False, padx=(self.P1_Table_B1_Col_Padx,0))
       
       for i in self.Data_Headings:
           self.P1_Pre_Mon_Label = tk.Label(self.P1_Pre_Mon_Container_Title_Frame, text= i , justify = "left", bd=-2)
           self.P1_Pre_Mon_Label.pack(side="top", anchor = "w", expand=False)

       self.P1_Data_Col1_Row2 = tk.Frame(self.P1_Table_Container_B2_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col1_Row2.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col1_Row2.pack_propagate(0)
       self.P1_Data_Col2_Row2 = tk.Frame(self.P1_Table_Container_B2_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col2_Row2.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col2_Row2.pack_propagate(0)
       self.P1_Data_Col3_Row2 = tk.Frame(self.P1_Table_Container_B2_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col3_Row2.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col3_Row2.pack_propagate(0)
       self.P1_Data_Col4_Row2 = tk.Frame(self.P1_Table_Container_B2_Frame, height = self.P1_Table_Data_Row_Height,
                                         width= self.P1_Table_B2_Col_Width, bd=1, relief='solid')
       self.P1_Data_Col4_Row2.pack(side="left", anchor = "nw", expand=False)
       self.P1_Data_Col4_Row2.pack_propagate(0)
       
       # 3phase previous month data
       self.P1_3PT_Pre_Mon_Data_Frane = tk.Frame(self.P1_Data_Col1_Row2, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_3PT_Pre_Mon_Data_Frane.pack(side="top", anchor = "n", expand=True)
       self.P1_3PT_Pre_Mon_Data_Frane.pack_propagate(0)
       self.P1_3PT_Pre_Mon_Data1 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Pre_Mon_Data2 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Pre_Mon_Data3 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Pre_Mon_Data4 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Pre_Mon_Data5 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_3PT_Pre_Mon_Data6 = tk.Label(self.P1_3PT_Pre_Mon_Data_Frane, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_3PT_Pre_Mon_Data6.pack(side="top", anchor = "w", expand=False)
      
       # Line1 previous month data
       self.P1_L1_Pre_Mon_Data_Frame = tk.Frame(self.P1_Data_Col2_Row2, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L1_Pre_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L1_Pre_Mon_Data_Frame.pack_propagate(0)
       self.P1_L1_Pre_Mon_Data1 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Pre_Mon_Data2 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Pre_Mon_Data3 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Pre_Mon_Data4 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Pre_Mon_Data5 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L1_Pre_Mon_Data6 = tk.Label(self.P1_L1_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L1_Pre_Mon_Data6.pack(side="top", anchor = "w", expand=False)

       # Line2 previous month data
       self.P1_L2_Pre_Mon_Data_Frame = tk.Frame(self.P1_Data_Col3_Row2, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L2_Pre_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L2_Pre_Mon_Data_Frame.pack_propagate(0)
       self.P1_L2_Pre_Mon_Data1 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Pre_Mon_Data2 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Pre_Mon_Data3 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Pre_Mon_Data4 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Pre_Mon_Data5 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L2_Pre_Mon_Data6 = tk.Label(self.P1_L2_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L2_Pre_Mon_Data6.pack(side="top", anchor = "w", expand=False)
 
       # Line3 previous month data
       self.P1_L3_Pre_Mon_Data_Frame = tk.Frame(self.P1_Data_Col4_Row2, height = self.P1_Table_Data_Row_Height, width = int(200-120))
       self.P1_L3_Pre_Mon_Data_Frame.pack(side="top", anchor = "n", expand=True)
       self.P1_L3_Pre_Mon_Data_Frame.pack_propagate(0)
       self.P1_L3_Pre_Mon_Data1 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data1.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Pre_Mon_Data2 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data2.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Pre_Mon_Data3 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data3.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Pre_Mon_Data4 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kW", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data4.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Pre_Mon_Data5 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kVA", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data5.pack(side="top", anchor = "w", expand=False)
       self.P1_L3_Pre_Mon_Data6 = tk.Label(self.P1_L3_Pre_Mon_Data_Frame, text= "{: >4d}".format(0)+" kWh", justify = "left", bd=-2)
       self.P1_L3_Pre_Mon_Data6.pack(side="top", anchor = "w", expand=False)

   def __del__(self, *args, **kwargs):
       print("Page1 being automatically destroyed. Goodbye!")

   # Update billing data
   def UpdateBillInfo(self):
       print("Page1 data being updated")
       self.BillInstance = HMI_Library.Billing()
       self.BillInstance.UpdateArrays()

       # 3phase data
       CurMon3PTData = self.BillInstance.getPhaseCur()
       PreMon3PTData = self.BillInstance.getPhasePrev()

       # Signle phase data
       CurMonL1Data = self.BillInstance.getLine1Cur()
       CurMonL2Data = self.BillInstance.getLine2Cur()
       CurMonL3Data = self.BillInstance.getLine3Cur()
       PreMonL1Data = self.BillInstance.getLine1Prev()
       PreMonL2Data = self.BillInstance.getLine2Prev()
       PreMonL3Data = self.BillInstance.getLine3Prev()

       # Update data
       self.P1_3PT_Cur_Mon_Data1.config(text= "{:.3f}".format(CurMon3PTData[0])+" kWh")
       self.P1_3PT_Cur_Mon_Data2.config(text= "{:.3f}".format(CurMon3PTData[1])+" kWh")
       self.P1_3PT_Cur_Mon_Data3.config(text= "{:.3f}".format(CurMon3PTData[2])+" kWh")
       self.P1_3PT_Cur_Mon_Data4.config(text= "{:.3f}".format(CurMon3PTData[3])+" kW")
       self.P1_3PT_Cur_Mon_Data5.config(text= "{:.3f}".format(CurMon3PTData[4])+" kVA")
       self.P1_3PT_Cur_Mon_Data6.config(text= "{:.3f}".format(CurMon3PTData[5])+" kWh")

       self.P1_3PT_Pre_Mon_Data1.config(text= "{:.3f}".format(PreMon3PTData[0])+" kWh")
       self.P1_3PT_Pre_Mon_Data2.config(text= "{:.3f}".format(PreMon3PTData[1])+" kWh")
       self.P1_3PT_Pre_Mon_Data3.config(text= "{:.3f}".format(PreMon3PTData[2])+" kWh")
       self.P1_3PT_Pre_Mon_Data4.config(text= "{:.3f}".format(PreMon3PTData[3])+" kW")
       self.P1_3PT_Pre_Mon_Data5.config(text= "{:.3f}".format(PreMon3PTData[4])+" kVA")
       self.P1_3PT_Pre_Mon_Data6.config(text= "{:.3f}".format(PreMon3PTData[5])+" kWh")

       self.P1_L1_Cur_Mon_Data1.config(text ="{:.3f}".format(CurMonL1Data[0])+" kWh")
       self.P1_L1_Cur_Mon_Data1.config(text= "{:.3f}".format(CurMonL1Data[0])+" kWh")
       self.P1_L1_Cur_Mon_Data2.config(text= "{:.3f}".format(CurMonL1Data[1])+" kWh")
       self.P1_L1_Cur_Mon_Data3.config(text= "{:.3f}".format(CurMonL1Data[2])+" kWh")
       self.P1_L1_Cur_Mon_Data4.config(text= "{:.3f}".format(CurMonL1Data[3])+" kW")
       self.P1_L1_Cur_Mon_Data5.config(text= "{:.3f}".format(CurMonL1Data[4])+" kVA")
       self.P1_L1_Cur_Mon_Data6.config(text= "{:.3f}".format(CurMonL1Data[5])+" kWh")

       self.P1_L2_Cur_Mon_Data1.config(text ="{:.3f}".format(CurMonL2Data[0])+" kWh")
       self.P1_L2_Cur_Mon_Data1.config(text= "{:.3f}".format(CurMonL2Data[0])+" kWh")
       self.P1_L2_Cur_Mon_Data2.config(text= "{:.3f}".format(CurMonL2Data[1])+" kWh")
       self.P1_L2_Cur_Mon_Data3.config(text= "{:.3f}".format(CurMonL2Data[2])+" kWh")
       self.P1_L2_Cur_Mon_Data4.config(text= "{:.3f}".format(CurMonL2Data[3])+" kW")
       self.P1_L2_Cur_Mon_Data5.config(text= "{:.3f}".format(CurMonL2Data[4])+" kVA")
       self.P1_L2_Cur_Mon_Data6.config(text= "{:.3f}".format(CurMonL2Data[5])+" kWh")
       
       self.P1_L3_Cur_Mon_Data1.config(text ="{:.3f}".format(CurMonL3Data[0])+" kWh")
       self.P1_L3_Cur_Mon_Data1.config(text= "{:.3f}".format(CurMonL3Data[0])+" kWh")
       self.P1_L3_Cur_Mon_Data2.config(text= "{:.3f}".format(CurMonL3Data[1])+" kWh")
       self.P1_L3_Cur_Mon_Data3.config(text= "{:.3f}".format(CurMonL3Data[2])+" kWh")
       self.P1_L3_Cur_Mon_Data4.config(text= "{:.3f}".format(CurMonL3Data[3])+" kW")
       self.P1_L3_Cur_Mon_Data5.config(text= "{:.3f}".format(CurMonL3Data[4])+" kVA")
       self.P1_L3_Cur_Mon_Data6.config(text= "{:.3f}".format(CurMonL3Data[5])+" kWh")

       self.P1_L1_Pre_Mon_Data1.config(text= "{:.3f}".format(PreMonL1Data[0])+" kWh")
       self.P1_L1_Pre_Mon_Data2.config(text= "{:.3f}".format(PreMonL1Data[1])+" kWh")
       self.P1_L1_Pre_Mon_Data3.config(text= "{:.3f}".format(PreMonL1Data[2])+" kWh")
       self.P1_L1_Pre_Mon_Data4.config(text= "{:.3f}".format(PreMonL1Data[3])+" kW")
       self.P1_L1_Pre_Mon_Data5.config(text= "{:.3f}".format(PreMonL1Data[4])+" kVA")
       self.P1_L1_Pre_Mon_Data6.config(text= "{:.3f}".format(PreMonL1Data[5])+" kWh")

       self.P1_L2_Pre_Mon_Data1.config(text= "{:.3f}".format(PreMonL2Data[0])+" kWh")
       self.P1_L2_Pre_Mon_Data2.config(text= "{:.3f}".format(PreMonL2Data[1])+" kWh")
       self.P1_L2_Pre_Mon_Data3.config(text= "{:.3f}".format(PreMonL2Data[2])+" kWh")
       self.P1_L2_Pre_Mon_Data4.config(text= "{:.3f}".format(PreMonL2Data[3])+" kW")
       self.P1_L2_Pre_Mon_Data5.config(text= "{:.3f}".format(PreMonL2Data[4])+" kVA")
       self.P1_L2_Pre_Mon_Data6.config(text= "{:.3f}".format(PreMonL2Data[5])+" kWh")

       self.P1_L3_Pre_Mon_Data1.config(text= "{:.3f}".format(PreMonL3Data[0])+" kWh")
       self.P1_L3_Pre_Mon_Data2.config(text= "{:.3f}".format(PreMonL3Data[1])+" kWh")
       self.P1_L3_Pre_Mon_Data3.config(text= "{:.3f}".format(PreMonL3Data[2])+" kWh")
       self.P1_L3_Pre_Mon_Data4.config(text= "{:.3f}".format(PreMonL3Data[3])+" kW")
       self.P1_L3_Pre_Mon_Data5.config(text= "{:.3f}".format(PreMonL3Data[4])+" kVA")
       self.P1_L3_Pre_Mon_Data6.config(text= "{:.3f}".format(PreMonL3Data[5])+" kWh")

  
# Engineering     
class Page2(Page):
   def __init__(self, *args, **kwargs):
       Page.__init__(self, *args, **kwargs)
       Page2.Page2View1(self)
       print("Page2 being instanciated!")

   def __del__(self, *args, **kwargs):
       print("Page2 being automatically destroyed. Goodbye!")

   def Page2View1(self):
       # Create frame  
       self.P2V1_DspFrame = tk.Frame(self, height = PAGE_FRAME_HEIGHT, width = PAGE_FRAME_WIDTH,borderwidth = 2,bg = "dark grey")
       self.P2V1_DspFrame.pack(side="top", fill = "both", expand = False)

       # Page title
       self.P2V1_Title_Frame = tk.Frame(self.P2V1_DspFrame,height = PAGE_HEADER_HEIGHT, bd=1, relief='solid', bg=PAGE_HEADIING_COLOUR)
       self.P2V1_Title_Frame.pack(side = "top", fill = "both",expand = False)
       self.P2V1_Title_Frame.pack_propagate(0)
       self.P2V1_Title = tk.Label(self.P2V1_Title_Frame, text="ENGINEERING SCREEN" ,justify = "right", font=30, bg=PAGE_HEADIING_COLOUR)
       self.P2V1_Title.pack(side="left", fill = "both", anchor="n", expand=True, pady=(7,5))

       # Header button
       self.P2V1btn = tk.Button(self.P2V1_Title_Frame, height = PAGE_HEADER_HEIGHT, text="TOTAL VALUES",
                                command=lambda:[self.P2V1_DspFrame.pack_forget(),self.P2V1_DspFrame.destroy(),Page2.Page2View2(self)], bg= BUTTON_COLOUR)
       self.P2V1btn.pack(anchor = "ne")
       self.P2V1btn.pack_propagate(0)
     
       self.P2V1_Label_Width = int(WINDOW_WIDTH*0.40)
       self.P2V1_Data_Width = int((WINDOW_WIDTH - self.P2V1_Label_Width)/3)
       self.P2V1_Col_Padx = int(self.P2V1_Label_Width/2)
       
       # Title frame
       self.P2V1_SPT_Titles_Container_Frame = tk.Frame(self.P2V1_DspFrame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT),
                                                       width = self.P2V1_Label_Width)
       self.P2V1_SPT_Titles_Container_Frame.pack(side="left", anchor = "nw", expand=False)
       self.P2V1_SPT_Titles_Container_Frame.pack_propagate(0)

       self.P2V1_Blank_Frame = tk.Frame(self.P2V1_SPT_Titles_Container_Frame, height = 20, width = self.P2V1_Label_Width,
                                        bd=1, relief='solid' )
       self.P2V1_Blank_Frame.pack(side="top", anchor = "nw", expand=False)
       self.P2V1_Blank_Frame.pack_propagate(0)

       self.P2V1_SPT_Titles_Frame = tk.Frame(self.P2V1_SPT_Titles_Container_Frame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 20),
                                             width = self.P2V1_Label_Width , bd=1, relief='solid')
       self.P2V1_SPT_Titles_Frame.pack(side="left", anchor = "nw", expand=False)
       self.P2V1_SPT_Titles_Frame.pack_propagate(0)
       
       self.P2V1_L1_Frame = tk.Frame(self.P2V1_SPT_Titles_Frame)
       self.P2V1_L1_Frame.pack(side="left", anchor = "nw", expand=False, padx=(self.P2V1_Col_Padx,0), pady=20)

       self.P2_SPT_L1_Container_Frame0 = tk.Frame(self.P2V1_DspFrame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT),
                                                  width = self.P2V1_Data_Width)
       self.P2_SPT_L1_Container_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L1_Container_Frame0.pack_propagate(0)
       self.P2_SPT_L2_Container_Frame0 = tk.Frame(self.P2V1_DspFrame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT),
                                                  width = self.P2V1_Data_Width)
       self.P2_SPT_L2_Container_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L2_Container_Frame0.pack_propagate(0)
       self.P2_SPT_L3_Container_Frame0 = tk.Frame(self.P2V1_DspFrame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT),
                                                  width = self.P2V1_Data_Width)
       self.P2_SPT_L3_Container_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L3_Container_Frame0.pack_propagate(0)
       
       # Read meter data first time
       self.RealTimeData = HMI_Library.RealTimeMeasurement()
       self.RealData = self.RealTimeData.DataArray()

       self.Data_Headings = ["V", "I", "KW", "KVAR", "KVA", "PF", "V ANGLE","I ANGLE", "V THD", "I THD", "I TDD", "FREQ"]
       
       for i in self.Data_Headings:
           self.P2_Title_Label = tk.Label(self.P2V1_L1_Frame, text= i , justify = "left", bd=-2)
           self.P2_Title_Label.pack(side="top", anchor = "w", expand=False)
    
       # Line1 titles
       self.P2V1_L1_Title_Frame = tk.Frame(self.P2_SPT_L1_Container_Frame0, height = 20, width = self.P2V1_Data_Width, bd=1,
                                           relief='solid')
       self.P2V1_L1_Title_Frame.pack(side="top", anchor = "nw", expand=False, fill="y")
       self.P2V1_L1_Title_Frame.pack_propagate(0)
       self.P2V1_L1_Title = tk.Label(self.P2V1_L1_Title_Frame, text= "LINE 1", justify = "center")
       self.P2V1_L1_Title.pack(side="top", anchor = "n", expand=False)

       self.P2_SPT_L1_Frame0 = tk.Frame(self.P2_SPT_L1_Container_Frame0, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT-20),
                                        width = self.P2V1_Data_Width , bd=1, relief='solid')
       self.P2_SPT_L1_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L1_Frame0.pack_propagate(0)
       
       # Line1 data
       self.P2V1_L1_Data_Frame = tk.Frame(self.P2_SPT_L1_Frame0)
       self.P2V1_L1_Data_Frame.pack(side="left", anchor = "nw", expand=False, padx=20, pady=20)

       self.P2V1_L1_Data1 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +" V", justify = "left", bd=-2)
       self.P2V1_L1_Data1.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data2 = tk.Label(self.P2V1_L1_Data_Frame, text=  "0" +" A", justify = "left", bd=-2)
       self.P2V1_L1_Data2.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data3 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +" KW", justify = "left", bd=-2)
       self.P2V1_L1_Data3.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data4 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +" KVAR", justify = "left", bd=-2)
       self.P2V1_L1_Data4.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data5 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +" KVA", justify = "left", bd=-2)
       self.P2V1_L1_Data5.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data6 = tk.Label(self.P2V1_L1_Data_Frame, text= "0", justify = "left", bd=-2)
       self.P2V1_L1_Data6.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data7 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L1_Data7.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data8 = tk.Label(self.P2V1_L1_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L1_Data8.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data9 = tk.Label(self.P2V1_L1_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L1_Data9.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data10 = tk.Label(self.P2V1_L1_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L1_Data10.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data11 = tk.Label(self.P2V1_L1_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L1_Data11.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L1_Data12 = tk.Label(self.P2V1_L1_Data_Frame, text= "0"+" Hz", justify = "left", bd=-2)
       self.P2V1_L1_Data12.pack(side="top", anchor = "w", expand=False)

       # Line2 titles
       self.P2V1_L2_Title_Frame = tk.Frame(self.P2_SPT_L2_Container_Frame0, height = 20, width = self.P2V1_Data_Width,
                                           bd=1, relief='solid')
       self.P2V1_L2_Title_Frame.pack(side="top", anchor = "nw", expand=False)
       self.P2V1_L2_Title_Frame.pack_propagate(0)
       self.P2V1_L2_Title = tk.Label(self.P2V1_L2_Title_Frame, text= "LINE 2", justify = "center")
       self.P2V1_L2_Title.pack(side="top", anchor = "n", expand=False)

       self.P2_SPT_L2_Frame0 = tk.Frame(self.P2_SPT_L2_Container_Frame0, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT-20),
                                        width = self.P2V1_Data_Width, bd=1, relief='solid')
       self.P2_SPT_L2_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L2_Frame0.pack_propagate(0)
       
       # Line2 data        
       self.P2V1_L2_Data_Frame = tk.Frame(self.P2_SPT_L2_Frame0)
       self.P2V1_L2_Data_Frame.pack(side="left", anchor = "nw", expand=False, padx=20, pady=20)

       self.P2V1_L2_Data1 = tk.Label(self.P2V1_L2_Data_Frame, text= "0"+" V", justify = "left", bd=-2)
       self.P2V1_L2_Data1.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data2 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +" A", justify = "left", bd=-2)
       self.P2V1_L2_Data2.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data3 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +" KW", justify = "left", bd=-2)
       self.P2V1_L2_Data3.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data4 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +" KVAR", justify = "left", bd=-2)
       self.P2V1_L2_Data4.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data5 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +" KVA", justify = "left", bd=-2)
       self.P2V1_L2_Data5.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data6 = tk.Label(self.P2V1_L2_Data_Frame, text= "0", justify = "left", bd=-2)
       self.P2V1_L2_Data6.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data7 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L2_Data7.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data8 = tk.Label(self.P2V1_L2_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L2_Data8.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data9 = tk.Label(self.P2V1_L2_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L2_Data9.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data10 = tk.Label(self.P2V1_L2_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L2_Data10.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data11 = tk.Label(self.P2V1_L2_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L2_Data11.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L2_Data12 = tk.Label(self.P2V1_L2_Data_Frame, text= "0"+" Hz", justify = "left", bd=-2)
       self.P2V1_L2_Data12.pack(side="top", anchor = "w", expand=False)

       # Line3 title
       self.P2V1_L3_Title_Frame = tk.Frame(self.P2_SPT_L3_Container_Frame0, height = 20, width = self.P2V1_Data_Width,
                                           bd=1, relief='solid')
       self.P2V1_L3_Title_Frame.pack(side="top", anchor = "nw", expand=False, fill="x")
       self.P2V1_L3_Title_Frame.pack_propagate(0)
       self.P2V1_L3_Title = tk.Label(self.P2V1_L3_Title_Frame, text= "LINE 3", justify = "center")
       self.P2V1_L3_Title.pack(side="top", anchor = "n", expand=False)

       self.P2_SPT_L3_Frame0 = tk.Frame(self.P2_SPT_L3_Container_Frame0, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT-20),
                                        width = self.P2V1_Data_Width , bd=1, relief='solid')
       self.P2_SPT_L3_Frame0.pack(side="left", anchor = "nw", expand=False)
       self.P2_SPT_L3_Frame0.pack_propagate(0)

       # Line3 data
       self.P2V1_L3_Data_Frame = tk.Frame(self.P2_SPT_L3_Frame0)
       self.P2V1_L3_Data_Frame.pack(side="left", anchor = "nw", expand=False, padx=20, pady=20)

       self.P2V1_L3_Data1 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +" V", justify = "left", bd=-2)
       self.P2V1_L3_Data1.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data2 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +" A", justify = "left", bd=-2)
       self.P2V1_L3_Data2.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data3 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +" KW", justify = "left", bd=-2)
       self.P2V1_L3_Data3.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data4 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +" KVAR", justify = "left", bd=-2)
       self.P2V1_L3_Data4.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data5 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +" KVA", justify = "left", bd=-2)
       self.P2V1_L3_Data5.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data6 = tk.Label(self.P2V1_L3_Data_Frame, text= "0", justify = "left", bd=-2)
       self.P2V1_L3_Data6.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data7 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L3_Data7.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data8 = tk.Label(self.P2V1_L3_Data_Frame, text= "0" +"°", justify = "left", bd=-2)
       self.P2V1_L3_Data8.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data9 = tk.Label(self.P2V1_L3_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L3_Data9.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data10 = tk.Label(self.P2V1_L3_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L3_Data10.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data11 = tk.Label(self.P2V1_L3_Data_Frame, text= "0"+"%", justify = "left", bd=-2)
       self.P2V1_L3_Data11.pack(side="top", anchor = "w", expand=False)
       self.P2V1_L3_Data12 = tk.Label(self.P2V1_L3_Data_Frame, text= "0"+" Hz", justify = "left", bd=-2)
       self.P2V1_L3_Data12.pack(side="top", anchor = "w", expand=False)
       
       # Read meter data continously
       self.RealTimeMeasurementReading()
    
   def RealTimeMeasurementReading(self):
       
       if self.P2V1_L1_Data1.winfo_exists():
           print("Realtime data is being called by page2 view1")
           self.RealTimeData.ReadData()
           self.RealData = self.RealTimeData.DataArray()
           self.P2V1_L1_Data1.config (text = self.RealData['V1Eu'] +" V")
           self.P2V1_L1_Data2.config (text = self.RealData['I1Eu'] +" A")
           self.P2V1_L1_Data3.config (text = self.RealData['kW1Eu'] +" kW")
           self.P2V1_L1_Data4.config (text = self.RealData['kvar1Eu'] +" kvar")
           self.P2V1_L1_Data5.config (text = self.RealData['kVA1Eu'] +" kVA")
           self.P2V1_L1_Data6.config (text = self.RealData['PF1Eu'])
           self.P2V1_L1_Data7.config (text = self.RealData['V1AngEu'] +"°")
           self.P2V1_L1_Data8.config (text = self.RealData['I1AngEu'] +"°")
           self.P2V1_L1_Data9.config (text = self.RealData['V1THDEu'] +" %")
           self.P2V1_L1_Data10.config (text = self.RealData['I1THDEu']+" %")
           self.P2V1_L1_Data11.config (text = self.RealData['I1TDDEu']+" %")
           self.P2V1_L1_Data12.config (text = self.RealData['FreqEu']+" Hz")
         

           self.P2V1_L2_Data1.config (text = self.RealData['V2Eu'] +" V")
           self.P2V1_L2_Data2.config (text = self.RealData['I2Eu'] +" A")
           self.P2V1_L2_Data3.config (text = self.RealData['kW2Eu'] +" kW")
           self.P2V1_L2_Data4.config (text = self.RealData['kvar2Eu'] +" kvar")
           self.P2V1_L2_Data5.config (text = self.RealData['kVA2Eu'] +" kVA")
           self.P2V1_L2_Data6.config (text = self.RealData['PF2Eu'])
           self.P2V1_L2_Data7.config (text = self.RealData['V2AngEu'] +"°")
           self.P2V1_L2_Data8.config (text = self.RealData['I2AngEu'] +"°")
           self.P2V1_L2_Data9.config (text = self.RealData['V2THDEu'] +" %")
           self.P2V1_L2_Data10.config (text = self.RealData['I2THDEu']+" %")
           self.P2V1_L2_Data11.config (text = self.RealData['I2TDDEu']+" %")
           self.P2V1_L2_Data12.config (text = self.RealData['FreqEu']+" Hz")

           self.P2V1_L3_Data1.config (text = self.RealData['V3Eu'] +" V")
           self.P2V1_L3_Data2.config (text = self.RealData['I3Eu'] +" A")
           self.P2V1_L3_Data3.config (text = self.RealData['kW3Eu'] +" kW")
           self.P2V1_L3_Data4.config (text = self.RealData['kvar3Eu'] +" kvar")
           self.P2V1_L3_Data5.config (text = self.RealData['kVA3Eu'] +" kVA")
           self.P2V1_L3_Data6.config (text = self.RealData['PF3Eu'])
           self.P2V1_L3_Data7.config (text = self.RealData['V3AngEu'] +"°")
           self.P2V1_L3_Data8.config (text = self.RealData['I3AngEu'] +"°")
           self.P2V1_L3_Data9.config (text = self.RealData['V3THDEu'] +" %")
           self.P2V1_L3_Data10.config (text = self.RealData['I3THDEu']+" %")
           self.P2V1_L3_Data11.config (text = self.RealData['I3TDDEu']+" %")
           self.P2V1_L3_Data12.config (text = self.RealData['FreqEu']+" Hz")

           self.after(2000, self.RealTimeMeasurementReading)

   def Page2View2(self):
       # Create frame  
       self.P2V2_DspFrame = tk.Frame(self, height = PAGE_FRAME_HEIGHT, width = PAGE_FRAME_WIDTH,borderwidth = 2,bg = "dark grey")
       self.P2V2_DspFrame.pack(side="top", fill = "both", expand = False)

       # Page title
       self.P2V2_Title_Frame = tk.Frame(self.P2V2_DspFrame,height = PAGE_HEADER_HEIGHT, bd=1, relief='solid', bg=PAGE_HEADIING_COLOUR)
       self.P2V2_Title_Frame.pack(side = "top", fill = "both",expand = False)
       self.P2V2_Title_Frame.pack_propagate(0)
       self.P2V2_Title = tk.Label(self.P2V2_Title_Frame, text="ENGINEERING SCREEN" ,justify = "right", font=30,
                                  bg=PAGE_HEADIING_COLOUR)
       self.P2V2_Title.pack(side="left", fill = "both", anchor="n", expand=True, pady=(7,5))

       # Header button
       self.P2btn2 = tk.Button(self.P2V2_Title_Frame, height = PAGE_HEADER_HEIGHT, text="SINGLE PHASE VALUES",
                               command=lambda:[self.P2V2_DspFrame.pack_forget(),self.P2V2_DspFrame.destroy(), Page2.Page2View1(self)], bg = BUTTON_COLOUR)
       self.P2btn2.pack(anchor = "ne")
       self.P2btn2.pack_propagate(0)

       self.P2V2_Body_Frame = tk.Frame(self.P2V2_DspFrame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT),
                                       width = int(WINDOW_WIDTH))
       self.P2V2_Body_Frame.pack(side="top", anchor = "n", expand=True, fill="both")
       self.P2V2_Body_Frame.pack_propagate(0)

       self.P2V2_Body_Header_Frame = tk.Frame(self.P2V2_Body_Frame, height = 20, width = int(WINDOW_WIDTH), bd=1, relief='solid')
       self.P2V2_Body_Header_Frame.pack(side="top", anchor = "n", expand=True, fill="x")
       self.P2V2_Body_Header_Frame.pack_propagate(0)

       self.P2_TEV_L1_Header_Label = tk.Label(self.P2V2_Body_Header_Frame, text= "TOTALS", justify = "center")
       self.P2_TEV_L1_Header_Label.pack(side="top", anchor = "n", expand=False)
       
       self.P2V2_Body_Frame1 = tk.Frame(self.P2V2_Body_Frame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 20),
                                        width = int(WINDOW_WIDTH))
       self.P2V2_Body_Frame1.pack(side="top", anchor = "nw", expand=False)
       self.P2V2_Body_Frame1.pack_propagate(0)
       
       # Label frame
       self.P2V2_Col1_Black_Frame = tk.Frame(self.P2V2_Body_Frame1, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                             width = int(WINDOW_WIDTH/2 * (2/3)), bd=1, relief='solid')
       self.P2V2_Col1_Black_Frame.pack(side="left", anchor = "nw", expand=False)
       self.P2V2_Col1_Black_Frame.pack_propagate(0)

       self.P2V2_Col2_Frame = tk.Frame(self.P2V2_Body_Frame1, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                       width = int(WINDOW_WIDTH/2 * (1/3)), bd=1, relief='solid')
       self.P2V2_Col2_Frame.pack(side="left", anchor = "nw", expand=False)
       self.P2V2_Col2_Frame.pack_propagate(0)
  
       self.P2V2_Container1_Frame = tk.Frame(self.P2V2_Col2_Frame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                             width = int(WINDOW_WIDTH/2 * (1/3)))
       self.P2V2_Container1_Frame.pack(side="left", anchor = "nw", expand=False, padx=(10,0))
       self.P2V2_Container1_Frame.pack_propagate(0)

       self.P2V2_Blank_Label1 = tk.Label(self.P2V2_Container1_Frame, text= "\n", justify = "left", bd=-2)
       self.P2V2_Blank_Label1.pack(side="top", anchor = "w", expand=False)


       self.P2V2_Label1 = tk.Label(self.P2V2_Container1_Frame, text= "V1 TOTAL", justify = "left", bd=-2)
       self.P2V2_Label1.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label2 = tk.Label(self.P2V2_Container1_Frame, text= "I1", justify = "left", bd=-2)
       self.P2V2_Label2.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label3 = tk.Label(self.P2V2_Container1_Frame, text= "I2", justify = "left", bd=-2)
       self.P2V2_Label3.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label4 = tk.Label(self.P2V2_Container1_Frame, text= "I3", justify = "left", bd=-2)
       self.P2V2_Label4.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label5 = tk.Label(self.P2V2_Container1_Frame, text= "KW TOTAL", justify = "left", bd=-2)
       self.P2V2_Label5.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label6 = tk.Label(self.P2V2_Container1_Frame, text= "KVAR TOTAL", justify = "left", bd=-2)
       self.P2V2_Label6.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label7 = tk.Label(self.P2V2_Container1_Frame, text= "KVA TOTAL", justify = "left", bd=-2)
       self.P2V2_Label7.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label8 = tk.Label(self.P2V2_Container1_Frame, text= "PF TOTAL", justify = "left", bd=-2)
       self.P2V2_Label8.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label9 = tk.Label(self.P2V2_Container1_Frame, text= "IN", justify = "left", bd=-2)
       self.P2V2_Label9.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Label10 = tk.Label(self.P2V2_Container1_Frame, text= "FREQ", justify = "left", bd=-2)
       self.P2V2_Label10.pack(side="top", anchor = "w", expand=False)

       # Read meter data first time
       self.RealTimeData1 = HMI_Library.RealTimeMeasurement()
       self.RealData1 = self.RealTimeData1.DataArray()

       # 123 frame
       self.P2V2_Col4_Blank_Frame = tk.Frame(self.P2V2_Body_Frame1, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                             width = int(WINDOW_WIDTH/2 * (2/3)), bd=1, relief='solid')
       self.P2V2_Col4_Blank_Frame.pack(side="right", anchor = "nw", expand=False)
       self.P2V2_Col4_Blank_Frame.pack_propagate(0)

       self.P2V2_Col3_Frame = tk.Frame(self.P2V2_Body_Frame1, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                       width = int(WINDOW_WIDTH/2 * (1/3)), bd=1, relief='solid')
       self.P2V2_Col3_Frame.pack(side="right", anchor = "nw", expand=False)
       self.P2V2_Col3_Frame.pack_propagate(0)

       self.P2V2_Container2_Frame = tk.Frame(self.P2V2_Col3_Frame, height = int(PAGE_FRAME_HEIGHT - PAGE_HEADER_HEIGHT - 30),
                                             width = int(WINDOW_WIDTH/2 * (1/3)))
       self.P2V2_Container2_Frame.pack(side="right", anchor = "nw", expand=False, padx=(10,0))
       self.P2V2_Container2_Frame.pack_propagate(0)

       self.P2V2_Blank_Data1 = tk.Label(self.P2V2_Container2_Frame, text= "\n", justify = "left", bd=-2)
       self.P2V2_Blank_Data1.pack(side="top", anchor = "w", expand=False)

       self.P2V2_Data1 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" V", justify = "left", bd=-2)
       self.P2V2_Data1.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data2 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" A", justify = "left", bd=-2)
       self.P2V2_Data2.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data3 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" A", justify = "left", bd=-2)
       self.P2V2_Data3.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data4 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" A", justify = "left", bd=-2)
       self.P2V2_Data4.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data5 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" KW", justify = "left", bd=-2)
       self.P2V2_Data5.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data6 = tk.Label(self.P2V2_Container2_Frame, text= "0"+" KVAR", justify = "left", bd=-2)
       self.P2V2_Data6.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data7 = tk.Label(self.P2V2_Container2_Frame, text= "0" +" KVA", justify = "left", bd=-2)
       self.P2V2_Data7.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data8 = tk.Label(self.P2V2_Container2_Frame, text= "0", justify = "left", bd=-2)
       self.P2V2_Data8.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data9 = tk.Label(self.P2V2_Container2_Frame, text= "0"+" A", justify = "left", bd=-2)
       self.P2V2_Data9.pack(side="top", anchor = "w", expand=False)
       self.P2V2_Data10 = tk.Label(self.P2V2_Container2_Frame, text= "0"+" Hz", justify = "left", bd=-2)
       self.P2V2_Data10.pack(side="top", anchor = "w", expand=False)
	   
       # Read meter data continously
       self.RealTimeMeasurementReading1()
    
   def RealTimeMeasurementReading1(self):
       if self.P2V2_Data1.winfo_exists():
          print("Realtime data is being called by page2 view2s")
          self.RealTimeData1.ReadData()
          self.RealData1 = self.RealTimeData1.DataArray()      
          self.P2V2_Data1.config (text = self.RealData1['TotVEu'] +" V")
          self.P2V2_Data2.config (text = self.RealData1['I1Eu'] +" A")
          self.P2V2_Data3.config (text = self.RealData1['I2Eu'] +" A")
          self.P2V2_Data4.config (text = self.RealData1['I3Eu'] +" A")
          self.P2V2_Data5.config (text = self.RealData1['TotkWEu'] +" kW")
          self.P2V2_Data6.config (text = self.RealData1['TotkvarEu'] +" kvar")
          self.P2V2_Data7.config (text = self.RealData1['TotkVAEu'] +" kVA")
          self.P2V2_Data8.config (text = self.RealData1['TotPFEu'])
          self.P2V2_Data9.config (text = self.RealData1['INEu'] +" A")
          self.P2V2_Data10.config (text = self.RealData1['FreqEu'] +" Hz")
          self.after(2000, self.RealTimeMeasurementReading1)       

# History
class Page3(Page):
   def __init__(self, *args, **kwargs):
       Page.__init__(self, *args, **kwargs)
       print("Page3 being instanciated!")

       # Create data table display arrays
       self.DspHeaderArray = ["# ", "Time Stamp", "Para1", "Para2", "Para3", "Para4", "Para5", "Para6"]
       self.DspDataArray = [[0]*8 for i in range(18)] # 18 rows and 8 columns.
       self.TwoDArray = []

       self.DspMaxRecordNo = 1
       self.DspMaxColumnNo = 8     
       self.DspRecPointer = 1
       self.DspLogName = "Log File"
   
       # Create display frame  
       self.P3_DspFrame = tk.Frame(self, height = PAGE_FRAME_HEIGHT, width = PAGE_FRAME_WIDTH,borderwidth = 2,bg = "dark grey")
       self.P3_DspFrame.pack(side="top", fill = "both", expand = False)
       self.P3_DspFrame.pack_propagate(0)

       # Page Title
       self.P3_Title_Frame = tk.Frame(self.P3_DspFrame,height = PAGE_HEADER_HEIGHT, bd=1, relief='solid', bg=PAGE_HEADIING_COLOUR)
       self.P3_Title_Frame.pack(side = "top", fill = "both",expand = False)
       self.P3_Title_Frame.pack_propagate(0)

       self.P3_Title = tk.Label(self.P3_Title_Frame, text="HISTORY" ,justify = "right", font=30, bg=PAGE_HEADIING_COLOUR)
       self.P3_Title.pack(side="left", fill = "both", anchor="n", expand=True, pady=(7,5))
       
       # Page sub frames
       self.P3_Btn_Frame = tk.Frame(self.P3_DspFrame , height = int(WINDOW_HEIGHT-30-50), width = int(WINDOW_WIDTH*0.15),
                                    bd=1, relief='solid')
       self.P3_Btn_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P3_Btn_Frame.pack_propagate(0)  
       
       self.P3_Data_Frame= tk.Frame(self.P3_DspFrame , height = int(WINDOW_HEIGHT-30-50) , width = int(WINDOW_WIDTH*0.85),
                                    bd=1, relief='solid')
       self.P3_Data_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P3_Data_Frame.pack_propagate(0)
     
       self.P3_TableTitle = tk.Label(self.P3_Data_Frame, text="DATALOG # " ,justify = "center")
       self.P3_TableTitle.pack(side="top", expand=False,anchor="nw")
       self.P3_TableTitle.pack_propagate(0)

       # Create table frame
       self.P3_Table_Frame = tk.Frame(self.P3_Data_Frame, height = 30 , width = int(WINDOW_WIDTH*0.85), bd=1, relief='solid')
       self.P3_Table_Frame.pack(side = "top", expand = False, anchor="n")
              
       # Buttons to call datalogs  
       self.P3btn1 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="DATALOG 1",command=lambda:[self.ReadDataLog(1)], bg = BUTTON_COLOUR)
       self.P3btn1.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))

       self.P3btn2 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="DATALOG 2",command=lambda:[self.ReadDataLog(2)], bg = BUTTON_COLOUR)
       self.P3btn2.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))

       self.P3btn3 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="DATALOG 3",command=lambda:[self.ReadDataLog(3)], bg = BUTTON_COLOUR)
       self.P3btn3.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))

       self.P3btn4 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="EVENT LOG",command=lambda:[self.ReadDataLog(0)], bg = BUTTON_COLOUR)
       self.P3btn4.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))

       self.P3btn5 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="Pre PAGE ",command=lambda:[Page3.PagePrevBtn(self)], bg = BUTTON_COLOUR)
       self.P3btn5.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))

       self.P3btn6 = tk.Button(self.P3_Btn_Frame, height = 2, width = int(WINDOW_WIDTH*0.15-7*2), text="Next PAGE ",command=lambda:[Page3.PageNextBtn(self)], bg = BUTTON_COLOUR)
       self.P3btn6.pack(side="top", anchor = "n", fill="x", pady=(7,7), padx=(7,7))
 
       # Create instances of datalogger
       self.DataLoggerInstance = HMI_Library.DataLogger()
       self.EventLoggerInstance = HMI_Library.EventLogger()

   def __del__(self, *args, **kwargs):
       print("Page3 being automatically destroyed. Goodbye!")

   # Read datalog      
   def ReadDataLog(self, LogNo):
       self.DataLoggerNo = LogNo
       self.DspRecPointer = 1
       self.DspMaxRecordNo = 0
       self.DspMaxColumnNo = 8     
       self.TwoDArray.clear()
       

       if self.DataLoggerNo == 0:
           self.DspLogName = "EVENTLOG"
           self.DspHeaderArray = ["#","Time Stamp", "Cause", "Source", "Effect","","","",""]

           # Read event log
           self.EventLoggerInstance.ReadEventlogger()
           self.TwoDArray = self.EventLoggerInstance.GetDataMatrix()
           self.DspMaxRecordNo = len(self.TwoDArray)
           self.DspMaxColumnNo = len(self.TwoDArray[0])
          
       else:
           self.DspLogName = "DATALOG"+str(self.DataLoggerNo)
           self.DspHeaderArray = ["#","Time Stamp", "Total kW", "Peak kW", "Shoulder kW", "Off Peak kW", "Mx Dmd kW", "Mx Dmd kVA"]
           self.DataLoggerInstance.ReadDatalogger(self.DataLoggerNo)
           self.TwoDArray = self.DataLoggerInstance.GetDataMatrix()
           self.DspMaxRecordNo = len(self.TwoDArray)
           self.DspMaxColumnNo = len(self.TwoDArray[0])

       self.P3_TableTitle.config (text = self.DspLogName+ ": (No Records:"+ str(self.DspMaxRecordNo) +")")          
       self.UpdateDisplayArray()# Prepare table
       self.DisplayTable()#Display table

   def UpdateDisplayArray(self):
        self.DummyArray =  (self.TwoDArray).copy()

        # Copy data int to display array
        for r in range (0,18):
           if self.DspRecPointer <= self.DspMaxRecordNo:
               self.DspDataArray[r] =  self.DummyArray[self.DspRecPointer -1]
               self.DspRecPointer = self.DspRecPointer + 1
           else:
               self.DspDataArray[r] = ["","","","","","","","",""]
               self.DspRecPointer = self.DspRecPointer + 1


   def EmptyDspArray(self):
       for r in range(18):
           for c in range (8):
               self.DspDataArray[r][c] = ""
   
   def DisplayTable(self):
       if self.DataLoggerNo == 0:
           DataWidth = 20 # Event log field width
           DataAnchor = 'nw'
       else:
           DataWidth = 10 # 123 log field width
           DataAnchor = 'ne'
           
       #Display table header
       for c in range(8):
                self.P3_Data_Label = tk.Label(self.P3_Table_Frame, font=(None, 8))
                self.P3_Data_Label.grid(row=0, column=c)
                if c == 0:
                    cWidth = 3 # First column width
                elif c == 1:
                    cWidth = 17# Time stamp column width
                else:
                    cWidth = DataWidth# data column width
                self.P3_Data_Label.config(width=cWidth, bd=1, relief='solid') 
                self.P3_Data_Label.config(text = self.DspHeaderArray[c])

       for r in range(18):
           for c in range (8):
                if c ==0:
                    self.P3_Data_Label = tk.Label(self.P3_Table_Frame, font=(None, 8))
                    self.P3_Data_Label.grid(row=r+1, column=c)
                else:
                    self.P3_Data_Label = tk.Label(self.P3_Table_Frame,bg = 'white', font=(None, 8))
                    self.P3_Data_Label.grid(row=r+1, column=c)                    
                
                if c == 0:
                    cWidth = 3 # First column width
                elif c == 1:
                    cWidth = 17# Time stamp column width
                else:
                    cWidth = DataWidth# data column width
                self.P3_Data_Label.config(width=cWidth, bd=1, relief='solid',anchor=DataAnchor) 
                self.P3_Data_Label.config(text = self.DspDataArray[r][c])              


   def PageNextBtn(self):
       if  self.DspRecPointer > self.DspMaxRecordNo:
           return
       else: 
           self.UpdateDisplayArray()# Prepare table
           self.DisplayTable()# Display table


   def PagePrevBtn(self):
       if  self.DspRecPointer <= 19:
           return
       else: 
           self.DspRecPointer = self.DspRecPointer - 36
           self.UpdateDisplayArray()# Prepare table
           self.DisplayTable()# Display table


# Settings
class Page4(Page):
   def __init__(self, *args, **kwargs):
       Page.__init__(self, *args, **kwargs)
       print("Page4 being instanciated!")

       self.NetworkConfig = HMI_Library.ModbusNetworkConfiguration()
       global PageTittle
       PageTitle.config(text = HMI_Library.METER_MODELNAME)
       
       # Create display are frame
       self.P4_DspFrame = tk.Frame(self, height = PAGE_FRAME_HEIGHT, width = PAGE_FRAME_WIDTH,borderwidth = 2,bg = "dark grey")
       self.P4_DspFrame.pack(side="top", fill = "both", expand = False)
       self.P4_DspFrame.pack_propagate(0)      
       
       # Page title
       self.P4_Title_Frame = tk.Frame(self.P4_DspFrame,height = PAGE_HEADER_HEIGHT, bd=1, relief='solid', bg=PAGE_HEADIING_COLOUR)
       self.P4_Title_Frame.pack(side = "top", fill = "both",expand = False)
       self.P4_Title_Frame.pack_propagate(0)
       self.P4_Title = tk.Label(self.P4_Title_Frame, text="SETTINGS" ,justify = "right", font=30, bg=PAGE_HEADIING_COLOUR)
       self.P4_Title.pack(side="left", fill = "both", anchor="n", expand=True, pady=(7,5))

       # Exit button
       self.Exitbtn = tk.Button(self.P4_Title_Frame, height = PAGE_HEADER_HEIGHT, width = 10, text="EXIT", command=lambda value=self.NetworkConfig.CloseProgram: PasswordWindow.create(value), bg= BUTTON_COLOUR)
       self.Exitbtn.pack(anchor = "ne")
       self.Exitbtn.pack_propagate(0)
       
       # Create body1 frame
       self.P4_B1_Container_Frame = tk.Frame(self.P4_DspFrame , height = int(PAGE_FRAME_HEIGHT-PAGE_HEADER_HEIGHT), width = int(WINDOW_WIDTH/2))
       self.P4_B1_Container_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P4_B1_Container_Frame.pack_propagate(0)

       self.P4_B1_Title_Frame = tk.Frame(self.P4_B1_Container_Frame , height = int(20), width = int(WINDOW_WIDTH/2), bd=1, relief='solid')
       self.P4_B1_Title_Frame.pack(side = "top", expand = False, anchor="nw")
       self.P4_B1_Title_Frame.pack_propagate(0)
       
       self.P4_B1_Title = tk.Label(self.P4_B1_Title_Frame, text="CURRENT SETTINGS", justify = "center")
       self.P4_B1_Title.pack(side="top", expand=False) 
       self.P4_B1_Title.pack_propagate(0)

       self.P4_B1_Frame = tk.Frame(self.P4_B1_Container_Frame , height = int(PAGE_FRAME_HEIGHT-PAGE_HEADER_HEIGHT-20), width = int(WINDOW_WIDTH/2), bd=1, relief='solid')
       self.P4_B1_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P4_B1_Frame.pack_propagate(0)

       # Build body1 for displaying current network settings  
       self.P4_Col1_Height_var= int((PAGE_FRAME_HEIGHT-PAGE_HEADER_HEIGHT-20-20)/4)
       self.P4_Col2_Height_var= int((PAGE_FRAME_HEIGHT-self.P4_Col1_Height_var))


       self.P4_S1_C1_Data1 = ["IP", "SUBNET", "GATEWAY"]
       self.P4_S1_C1_Data2 = ["IP", "SUBNET", "GATEWAY","MODBUS NODE"]
       self.P4_S1_C1_Data3 = ["CT RATIO","SERIAL NO","FIRMWARE", "BOOT"]
       self.P4_S1_C1_Data4 = ["IP", "MODBUS NODE"]
       self.pady_bottom = 5
 
       # Labels frame
       self.P4_B1_Label1 = tk.Label(self.P4_B1_Frame, text="HMI:", justify = "left")
       self.P4_B1_Label1.pack(side="top", expand=False, anchor="nw")
       
       self.P4_B1_HMI_Container_Frame = tk.Frame(self.P4_B1_Frame)
       self.P4_B1_HMI_Container_Frame.pack(side = "top", expand = False, anchor="nw", pady=(7,self.pady_bottom), padx=(10,0))
       self.P4_B1_HMI_Col1_Container_Frame = tk.Frame(self.P4_B1_HMI_Container_Frame, height = self.P4_Col1_Height_var, width = int(WINDOW_WIDTH/4))
       self.P4_B1_HMI_Col1_Container_Frame.pack(side = "left", expand = False, anchor="w")
       self.P4_B1_HMI_Col1_Container_Frame.pack_propagate(0)
       self.P4_B1_HMI_Col2_Container_Frame = tk.Frame(self.P4_B1_HMI_Container_Frame)
       self.P4_B1_HMI_Col2_Container_Frame.pack(side = "top", expand = False, anchor="w")

       self.P4_B1_Label2 = tk.Label(self.P4_B1_Frame, text="METER:", justify = "left")
       self.P4_B1_Label2.pack(side="top", expand=False, anchor="nw")

       self.P4_B1_Meter_Container_Frame = tk.Frame(self.P4_B1_Frame)
       self.P4_B1_Meter_Container_Frame.pack(side = "top", expand = False, anchor="nw", pady=(7,0), padx=(7,0))
       self.P4_B1_Meter_Col1_Container_Frame = tk.Frame(self.P4_B1_Meter_Container_Frame, height = self.P4_Col2_Height_var , width = int(WINDOW_WIDTH/4))
       self.P4_B1_Meter_Col1_Container_Frame.pack(side = "left", expand = False, anchor="w")
       self.P4_B1_Meter_Col1_Container_Frame.pack_propagate(0)
       self.P4_B1_Meter_Col2_Container_Frame = tk.Frame(self.P4_B1_Meter_Container_Frame)
       self.P4_B1_Meter_Col2_Container_Frame.pack(side = "top", expand = False, anchor="w")

       # HMI
       for data in self.P4_S1_C1_Data1 :
           tk.Label(self.P4_B1_HMI_Col1_Container_Frame, text= data, justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")

       self.P4_B1_S1_C2_Label1 = tk.Label(self.P4_B1_HMI_Col2_Container_Frame, text= '0.0.0.0', justify = "left")
       self.P4_B1_S1_C2_Label1.config(text= self.NetworkConfig.GetHMIIPAddressStr())
       self.P4_B1_S1_C2_Label1.pack(side = "top", pady=(2,2), anchor = "nw")
       self.P4_B1_S1_C2_Label2 = tk.Label(self.P4_B1_HMI_Col2_Container_Frame, text= '0.0.0.0', justify = "left", bd=-2)     
       self.P4_B1_S1_C2_Label2.config (text= self.NetworkConfig.GetHMISubnetStr())
       self.P4_B1_S1_C2_Label2.pack(side = "top", pady=(2,2), anchor = "nw")
       self.P4_B1_S1_C2_Label3 = tk.Label(self.P4_B1_HMI_Col2_Container_Frame, text= '0.0.0.0', justify = "left", bd=-2)
       self.P4_B1_S1_C2_Label3.config  (text= self.NetworkConfig.GetHMIGatewayStr())
       self.P4_B1_S1_C2_Label3.pack(side = "top", pady=(2,2), anchor = "nw")

       # METER
       for data in self.P4_S1_C1_Data2 :
           tk.Label(self.P4_B1_Meter_Col1_Container_Frame, text= data, justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")
    
       self.P4_B1_S2_C2_Label1 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text= '0.0.0.0', justify = "left", bd=-2)    
       self.P4_B1_S2_C2_Label1.config(text= self.NetworkConfig.GetMeterIPAddressStr())
       self.P4_B1_S2_C2_Label1.pack(side = "top", pady=(2,2), anchor = "nw")
       self.P4_B1_S2_C2_Label2 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text= '0.0.0.0', justify = "left", bd=-2)    
       self.P4_B1_S2_C2_Label2.config(text= HMI_Libray.METER_SUBNET)
       self.P4_B1_S2_C2_Label2.pack(side = "top", pady=(2,2), anchor = "nw")
       self.P4_B1_S2_C2_Label3= tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text= '0.0.0.0', justify = "left", bd=-2)    
       self.P4_B1_S2_C2_Label3.config(text= HMI_Libray.METER_GATEWAY)
       self.P4_B1_S2_C2_Label3.pack(side = "top", pady=(2,2), anchor = "nw")
       self.P4_B1_S2_C2_Label4 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text= "1", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Label4.config(text= self.NetworkConfig.GetMeterNodeAddressStr())
       self.P4_B1_S2_C2_Label4.pack(side = "top", pady=(2,2), anchor = "nw")

       tk.Label(self.P4_B1_Meter_Col1_Container_Frame, text= "", justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")
       
       for data in self.P4_S1_C1_Data3 :
           tk.Label(self.P4_B1_Meter_Col1_Container_Frame, text= data, justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")

       self.P4_B1_S2_C2_Black_Label = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text= "", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Black_Label.pack(side = "top", anchor = "nw")

       self.P4_B1_S2_C2_Label3 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text="XXXXXXX", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Label3.config(text= HMI_Library.METER_CTRATIO)
       self.P4_B1_S2_C2_Label3.pack(side = "top", anchor = "nw")
       self.P4_B1_S2_C2_Label4 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text="XXX", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Label4.config(text= HMI_Library.METER_SNO)    
       self.P4_B1_S2_C2_Label4.pack(side = "top", anchor = "nw")
       self.P4_B1_S2_C2_Label5 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text="XX.XX.XX.XX", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Label5.config(text= HMI_Library.METER_FIRMWARE)        
       self.P4_B1_S2_C2_Label5.pack(side = "top", anchor = "nw")
       self.P4_B1_S2_C2_Label6 = tk.Label(self.P4_B1_Meter_Col2_Container_Frame, text="XX.XX", justify = "left", bd=-2)
       self.P4_B1_S2_C2_Label6.config(text= HMI_Library.METER_BOOT)
       self.P4_B1_S2_C2_Label6.pack(side = "top", anchor = "nw")

       # Create body2 frame
       self.P4_B2_Container_Frame = tk.Frame(self.P4_DspFrame , height = int(PAGE_FRAME_HEIGHT-PAGE_HEADER_HEIGHT), width = int(WINDOW_WIDTH/2))
       self.P4_B2_Container_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P4_B2_Container_Frame.pack_propagate(0)

       self.P4_B2_Title_Frame = tk.Frame(self.P4_B2_Container_Frame , height = int(20), width = int(WINDOW_WIDTH/2), bd=1, relief='solid')
       self.P4_B2_Title_Frame.pack(side = "top", expand = False, anchor="nw")
       self.P4_B2_Title_Frame.pack_propagate(0)

       self.P4_B2_Title = tk.Label(self.P4_B2_Title_Frame, text="MODIFY SETTINGS", justify = "center")
       self.P4_B2_Title.pack(side="top", expand=False)
       self.P4_B2_Title.pack_propagate(0)

       self.P4_B2_Frame = tk.Frame(self.P4_B2_Container_Frame , height = int(PAGE_FRAME_HEIGHT-PAGE_HEADER_HEIGHT-20), width = int(WINDOW_WIDTH/2), bd=1, relief='solid')
       self.P4_B2_Frame.pack(side = "left", expand = False, anchor="nw")
       self.P4_B2_Frame.pack_propagate(0)

       #Display Modify Settings
       self.P4_B2_Label1 = tk.Label(self.P4_B2_Frame, text="HMI:", justify = "left")
       self.P4_B2_Label1.pack(side="top", expand=False, anchor="nw")

       #Display Body2 Labels
       self.P4_B2_HMI_Container_Frame = tk.Frame(self.P4_B2_Frame)
       self.P4_B2_HMI_Container_Frame.pack(side = "top", expand = False, anchor="nw", pady=(7,self.pady_bottom), padx=(7,0))
       self.P4_B2_HMI_Col1_Container_Frame = tk.Frame(self.P4_B2_HMI_Container_Frame, height = self.P4_Col1_Height_var, width = int(WINDOW_WIDTH/4))
       self.P4_B2_HMI_Col1_Container_Frame.pack(side = "left", expand = False, anchor="w")
       self.P4_B2_HMI_Col1_Container_Frame.pack_propagate(0)
       self.P4_B2_HMI_Col2_Container_Frame = tk.Frame(self.P4_B2_HMI_Container_Frame)
       self.P4_B2_HMI_Col2_Container_Frame.pack(side = "top", expand = False, anchor="w")

       self.P4_B2_Label2 = tk.Label(self.P4_B2_Frame, text="METER:", justify = "left")
       self.P4_B2_Label2.pack(side="top", expand=False, anchor="nw")

       self.P4_B2_Meter_Container_Frame = tk.Frame(self.P4_B2_Frame)
       self.P4_B2_Meter_Container_Frame.pack(side = "top", expand = False, anchor="nw", pady=(7,0), padx=(7,0))
       self.P4_B2_Meter_Col1_Container_Frame = tk.Frame(self.P4_B2_Meter_Container_Frame, height = self.P4_Col1_Height_var, width = int(WINDOW_WIDTH/4))
       self.P4_B2_Meter_Col1_Container_Frame.pack(side = "left", expand = False, anchor="w")
       self.P4_B2_Meter_Col1_Container_Frame.pack_propagate(0)
       self.P4_B2_Meter_Col2_Container_Frame = tk.Frame(self.P4_B2_Meter_Container_Frame)
       self.P4_B2_Meter_Col2_Container_Frame.pack(side = "top", expand = False, anchor="w")


       for data in self.P4_S1_C1_Data1 :
           tk.Label(self.P4_B2_HMI_Col1_Container_Frame, text= data, justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")
       
       self.Line1Textbox = tk.Entry(self.P4_B2_HMI_Col2_Container_Frame, highlightbackground="gray40", width = 15, bg = "white", bd=-2, justify='center')
       self.Line1Textbox.pack(side = "top", pady=(2,2), anchor = "nw")
       self.Line2Textbox = tk.Entry(self.P4_B2_HMI_Col2_Container_Frame, highlightbackground="gray40", width = 15, bg = "white", bd=-2, justify='center')
       self.Line2Textbox.pack(side = "top", pady=(2,2), anchor = "nw")
       self.Line3Textbox = tk.Entry(self.P4_B2_HMI_Col2_Container_Frame, highlightbackground="gray40", width = 15, bg = "white", bd=-2, justify='center')
       self.Line3Textbox.pack(side = "top", pady=(2,2), anchor = "nw")


       for data in self.P4_S1_C1_Data4 :
           tk.Label(self.P4_B2_Meter_Col1_Container_Frame, text= data, justify = "left", anchor='w').pack(side="top", anchor = "nw", expand=False, fill="x")

       self.Line4Textbox = tk.Entry(self.P4_B2_Meter_Col2_Container_Frame, highlightbackground="gray40", width = 15, bg = "white", bd=-2, justify='center')
       self.Line4Textbox.pack(side = "top", pady=(2,2), anchor = "nw")
       self.Line5Textbox = tk.Entry(self.P4_B2_Meter_Col2_Container_Frame, highlightbackground="gray40", width = 3, bg = "white", bd=-2, justify='center')
       self.Line5Textbox.pack(side = "top", pady=(2,2), anchor = "nw")

       # Display current settings as default values for entries
       self.Line1Textbox.insert(0, self.NetworkConfig.GetHMIIPAddressStr())
       self.Line2Textbox.insert(0, self.NetworkConfig.GetHMISubnetStr())
       self.Line3Textbox.insert(0, self.NetworkConfig.GetHMIGatewayStr())
       self.Line4Textbox.insert(0, self.NetworkConfig.GetMeterIPAddressStr())
       self.Line5Textbox.insert(0, self.NetworkConfig.GetMeterNodeAddressStr())

       global NumberPadWindow
       
       self.Line1Textbox.bind("<ButtonRelease-1>", lambda event, arg=self.Line1Textbox: NumberPadWindow.create(arg))
       self.Line2Textbox.bind("<ButtonRelease-1>", lambda event, arg=self.Line2Textbox: NumberPadWindow.create(arg))
       self.Line3Textbox.bind("<ButtonRelease-1>", lambda event, arg=self.Line3Textbox: NumberPadWindow.create(arg))
       self.Line4Textbox.bind("<ButtonRelease-1>", lambda event, arg=self.Line4Textbox: NumberPadWindow.create(arg))
       self.Line5Textbox.bind("<ButtonRelease-1>", lambda event, arg=self.Line5Textbox: NumberPadWindow.create(arg))

       global PasswordWindow

       

       #Create apply button
       self.Applybtn = tk.Button(self.P4_B2_Frame, height = 2, width = BUTTON_WIDTH, text="Apply", command=lambda value=self.OnClickSubmit1: PasswordWindow.create(value), bg = BUTTON_COLOUR)
       self.Applybtn.pack(side="bottom", anchor = "s", fill="x", pady=(7,7), padx=(7,7))


   def OnClickSubmit1(self):   
       Ip1  =     self.Line1Textbox.get()
       Sub1 =     self.Line2Textbox.get()
       Gateway1 = self.Line3Textbox.get()
       Ip2 =      self.Line4Textbox.get()
       Node =     self.Line5Textbox.get()

       NumberPadWindow.destroyNumberPad()       
       self.NetworkConfig.SetNetwork(Ip1, Sub1, Gateway1, Ip2,Node)

       self.Line1Textbox.delete(0,"end")
       self.Line2Textbox.delete(0,"end")
       self.Line3Textbox.delete(0,"end")
       self.Line4Textbox.delete(0,"end")
       self.Line5Textbox.delete(0,"end")

       self.Line1Textbox.insert(0, self.NetworkConfig.GetHMIIPAddressStr())
       self.Line2Textbox.insert(0, self.NetworkConfig.GetHMISubnetStr())
       self.Line3Textbox.insert(0, self.NetworkConfig.GetHMIGatewayStr())
       self.Line4Textbox.insert(0, self.NetworkConfig.GetMeterIPAddressStr())
       self.Line5Textbox.insert(0, self.NetworkConfig.GetMeterNodeAddressStr())

#     tk.messagebox.showerror("Error","Enter Valid IP/Modbus Address")
       self.P4_B1_S1_C2_Label1.config(text= self.NetworkConfig.GetHMIIPAddressStr())
       self.P4_B1_S1_C2_Label1.pack(side="top", anchor = "w", expand=False)
       self.P4_B1_S1_C2_Label2.config (text= self.NetworkConfig.GetHMISubnetStr())
       self.P4_B1_S1_C2_Label2.pack(side="top", anchor = "w", expand=False)
       self.P4_B1_S1_C2_Label3.config  (text= self.NetworkConfig.GetHMIGatewayStr() + "\n")
       self.P4_B1_S1_C2_Label3.pack(side="top", anchor = "w", expand=False)	   


       self.P4_B1_S2_C2_Label1.config(text= self.NetworkConfig.GetMeterIPAddressStr())
       self.P4_B1_S2_C2_Label1.pack(side="top", anchor = "w", expand=False)
       self.P4_B1_S2_C2_Label2.config(text= self.NetworkConfig.GetMeterNodeAddressStr())
       self.P4_B1_S2_C2_Label2.pack(side="top", anchor = "w", expand=False)     

#Numberpad
class NumberPad:
    def __init__(self, root):
        self.alphabets = [['1','2','3','4','5'],['6','7','8','9','0'],['.','Backspace','Enter']]
        self.windowState = False
        self.root = root
        self.entryprev = 'None'
        self.entryBox = 'None'

    def destroyNumberPad(self):
        if self.windowState:
            self.window.destroy()
            self.windowState = False

    def create(self, entry):
        entry.delete(0, 'end')
        entry.config(fg = "black")
        if entry != self.entryprev or self.entryprev == 'None':
            self.entryBox = entry
        if self.windowState and self.entryprev != 'None' and self.entryBox != self.entryprev:
            self.window.destroy()
            self.windowState = False
        if not self.windowState:
            self.windowState = True
            self.entryprev = self.entryBox
            self.window = tk.Toplevel(self.root)
            self.window.attributes('-type', 'dock')
            self.window.attributes('-topmost', 'true')
            self.window.lift() 

            position = '+' + str(int(WINDOW_WIDTH/2) + 80) + '+' + str(310)
            self.window.geometry(str(position))
            self.window.configure(background="black")
            self.window.title("Enter Address")

          
            for y, row in enumerate(self.alphabets):
                x = 0
                for text in row:
                    if text in ('Backspace','Enter'):
                        columnspan = 2
                        width = 19
                        padx = 0
                    else:
                        columnspan = 1
                        width = 9
                        padx = 0
                    tk.Button(self.window, text=text, width=width, command=lambda value=text: self.select(self.entryBox, value)
                    ,padx=padx, pady=1, bg="light grey", fg="black", font=(None, 8)).grid(row=y, column=x, columnspan=columnspan)
                    x += columnspan
        else:
            self.window.lift() 

    def select(self, entry, value):
        if value == "Backspace":
            if isinstance(entry, tk.Entry):
                entry.delete(len(entry.get())-1, 'end')
            else:
                entry.delete('end - 2c', 'end')
        elif value == 'Enter':
            self.window.destroy()
            self.windowState = False
        elif len(entry.get()) > 15:
            pass
        else:
            entry.insert('end', value)

#Authentication
class Authentication:
    def __init__(self,root, NumberpadWindow):
        self.ExitWindowState = False
        self.NumberpadWindow = NumberpadWindow
        self.PaddingWindow = 30
        self.root = root

    def create(self, func):
        self.NumberpadWindow.destroyNumberPad()
        if not self.ExitWindowState:
            self.ExitWindowState = True
            self.ExitWindow = tk.Toplevel(self.root)
            self.ExitWindow.title("Exit Window")
            self.ExitWindow.geometry('+250+180')
            self.ExitWindow.configure(highlightbackground="gray40", highlightthickness=3)
            self.ExitWindow.attributes('-type', 'dock')
            self.ExitWindow.attributes('-topmost', 'true')

            ExitLine1 = tk.Frame(self.ExitWindow,height = 5)
            ExitLine1.pack(side ="top", fill="both",expand=False, pady = (15,5), padx = (self.PaddingWindow,self.PaddingWindow))
            ExitLine2 = tk.Frame(self.ExitWindow,height = 5)
            ExitLine2.pack(side ="top", fill="both",expand=False, pady = (1,0), padx = (self.PaddingWindow,self.PaddingWindow))
            ExitLine3 = tk.Frame(self.ExitWindow,height = 5)
            ExitLine3.pack(side ="top", fill="both",expand=False, pady = (1,0), padx = (self.PaddingWindow,self.PaddingWindow))
            
            label = tk.Label(ExitLine1, text="Password :",justify = "right")
            label.pack(side="left", fill="x", expand=False, anchor = "nw")

            self.PwdTextbox = tk.Entry(ExitLine1, width = 7, highlightbackground="gray40", bg = "white", borderwidth = 0, show="*")
            self.PwdTextbox.pack(side = "left", fill="x", padx = (1,1))
            self.PwdTextbox.bind("<ButtonRelease-1>", lambda event, arg=self.PwdTextbox: self.NumberpadWindow.create(arg))
            
            CancelBtn = tk.Button(ExitLine2, height = 1, width = 5, text="Cancel", command = self.ExitPasswordWindow)
            CancelBtn.pack(side="left", fill = "x")
            OkBtn = tk.Button(ExitLine2, height = 1, width = 5, text="Ok", command=lambda value=func: self.OnClickOkBtn(value))
            OkBtn.pack(side="left", fill = "x")

            self.Errlabel = tk.Label(ExitLine3,justify = "right")
            self.Errlabel.pack(side="left", fill="x", expand=False, anchor = "nw")
        else:
            self.ExitWindow.lift()
            self.PwdTextbox.delete(0, 'end') 
            self.Errlabel.configure(text="")
            self.NumberpadWindow.destroyNumberPad()

    def ExitPasswordWindow(self):
        self.NumberpadWindow.destroyNumberPad()
        self.ExitWindow.destroy()
        self.ExitWindowState = False
    
    def OnClickOkBtn(self, func):
        if self.PwdTextbox.get() == "9":
            self.ExitPasswordWindow()
            func()
        else:
            self.PwdTextbox.delete(0, 'end') 
            self.Errlabel.configure(text="Wrong Password!")

# the main frame that encloses the full window 
class MainView(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)

        global NumberPadWindow
        NumberPadWindow = NumberPad(self)

        global PasswordWindow
        PasswordWindow = Authentication(self, NumberPadWindow)

        #Create top menu frame
        self.Headerframe = tk.Frame(self,height = MENU_HEIGHT, width=WINDOW_WIDTH, borderwidth = 1,bg = 'dark grey')
        self.Headerframe.pack(side = "top", fill = "both", expand = False)
        self.Headerframe.pack_propagate(0)

        # Add buttons       
        self.Menubtn1 = tk.Button(self.Headerframe, height = 1, width = BUTTON_WIDTH, bg = BUTTON_COLOUR, text="BILLING",command=lambda:[self.p1.lift(), NumberPadWindow.destroyNumberPad()])
        self.Menubtn1.pack(side="left", anchor = "nw", fill = "y")
        self.Menubtn2 = tk.Button(self.Headerframe, height = 1, width = BUTTON_WIDTH, bg = BUTTON_COLOUR, text="ENGINEERING",command=lambda:[self.p2.lift(), NumberPadWindow.destroyNumberPad()])
        self.Menubtn2.pack(side="left", anchor = "nw", fill = "y")
        self.Menubtn3 = tk.Button(self.Headerframe, height = 1, width = BUTTON_WIDTH, bg = BUTTON_COLOUR, text="HISTORY",command=lambda:[self.p3.lift(), NumberPadWindow.destroyNumberPad()])
        self.Menubtn3.pack(side="left", anchor = "nw", fill = "y")
        self.Menubtn4 = tk.Button(self.Headerframe, height = 1, width = BUTTON_WIDTH, bg = BUTTON_COLOUR, text="SETTINGS",command=lambda:[self.p4.lift(), NumberPadWindow.destroyNumberPad()])
        self.Menubtn4.pack(side="left", anchor = "nw", fill = "y")

        #Page title didget
        global PageTitle
        PageTitle = tk.Label(self.Headerframe, width = 10, text = HMI_Library.METER_MODELNAME, justify = "center",font= '-weight bold')
        PageTitle.pack(side="left", fill = "both", expand=True)
        
        
# Logo
        # Get relative path to logo
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.Logo_Full_Path = os.path.join(__location__, 'Logo.bmp')
        # Get logo image
        self.Logo_File = Image.open(self.Logo_Full_Path)
        # Resize
        self.Logo_Width, self.Logo_Height = self.Logo_File.size
        self.Logo_File = self.Logo_File.resize((int(self.Logo_Width / self.Logo_Height * 30), 30), Image.ANTIALIAS)
        self.Logo_File = ImageTk.PhotoImage(self.Logo_File)
        # Place to Header
        self.Logo_Label = tk.Label(self.Headerframe, justify = "right", image = self.Logo_File)
        self.Logo_Label.pack(anchor = "e")


#Page Frames

        #Build Page Frames
        self.p1 = Page1(self)
        self.p2 = Page2(self)
        self.p3 = Page3(self)
        self.p4 = Page4(self)
        
        # Create Containers for Page Frames
        self.container = tk.Frame(self,height = PAGE_FRAME_HEIGHT, borderwidth = 1)
        self.container.pack(side="top", fill="both", expand=True)

        #Place page frames in to container
        self.p1.place(in_=self.container, x=0, y=0, relwidth=1, relheight=1)
        self.p2.place(in_=self.container, x=0, y=0, relwidth=1, relheight=1)
        self.p3.place(in_=self.container, x=0, y=0, relwidth=1, relheight=1)
        self.p4.place(in_=self.container, x=0, y=0, relwidth=1, relheight=1)

        self.p1.show() # page 1 is default first view


if __name__ == "__main__":
    # creates instance for root window
    root = tk.Tk()
    #TODO undo comment
    root.attributes('-type', 'dock')

    root.title(TITTLE_BAR)
    root.geometry(WINDOW_GEOMETRY)
   
    main = MainView(root)
    
    main.pack(side="top", fill="both", expand=True)

    root.mainloop()



   

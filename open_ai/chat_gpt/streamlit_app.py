#! pip install streamlit
import calendar
from datetime import datetime
import streamlit as st

## Settings
incomes = ['Salary', 'Blog', 'Other Income']
expenses = ['Rent', 'Utilities', 'Groceries', 'Car', 'Other Expenses', 'Saving']
currency = 'USD'
page_title = 'Income and Expense Tracker'
page_icon = ':money_with_wings'
layout = 'centered'

st.set_page_config(page_title=page_title, page_icon= page_icon, layout= layout)
st.title(page_title+ " "+ page_icon)

years = [datetime.today().year, datetime.today().year-1]
months = list(calendar.month_name[1:])

st.header(f'Data Entry in {currency}')
with st.form('entry form', clear_on_submit= True):
    col1, col2 = st.columns(2)
    col1.selectbox('Select Month: ', months, key = 'month')
    col2.selectbox('Select Year: ', years, key = 'year')
    
    
    with st.expander('Income'):
        for income in incomes:
            st.number_input(f'{income}:', min_value = 0, format = '%i', step = 10, key = income)
    with st.expander('Expense'):
        for expense in expenses:
            st.number_input(f'{expense}:', min_value = 0, format = '%i', step = 10, key = expense)
    with st.expander('Comments'):
        comment = st.text_area("", placeholder = 'Entre a comment here')  
        
    submitted = st.form_submit_button('Save Data')
    if submitted:
        period = str(st.session_state['year']) +'_'+ str(st.session_state['month'])
        incomes = {income: st.session_state[income] for income in incomes} 
        expenses = {expense: st.session_state[expense] for expense in expenses}
        
        st.write(f'incomes: {incomes}')
        st.write(f'expenses: {expenses}')
        st.write(f'Comentarios: {comment}')   
        st.success('Data saved!')
        
        
        
         
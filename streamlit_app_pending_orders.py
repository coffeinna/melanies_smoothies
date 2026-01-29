# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col,when_matched



# Write directly to the app
st.title(f":cup_with_straw: Smoothie! :cup_with_straw: ") #{st.__version__}
st.write("""To do:""")

cnx=st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()


submitted = st.button('Submit!')
if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    if submitted:
        st.success("Done!", icon = '👍')
        
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        og_dataset.merge(edited_dataset
                         , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                         , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                        )
else:
    st.success('There are no pending orders.', icon = '👍')

st.write("""Done orders:""")
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==1).collect()
st.dataframe(data=my_dataframe, use_container_width=True)


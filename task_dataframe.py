import pandas as pd

from custom_exc import RequiredColumnsException


def add_session_id_to_dataframe(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must have pandas.DataFrame type')
    if 'customer_id' not in df and 'timestamp' not in df:
        raise RequiredColumnsException('not found required columns: "customer_id" or "timestamp"')

    customers = df['customer_id'].unique().tolist()
    for customer in customers:
        mask = df['customer_id'] == customer
        session_change_list = df.loc[mask, 'timestamp'].sort_values().diff().dt.total_seconds() > 180
        session_id_list = []
        session_count = 0
        for session_change in session_change_list.tolist():
            session_count = session_count + 1 if session_change else session_count
            session_id_list.append(session_count)
        df.loc[mask, 'session_id'] = pd.Series(session_id_list, index=df.loc[mask].index, dtype=object)

import pandas as pd
import pytest

from custom_exc import RequiredColumnsException
from task_dataframe import add_session_id_to_dataframe


class TestAddSessionFunction:
    df_positive = pd.DataFrame({
        'customer_id': [1, 2, 3,
                        1, 2, 3,
                        1, 2, 3,

                        1, 2, 3,
                        1, 2, 3,
                        1, 2, 3,

                        1, 2, 3,
                        1, 2, 3,
                        1, 2, 3,

                        1, 2, 3,
                        1, 2, 3,
                        1, 2, 3],
        'timestamp': pd.to_datetime(['2020-02-14 06:00:00', '2020-03-14 06:20:00', '2020-04-05 10:07:00',
                                     '2020-02-14 06:05:00', '2020-03-14 06:21:00', '2020-04-05 10:15:00',
                                     '2020-02-14 06:07:00', '2020-03-14 06:25:00', '2020-04-05 10:19:00',

                                     '2020-03-20 17:00:00', '2020-05-01 10:25:00', '2020-06-25 15:19:00',
                                     '2020-03-20 17:02:00', '2020-05-01 10:25:30', '2020-06-25 15:22:00',
                                     '2020-03-20 17:32:00', '2020-05-01 10:26:30', '2020-06-25 15:46:00',

                                     '2020-04-02 12:00:00', '2020-07-10 11:05:00', '2020-10-11 21:36:00',
                                     '2020-04-02 16:07:00', '2020-07-10 11:08:00', '2020-10-11 21:46:00',
                                     '2020-04-02 16:08:00', '2020-07-10 11:15:00', '2020-10-11 21:48:00',

                                     '2020-05-30 06:07:00', '2020-08-16 22:45:00', '2020-12-14 06:19:00',
                                     '2020-05-30 06:08:00', '2020-08-16 22:49:00', '2020-12-14 06:45:00',
                                     '2020-05-30 06:18:00', '2020-08-16 22:55:00', '2020-12-14 06:46:00'])
    })

    df_one_negative = pd.DataFrame({
        'customer_ids': [1, 2, 3,
                         1, 2, 3,
                         1, 2, 3,

                         1, 2, 3,
                         1, 2, 3,
                         1, 2, 3,

                         1, 2, 3,
                         1, 2, 3,
                         1, 2, 3,

                         1, 2, 3,
                         1, 2, 3,
                         1, 2, 3],
        'timestamps': pd.to_datetime(['2020-02-14 06:00:00', '2020-03-14 06:20:00', '2020-04-05 10:07:00',
                                      '2020-02-14 06:05:00', '2020-03-14 06:21:00', '2020-04-05 10:15:00',
                                      '2020-02-14 06:07:00', '2020-03-14 06:25:00', '2020-04-05 10:19:00',

                                      '2020-03-20 17:00:00', '2020-05-01 10:25:00', '2020-06-25 15:19:00',
                                      '2020-03-20 17:02:00', '2020-05-01 10:25:30', '2020-06-25 15:22:00',
                                      '2020-03-20 17:32:00', '2020-05-01 10:26:30', '2020-06-25 15:46:00',

                                      '2020-04-02 12:00:00', '2020-07-10 11:05:00', '2020-10-11 21:36:00',
                                      '2020-04-02 16:07:00', '2020-07-10 11:08:00', '2020-10-11 21:46:00',
                                      '2020-04-02 16:08:00', '2020-07-10 11:15:00', '2020-10-11 21:48:00',

                                      '2020-05-30 06:07:00', '2020-08-16 22:45:00', '2020-12-14 06:19:00',
                                      '2020-05-30 06:08:00', '2020-08-16 22:49:00', '2020-12-14 06:45:00',
                                      '2020-05-30 06:18:00', '2020-08-16 22:55:00', '2020-12-14 06:46:00'])
    })

    df_two_negative = 'hello, i am not DataFrame'

    '''
    expected data:
    <customer_id>: 1
    <session_id_list>: [0, 1, 1, 2, 2, 3, 4, 5, 5, 6, 6, 7]
    <customer_id>: 2
    <session_id_list>: [0, 0, 1, 2, 2, 2, 3, 3, 4, 5, 6, 7]
    <customer_id>: 3
    <session_id_list>: [0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8]
    '''
    def test_one_positive(self):
        add_session_id_to_dataframe(self.df_positive)
        assert self.df_positive.loc[self.df_positive['customer_id'] == 1]['session_id'].tolist() == \
               [0, 1, 1, 2, 2, 3, 4, 5, 5, 6, 6, 7]
        assert self.df_positive.loc[self.df_positive['customer_id'] == 2]['session_id'].tolist() == \
               [0, 0, 1, 2, 2, 2, 3, 3, 4, 5, 6, 7]
        assert self.df_positive.loc[self.df_positive['customer_id'] == 3]['session_id'].tolist() == \
               [0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8]

    '''
    expected data: RequiredColumnsException('not found required columns: "customer_id" or "timestamp"')
    '''
    def test_one_negative(self):
        with pytest.raises(RequiredColumnsException):
            add_session_id_to_dataframe(self.df_one_negative)

    '''
    expected data: TypeError('df must have pandas.DataFrame type')
    '''
    def test_two_negative(self):
        with pytest.raises(TypeError):
            add_session_id_to_dataframe(self.df_two_negative)
